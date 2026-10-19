use std::pin::Pin;
use std::time::Duration;

use anyhow::Result;
use futures::{Stream, StreamExt};
use tokio::sync::mpsc;
use tokio::time::{self, Instant};

use crate::consumer;
use crate::protocol::types::{AudioFrame, AudioStopReason, CtlEvent, InteractionModel, StreamId};
use crate::protocol::Protocol;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Disconnected,
    Connected,
    Opening,
    Streaming,
}

impl std::fmt::Display for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Disconnected => write!(f, "disconnected"),
            Self::Connected => write!(f, "connected"),
            Self::Opening => write!(f, "opening"),
            Self::Streaming => write!(f, "streaming"),
        }
    }
}

/// Commands that can be sent from external sources (D-Bus, CLI, etc.).
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
#[allow(clippy::enum_variant_names)]
pub enum ExternalCommand {
    MicOpen,
    MicClose,
    MicToggle,
}

/// Session timeout configuration.
#[derive(Debug, Clone)]
pub struct SessionTimeouts {
    /// Auto-close mic if no frames arrive for this long (device went to sleep).
    /// Resets on every received audio frame. `0` = disabled.
    pub frame_timeout: Duration,
    /// Re-send keepalive at this interval to reset the remote's audio transfer
    /// timeout (spec §4.6.1). `0` = disabled.
    pub keepalive: Duration,
}

/// Events from the device (connection state changes).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum DeviceConnectionEvent {
    Disconnected,
}

/// A boxed future that borrows `self` and returns `T`.
pub type BleFut<'a, T> = Pin<Box<dyn std::future::Future<Output = Result<T>> + Send + 'a>>;

/// A boxed async stream of `T`.
pub type BleStream<T> = Pin<Box<dyn Stream<Item = T> + Send>>;

/// BLE notification streams for an active session.
/// Created during negotiation and passed to `run_session`.
pub struct BleStreams {
    pub ctl: BleStream<Vec<u8>>,
    pub rx: BleStream<Vec<u8>>,
    pub events: BleStream<DeviceConnectionEvent>,
}

/// Abstraction over BLE device operations for testability.
pub trait BleDevice: Send {
    /// Write a command to the TX characteristic.
    fn write_command(&self, data: &[u8]) -> BleFut<'_, ()>;

    /// Get a stream of CTL notifications.
    fn ctl_notifications(&self) -> BleFut<'_, BleStream<Vec<u8>>>;

    /// Get a stream of RX (audio) notifications.
    fn rx_notifications(&self) -> BleFut<'_, BleStream<Vec<u8>>>;

    /// Get a stream of device connection events.
    fn connection_events(&self) -> BleFut<'_, BleStream<DeviceConnectionEvent>>;
}

/// Configuration and channels for a session.
pub struct SessionConfig<'a> {
    pub audio_tx: mpsc::Sender<AudioFrame>,
    pub timeouts: &'a SessionTimeouts,
    pub command_rx: Option<&'a mut mpsc::Receiver<ExternalCommand>>,
    pub state_tx: Option<&'a tokio::sync::watch::Sender<State>>,
    pub consumer_rx: Option<tokio::sync::mpsc::Receiver<consumer::ConsumerEvent>>,
    pub mic_on_demand: bool,
    /// Shell command run when streaming starts and again when it stops.
    ///
    /// The direction is passed in the `ATVVOICE_TRIGGER` environment variable
    /// (`start` or `stop`), so a wrapper can be idempotent rather than toggle.
    pub trigger_cmd: Option<&'a str>,
}

/// Wait this long before telling the trigger command to stop, so audio still
/// inside the PipeWire buffers reaches the consumer first.
const TRIGGER_STOP_DELAY: std::time::Duration = std::time::Duration::from_millis(300);

/// Ignore audio frames this soon after a stream ends: they are the tail of the
/// same utterance, not a new press. A late frame used to re-enter `Streaming`
/// and fire a second start trigger, leaving the dictation tool recording.
const IMPLICIT_START_GRACE: std::time::Duration = std::time::Duration::from_millis(750);

/// Consecutive frames required before audio arriving outside a session is
/// treated as an implicit stream start.
const IMPLICIT_START_FRAMES: u8 = 3;

/// Bumped on every trigger so a pending stop can tell it has been superseded.
static TRIGGER_GENERATION: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Run the configured trigger command, if any.
///
/// `action` is passed to the command as `ATVVOICE_TRIGGER` (`start` or `stop`)
/// so a stateful wrapper can make the call idempotent instead of toggling.
fn fire_trigger(cmd: Option<&str>, action: &'static str, delay: Option<std::time::Duration>) {
    let Some(cmd) = cmd else { return };
    let cmd = cmd.to_owned();
    let generation = TRIGGER_GENERATION.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
    // On a helper thread: a delayed stop must not block the session loop, and
    // the child still needs reaping.
    std::thread::spawn(move || {
        if let Some(delay) = delay {
            std::thread::sleep(delay);
            if TRIGGER_GENERATION.load(std::sync::atomic::Ordering::SeqCst) != generation {
                tracing::debug!("trigger superseded by a newer press, skipping: {cmd}");
                return;
            }
        }
        tracing::info!("trigger({action}): {cmd}");
        match std::process::Command::new("sh")
            .arg("-c")
            .arg(&cmd)
            .env("ATVVOICE_TRIGGER", action)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
        {
            Ok(mut child) => {
                let _ = child.wait();
            }
            Err(e) => tracing::warn!("trigger command failed to start: {e}"),
        }
    });
}

/// Run the ATVV protocol session.
///
/// Audio frames (decoded PCM) are sent to `config.audio_tx`.
/// External commands (e.g. from D-Bus) are received on `config.command_rx`.
/// State changes are broadcast via `config.state_tx` (for D-Bus signals, etc.).
/// Returns on device disconnect or unrecoverable BLE error.
///
/// # Errors
///
/// Returns an error if a BLE write to the TX characteristic fails (e.g. the
/// device disconnects mid-command). Timeout-triggered `MIC_CLOSE` writes are
/// best-effort and do not propagate errors.
pub async fn run_session(
    ble: &(impl BleDevice + ?Sized),
    protocol: &mut dyn Protocol,
    streams: BleStreams,
    config: SessionConfig<'_>,
) -> Result<()> {
    let SessionConfig {
        audio_tx,
        timeouts,
        mut command_rx,
        state_tx,
        mut consumer_rx,
        mic_on_demand,
        trigger_cmd,
    } = config;
    let mut state = State::Connected;
    // When the last stream ended, and how many consecutive frames have arrived
    // while not streaming (used to ignore the tail of the previous utterance).
    let mut last_stream_end: Option<std::time::Instant> = None;
    let mut stray_frames: u8 = 0;

    // set_state takes &mut State as a parameter rather than capturing it because
    // `state` is used independently in the tokio::select! branches below. Capturing
    // it mutably would prevent its use in match guards and conditions.
    let set_state = |s: State,
                     state: &mut State,
                     last_stream_end: &mut Option<std::time::Instant>,
                     stray_frames: &mut u8| {
        let previous = *state;
        *state = s;
        // Keep the external trigger paired with streaming: fire on the way in
        // and on every way out (release, frame timeout, transport error) so a
        // dictation tool is never left recording.
        if previous == State::Streaming && s != State::Streaming {
            *last_stream_end = Some(std::time::Instant::now());
            *stray_frames = 0;
            // Leaving streaming: wait for audio still in the buffers to reach
            // the consumer before telling it to stop.
            fire_trigger(trigger_cmd, "stop", Some(TRIGGER_STOP_DELAY));
        } else if previous != State::Streaming && s == State::Streaming {
            *stray_frames = 0;
            fire_trigger(trigger_cmd, "start", None);
        }
        if let Some(tx) = state_tx {
            if tx.send(s).is_err() {
                tracing::debug!("state_tx: no receivers");
            }
        }
    };

    // Helper: reset session state after mic close. Used in multiple `tokio::select!`
    // match arms where the pattern (set state → Connected, clear seq/stream_id) repeats.
    // Inlines `set_state` logic to avoid borrow conflicts.
    macro_rules! close_mic_reset {
        ($state:ident, $last_seq:ident, $current_stream_id:ident) => {
            set_state(
                State::Connected,
                &mut $state,
                &mut last_stream_end,
                &mut stray_frames,
            );
            $last_seq = None;
            $current_stream_id = None;
        };
    }

    // Destructure pre-subscribed BLE streams
    let ctl_stream = streams.ctl;
    let rx_stream = streams.rx;
    let device_events = streams.events;
    tokio::pin!(ctl_stream);
    tokio::pin!(rx_stream);
    tokio::pin!(device_events);

    let mut last_seq: Option<u16> = None;

    // Track the current stream ID (set on AudioStart, used for MIC_CLOSE/keepalive).
    let mut current_stream_id: Option<StreamId> = None;

    // Keepalive: reset the remote's audio transfer timeout (spec §4.6.1)
    // using protocol.keepalive_cmd() (MIC_EXTEND for v1.0, MIC_OPEN for v0.4).
    let keepalive_interval = timeouts.keepalive;
    let keepalive_enabled = !keepalive_interval.is_zero();
    let keepalive_timer = time::sleep(keepalive_interval);
    tokio::pin!(keepalive_timer);

    // Frame timeout: reset on every audio frame. Detects device going to sleep
    // so the next button press starts cleanly (no double-press needed).
    let frame_timer = time::sleep(timeouts.frame_timeout);
    tokio::pin!(frame_timer);
    let frame_timeout_enabled = !timeouts.frame_timeout.is_zero();

    loop {
        tokio::select! {
            Some(data) = ctl_stream.next() => {
                if data.is_empty() {
                    continue;
                }
                match protocol.parse_ctl(&data) {
                    CtlEvent::AudioStop { reason } => {
                        tracing::info!("AUDIO_STOP (reason={:?})", reason);
                        // For HTT button release, remote already stopped — don't send `MIC_CLOSE`
                        if !matches!(reason, AudioStopReason::HttButtonRelease)
                            && state == State::Streaming
                        {
                            let sid = current_stream_id.unwrap_or(StreamId::MIC_OPEN);
                            ble.write_command(&protocol.mic_close_cmd(sid)).await?;
                            tracing::info!("Sent MIC_CLOSE");
                        }
                        close_mic_reset!(state, last_seq, current_stream_id);
                    }
                    CtlEvent::AudioStart { reason, codec, stream_id } => {
                        tracing::info!("AUDIO_START (reason={:?}, codec={:?}, stream_id={:?})", reason, codec, stream_id);
                        // Remote resets its sequence counter on every AUDIO_START
                        last_seq = None;
                        current_stream_id = Some(stream_id);
                        set_state(State::Streaming, &mut state, &mut last_stream_end, &mut stray_frames);
                        // Reset keepalive timer when streaming starts/restarts
                        if keepalive_enabled {
                            keepalive_timer.as_mut().reset(Instant::now() + keepalive_interval);
                        }
                        // Reset frame timer too: a stale, already-elapsed timer from a
                        // previous session (which ended >frame_timeout ago) would
                        // otherwise fire on the very next select! poll, instantly
                        // closing this brand-new stream before any frame arrives.
                        if frame_timeout_enabled {
                            frame_timer.as_mut().reset(Instant::now() + timeouts.frame_timeout);
                        }
                    }
                    CtlEvent::StartSearch => {
                        tracing::info!("START_SEARCH (state={:?})", state);

                        // HoldToTalk remotes re-send START_SEARCH right after
                        // AUDIO_START on a *single* press. Treating that as a
                        // second (toggle-off) press closed the mic ~30 ms after
                        // it opened, so the stream never delivered audio. Only
                        // toggle for non-HTT models; with HTT the remote ends
                        // the stream itself (AUDIO_STOP / HttButtonRelease).
                        let hold_to_talk =
                            protocol.interaction_model() == InteractionModel::HoldToTalk;

                        if state == State::Streaming || state == State::Opening {
                            if hold_to_talk {
                                tracing::debug!(
                                    "START_SEARCH while active (HoldToTalk): keeping mic open"
                                );
                            } else {
                                // Toggle: second press stops streaming
                                let sid = current_stream_id.unwrap_or(StreamId::MIC_OPEN);
                                ble.write_command(&protocol.mic_close_cmd(sid)).await?;
                                tracing::info!("Sent MIC_CLOSE (toggle off)");
                                close_mic_reset!(state, last_seq, current_stream_id);
                            }
                        } else {
                            ble.write_command(&protocol.mic_open_cmd()).await?;
                            tracing::info!("Sent MIC_OPEN");
                            set_state(State::Opening, &mut state, &mut last_stream_end, &mut stray_frames);
                            if frame_timeout_enabled {
                                frame_timer.as_mut().reset(Instant::now() + timeouts.frame_timeout);
                            }
                        }
                    }
                    CtlEvent::AudioSync(ref sync) => {
                        tracing::debug!("AUDIO_SYNC: {:?}", sync);
                        protocol.on_audio_sync(sync);
                    }
                    CtlEvent::CapsResp(caps) => {
                        tracing::debug!("Ignoring unsolicited CAPS_RESP: {:?}", caps.version);
                    }
                    CtlEvent::MicOpenError(code) => {
                        tracing::warn!("MIC_OPEN_ERROR: {:?}", code);
                        if state == State::Opening {
                            set_state(
                                State::Connected,
                                &mut state,
                                &mut last_stream_end,
                                &mut stray_frames,
                            );
                            current_stream_id = None;
                        }
                    }
                    CtlEvent::Unknown(ref raw) => {
                        tracing::debug!("Unknown CTL: {:02x?}", raw);
                    }
                }
            }
            Some(data) = rx_stream.next() => {
                // A reconnect can leave the remote sending audio without
                // re-sending AUDIO_START. Those frames used to be dropped
                // silently — a whole press could vanish — so treat arriving
                // audio as an implicit stream start.
                if state != State::Streaming {
                    // A late frame can arrive right after AUDIO_STOP (the tail
                    // of the same utterance). Treating a single frame as a new
                    // press fired a second start trigger and left the dictation
                    // tool recording, which inverted every later press/release.
                    // Ignore frames briefly after a stream ends, and require a
                    // few consecutive frames before an implicit start.
                    let in_grace = last_stream_end
                        .is_some_and(|t| t.elapsed() < IMPLICIT_START_GRACE);
                    if in_grace || stray_frames < IMPLICIT_START_FRAMES {
                        stray_frames = stray_frames.saturating_add(1);
                        tracing::debug!(
                            "audio frame while not streaming (state={state:?}); deferring implicit start"
                        );
                    } else {
                        tracing::warn!(
                            "audio frame while not streaming (state={state:?}); treating as stream start"
                        );
                        set_state(
                            State::Streaming,
                            &mut state,
                            &mut last_stream_end,
                            &mut stray_frames,
                        );
                        last_seq = None;
                        if frame_timeout_enabled {
                            frame_timer.as_mut().reset(Instant::now() + timeouts.frame_timeout);
                        }
                    }
                } else {
                    stray_frames = 0;
                }

                if let Some(frame) = protocol.decode_audio(&data) {
                    // Reset frame timer on every audio frame
                    if frame_timeout_enabled {
                        frame_timer.as_mut().reset(Instant::now() + timeouts.frame_timeout);
                    }

                    // Sequence gap detection
                    if let Some(last) = last_seq {
                        let expected = last.wrapping_add(1);
                        if frame.seq != expected {
                            tracing::warn!(
                                "Sequence gap: expected {}, got {} (dropped {} frames)",
                                expected,
                                frame.seq,
                                frame.seq.wrapping_sub(expected)
                            );
                        }
                    }
                    last_seq = Some(frame.seq);

                    if audio_tx.send(frame).await.is_err() {
                        tracing::warn!("audio frame dropped: receiver channel full or closed");
                    }
                }
            }
            // External commands (D-Bus, etc.)
            Some(cmd) = async {
                match command_rx.as_mut() {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                tracing::info!("External command: {:?} (state={:?})", cmd, state);
                let sid = current_stream_id.unwrap_or(StreamId::MIC_OPEN);
                match cmd {
                    ExternalCommand::MicOpen => {
                        if state == State::Connected {
                            ble.write_command(&protocol.mic_open_cmd()).await?;
                            tracing::info!("Sent MIC_OPEN (external)");
                            set_state(State::Opening, &mut state, &mut last_stream_end, &mut stray_frames);
                            if frame_timeout_enabled {
                                frame_timer.as_mut().reset(Instant::now() + timeouts.frame_timeout);
                            }
                        }
                    }
                    ExternalCommand::MicClose => {
                        if state == State::Streaming || state == State::Opening {
                            ble.write_command(&protocol.mic_close_cmd(sid)).await?;
                            tracing::info!("Sent MIC_CLOSE (external)");
                            close_mic_reset!(state, last_seq, current_stream_id);
                        }
                    }
                    ExternalCommand::MicToggle => {
                        if state == State::Streaming || state == State::Opening {
                            ble.write_command(&protocol.mic_close_cmd(sid)).await?;
                            tracing::info!("Sent MIC_CLOSE (external toggle)");
                            close_mic_reset!(state, last_seq, current_stream_id);
                        } else if state == State::Connected {
                            ble.write_command(&protocol.mic_open_cmd()).await?;
                            tracing::info!("Sent MIC_OPEN (external toggle)");
                            set_state(State::Opening, &mut state, &mut last_stream_end, &mut stray_frames);
                            if frame_timeout_enabled {
                                frame_timer.as_mut().reset(Instant::now() + timeouts.frame_timeout);
                            }
                        }
                    }
                }
            }
            // Consumer presence events (PipeWire consumer connect/disconnect)
            event = async {
                match consumer_rx.as_mut() {
                    Some(rx) => rx.recv().await,
                    None => std::future::pending::<Option<consumer::ConsumerEvent>>().await,
                }
            } => {
                if let Some(consumer::ConsumerEvent::Changed(count)) = event {
                    if mic_on_demand {
                        if count > 0 && state == State::Connected {
                            ble.write_command(&protocol.mic_open_cmd()).await?;
                            tracing::info!("Sent MIC_OPEN (consumer connected, on-demand)");
                            set_state(State::Opening, &mut state, &mut last_stream_end, &mut stray_frames);
                            if frame_timeout_enabled {
                                frame_timer.as_mut().reset(Instant::now() + timeouts.frame_timeout);
                            }
                        }
                        if count == 0 && (state == State::Streaming || state == State::Opening) {
                            tracing::info!("All consumers disconnected, closing mic");
                            let sid = current_stream_id.unwrap_or(StreamId::MIC_OPEN);
                            let _ = ble.write_command(&protocol.mic_close_cmd(sid)).await;
                            close_mic_reset!(state, last_seq, current_stream_id);
                        }
                    }
                }
            }
            // Keepalive: protocol handles the version-appropriate command
            // (MIC_EXTEND for v1.0, MIC_OPEN for v0.4).
            () = &mut keepalive_timer, if keepalive_enabled && state == State::Streaming => {
                let sid = current_stream_id.unwrap_or(StreamId::MIC_OPEN);
                tracing::debug!("Sending keepalive");
                let _ = ble.write_command(&protocol.keepalive_cmd(sid)).await;
                keepalive_timer.as_mut().reset(Instant::now() + keepalive_interval);
            }
            () = &mut frame_timer, if frame_timeout_enabled && (state == State::Streaming || state == State::Opening) => {
                tracing::info!("Frame timeout ({:?}) - device likely asleep, closing mic", timeouts.frame_timeout);
                let sid = current_stream_id.unwrap_or(StreamId::MIC_OPEN);
                let _ = ble.write_command(&protocol.mic_close_cmd(sid)).await;
                close_mic_reset!(state, last_seq, current_stream_id);
            }
            Some(event) = device_events.next() => {
                match event {
                    DeviceConnectionEvent::Disconnected => {
                        tracing::info!("Device disconnected");
                        if state == State::Streaming || state == State::Opening {
                            // The session ended without AUDIO_STOP; don't leave
                            // the dictation tool recording.
                            fire_trigger(trigger_cmd, "stop", None);
                        }
                        break;
                    }
                }
            }
            else => {
                tracing::info!("Notification streams ended");
                if state == State::Streaming || state == State::Opening {
                    fire_trigger(trigger_cmd, "stop", None);
                }
                break;
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::types::{
        AudioFrameSize, Capabilities, Codecs, InteractionModel, ProtocolVersion,
    };
    use crate::protocol::v04::ProtocolV04;
    use tokio::sync::mpsc as tokio_mpsc;
    use tokio::sync::Mutex;
    use tokio_stream::wrappers::UnboundedReceiverStream;

    /// Mock BLE device for testing the ATVV state machine.
    struct MockBleDevice {
        /// Commands written by the state machine (for assertion).
        commands: tokio_mpsc::UnboundedSender<Vec<u8>>,
        /// Inject CTL notifications.
        ctl_rx: Mutex<Option<tokio_mpsc::UnboundedReceiver<Vec<u8>>>>,
        /// Inject RX (audio) notifications.
        rx_rx: Mutex<Option<tokio_mpsc::UnboundedReceiver<Vec<u8>>>>,
        /// Inject device events.
        event_rx: Mutex<Option<tokio_mpsc::UnboundedReceiver<DeviceConnectionEvent>>>,
    }

    struct MockControls {
        commands_rx: tokio_mpsc::UnboundedReceiver<Vec<u8>>,
        ctl_tx: tokio_mpsc::UnboundedSender<Vec<u8>>,
        rx_tx: tokio_mpsc::UnboundedSender<Vec<u8>>,
        event_tx: tokio_mpsc::UnboundedSender<DeviceConnectionEvent>,
    }

    fn mock_device() -> (MockBleDevice, MockControls) {
        let (commands_tx, commands_rx) = tokio_mpsc::unbounded_channel();
        let (ctl_tx, ctl_rx) = tokio_mpsc::unbounded_channel();
        let (rx_tx, rx_rx) = tokio_mpsc::unbounded_channel();
        let (event_tx, event_rx) = tokio_mpsc::unbounded_channel();

        let device = MockBleDevice {
            commands: commands_tx,
            ctl_rx: Mutex::new(Some(ctl_rx)),
            rx_rx: Mutex::new(Some(rx_rx)),
            event_rx: Mutex::new(Some(event_rx)),
        };

        let controls = MockControls {
            commands_rx,
            ctl_tx,
            rx_tx,
            event_tx,
        };

        (device, controls)
    }

    impl BleDevice for MockBleDevice {
        fn write_command(&self, data: &[u8]) -> BleFut<'_, ()> {
            let data = data.to_vec();
            Box::pin(async move {
                self.commands
                    .send(data)
                    .map_err(|e| anyhow::anyhow!("mock send error: {}", e))?;
                Ok(())
            })
        }

        fn ctl_notifications(&self) -> BleFut<'_, BleStream<Vec<u8>>> {
            Box::pin(async {
                let rx = self
                    .ctl_rx
                    .lock()
                    .await
                    .take()
                    .expect("ctl_notifications called more than once");
                Ok(Box::pin(UnboundedReceiverStream::new(rx)) as BleStream<Vec<u8>>)
            })
        }

        fn rx_notifications(&self) -> BleFut<'_, BleStream<Vec<u8>>> {
            Box::pin(async {
                let rx = self
                    .rx_rx
                    .lock()
                    .await
                    .take()
                    .expect("rx_notifications called more than once");
                Ok(Box::pin(UnboundedReceiverStream::new(rx)) as BleStream<Vec<u8>>)
            })
        }

        fn connection_events(&self) -> BleFut<'_, BleStream<DeviceConnectionEvent>> {
            Box::pin(async {
                let rx = self
                    .event_rx
                    .lock()
                    .await
                    .take()
                    .expect("connection_events called more than once");
                Ok(Box::pin(UnboundedReceiverStream::new(rx)) as BleStream<DeviceConnectionEvent>)
            })
        }
    }

    /// Helper: make a fake 134-byte audio frame with a given sequence ID.
    /// Uses v0.4 format: 6-byte header + 128 ADPCM bytes.
    fn make_audio_frame(seq: u16) -> Vec<u8> {
        let mut frame = vec![0u8; 134];
        let seq_bytes = seq.to_be_bytes();
        frame[0] = seq_bytes[0];
        frame[1] = seq_bytes[1];
        frame
    }

    /// Helper: receive all currently buffered commands from the mock controls.
    async fn try_recv_all_commands(
        rx: &mut tokio_mpsc::UnboundedReceiver<Vec<u8>>,
    ) -> Vec<Vec<u8>> {
        let mut cmds = Vec::new();
        while let Ok(cmd) = rx.try_recv() {
            cmds.push(cmd);
        }
        cmds
    }

    /// Helper: wait for a specific state on the watch channel (with timeout).
    async fn wait_for_state(
        state_rx: &mut tokio::sync::watch::Receiver<State>,
        expected: State,
        timeout: Duration,
    ) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if *state_rx.borrow() == expected {
                return true;
            }
            tokio::select! {
                result = state_rx.changed() => {
                    if result.is_err() {
                        return false;
                    }
                }
                _ = tokio::time::sleep_until(deadline) => {
                    return *state_rx.borrow() == expected;
                }
            }
        }
    }

    const V04_MIC_OPEN: &[u8] = &[0x0C, 0x00, 0x01];
    const V04_MIC_CLOSE: &[u8] = &[0x0D];

    /// CTL opcodes for test injection
    const CTL_AUDIO_START: u8 = 0x04;
    const CTL_START_SEARCH: u8 = 0x08;
    const CTL_AUDIO_STOP: u8 = 0x00;

    // ── Test 1: Frame timeout reset regression ──────────────────────────

    #[tokio::test]
    async fn test_frame_timeout_reset_regression() {
        tokio::time::pause();

        let (device, mut ctrl) = mock_device();
        let (audio_tx, _audio_rx) = tokio_mpsc::channel(64);
        let (state_tx, mut state_rx) = tokio::sync::watch::channel(State::Connected);

        let timeouts = SessionTimeouts {
            frame_timeout: Duration::from_millis(100),

            keepalive: Duration::ZERO,
        };

        // Pre-initialize protocol with capabilities
        let caps = Capabilities {
            version: ProtocolVersion::V0_4,
            codecs: Codecs::ADPCM_8KHZ,
            interaction_model: InteractionModel::OnRequest,
            audio_frame_size: AudioFrameSize(134),
        };
        let mut protocol = ProtocolV04::new();
        protocol.on_caps_resp(&caps).unwrap();

        // Create BleStreams from mock (consumes receivers)
        let ctl = device.ctl_notifications().await.unwrap();
        let rx = device.rx_notifications().await.unwrap();
        let events = device.connection_events().await.unwrap();
        let streams = BleStreams { ctl, rx, events };

        let session = tokio::spawn(async move {
            run_session(
                &device,
                &mut protocol,
                streams,
                SessionConfig {
                    audio_tx,

                    timeouts: &timeouts,
                    command_rx: None,
                    state_tx: Some(&state_tx),
                    consumer_rx: None,
                    mic_on_demand: false,
                    trigger_cmd: None,
                },
            )
            .await
        });

        // No GET_CAPS to drain -- session starts in Ready
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(wait_for_state(&mut state_rx, State::Connected, Duration::from_millis(10)).await);

        // START_SEARCH → Opening
        ctrl.ctl_tx.send(vec![CTL_START_SEARCH]).unwrap();
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(wait_for_state(&mut state_rx, State::Opening, Duration::from_millis(10)).await);
        try_recv_all_commands(&mut ctrl.commands_rx).await; // MIC_OPEN

        // AUDIO_START → Streaming
        ctrl.ctl_tx.send(vec![CTL_AUDIO_START]).unwrap();
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(wait_for_state(&mut state_rx, State::Streaming, Duration::from_millis(10)).await);

        // Send audio frames for 200ms (resets timer each time)
        for i in 0..6 {
            ctrl.rx_tx.send(make_audio_frame(i)).unwrap();
            tokio::time::advance(Duration::from_millis(33)).await;
        }
        // State should still be Streaming
        assert_eq!(*state_rx.borrow(), State::Streaming);

        // START_SEARCH → toggle off → Ready
        ctrl.ctl_tx.send(vec![CTL_START_SEARCH]).unwrap();
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(wait_for_state(&mut state_rx, State::Connected, Duration::from_millis(10)).await);
        try_recv_all_commands(&mut ctrl.commands_rx).await; // MIC_CLOSE

        // Wait 200ms - timer would have fired if not properly managed (but state is Ready,
        // so the guard `if frame_timeout_enabled && (state == Streaming || Opening)` prevents it)
        tokio::time::advance(Duration::from_millis(200)).await;
        assert_eq!(*state_rx.borrow(), State::Connected);

        // START_SEARCH → Opening again
        ctrl.ctl_tx.send(vec![CTL_START_SEARCH]).unwrap();
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(wait_for_state(&mut state_rx, State::Opening, Duration::from_millis(10)).await);
        try_recv_all_commands(&mut ctrl.commands_rx).await; // MIC_OPEN

        // Verify frame timeout does NOT fire immediately (wait 50ms, should still be Opening)
        tokio::time::advance(Duration::from_millis(50)).await;
        assert_eq!(*state_rx.borrow(), State::Opening);

        // Verify frame timeout DOES fire after the full 100ms from the new Opening
        tokio::time::advance(Duration::from_millis(60)).await;
        assert!(wait_for_state(&mut state_rx, State::Connected, Duration::from_millis(10)).await);

        // Clean up: disconnect
        ctrl.event_tx
            .send(DeviceConnectionEvent::Disconnected)
            .unwrap();
        let result = session.await.unwrap();
        assert!(result.is_ok());
    }

    // ── Test 2: Toggle mode state transitions ───────────────────────────

    #[tokio::test]
    async fn test_toggle_mode_state_transitions() {
        tokio::time::pause();

        let (device, mut ctrl) = mock_device();
        let (audio_tx, _audio_rx) = tokio_mpsc::channel(64);
        let (state_tx, mut state_rx) = tokio::sync::watch::channel(State::Connected);

        let timeouts = SessionTimeouts {
            frame_timeout: Duration::ZERO,

            keepalive: Duration::ZERO,
        };

        let caps = Capabilities {
            version: ProtocolVersion::V0_4,
            codecs: Codecs::ADPCM_8KHZ,
            interaction_model: InteractionModel::OnRequest,
            audio_frame_size: AudioFrameSize(134),
        };
        let mut protocol = ProtocolV04::new();
        protocol.on_caps_resp(&caps).unwrap();

        let ctl = device.ctl_notifications().await.unwrap();
        let rx = device.rx_notifications().await.unwrap();
        let events = device.connection_events().await.unwrap();
        let streams = BleStreams { ctl, rx, events };

        let session = tokio::spawn(async move {
            run_session(
                &device,
                &mut protocol,
                streams,
                SessionConfig {
                    audio_tx,

                    timeouts: &timeouts,
                    command_rx: None,
                    state_tx: Some(&state_tx),
                    consumer_rx: None,
                    mic_on_demand: false,
                    trigger_cmd: None,
                },
            )
            .await
        });

        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(wait_for_state(&mut state_rx, State::Connected, Duration::from_millis(10)).await);

        // START_SEARCH from Ready → Opening, MIC_OPEN sent
        ctrl.ctl_tx.send(vec![CTL_START_SEARCH]).unwrap();
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(wait_for_state(&mut state_rx, State::Opening, Duration::from_millis(10)).await);
        let cmds = try_recv_all_commands(&mut ctrl.commands_rx).await;
        assert_eq!(cmds, vec![V04_MIC_OPEN.to_vec()]);

        // AUDIO_START → Streaming
        ctrl.ctl_tx.send(vec![CTL_AUDIO_START]).unwrap();
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(wait_for_state(&mut state_rx, State::Streaming, Duration::from_millis(10)).await);

        // START_SEARCH from Streaming → Ready (toggle off), MIC_CLOSE sent (NOT MIC_OPEN)
        ctrl.ctl_tx.send(vec![CTL_START_SEARCH]).unwrap();
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(wait_for_state(&mut state_rx, State::Connected, Duration::from_millis(10)).await);
        let cmds = try_recv_all_commands(&mut ctrl.commands_rx).await;
        assert_eq!(cmds, vec![V04_MIC_CLOSE.to_vec()]);

        // START_SEARCH from Ready → Opening, MIC_OPEN sent again
        ctrl.ctl_tx.send(vec![CTL_START_SEARCH]).unwrap();
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(wait_for_state(&mut state_rx, State::Opening, Duration::from_millis(10)).await);
        let cmds = try_recv_all_commands(&mut ctrl.commands_rx).await;
        assert_eq!(cmds, vec![V04_MIC_OPEN.to_vec()]);

        // Clean up
        ctrl.event_tx
            .send(DeviceConnectionEvent::Disconnected)
            .unwrap();
        let result = session.await.unwrap();
        assert!(result.is_ok());
    }

    // ── Test 3: Disconnect detection ────────────────────────────────────

    #[tokio::test]
    async fn test_disconnect_detection() {
        tokio::time::pause();

        let (device, ctrl) = mock_device();
        let (audio_tx, _audio_rx) = tokio_mpsc::channel(64);
        let (state_tx, _state_rx) = tokio::sync::watch::channel(State::Connected);

        let timeouts = SessionTimeouts {
            frame_timeout: Duration::ZERO,

            keepalive: Duration::ZERO,
        };

        let caps = Capabilities {
            version: ProtocolVersion::V0_4,
            codecs: Codecs::ADPCM_8KHZ,
            interaction_model: InteractionModel::OnRequest,
            audio_frame_size: AudioFrameSize(134),
        };
        let mut protocol = ProtocolV04::new();
        protocol.on_caps_resp(&caps).unwrap();

        let ctl = device.ctl_notifications().await.unwrap();
        let rx = device.rx_notifications().await.unwrap();
        let events = device.connection_events().await.unwrap();
        let streams = BleStreams { ctl, rx, events };

        let session = tokio::spawn(async move {
            run_session(
                &device,
                &mut protocol,
                streams,
                SessionConfig {
                    audio_tx,

                    timeouts: &timeouts,
                    command_rx: None,
                    state_tx: Some(&state_tx),
                    consumer_rx: None,
                    mic_on_demand: false,
                    trigger_cmd: None,
                },
            )
            .await
        });

        tokio::time::advance(Duration::from_millis(1)).await;

        // Send disconnect event
        ctrl.event_tx
            .send(DeviceConnectionEvent::Disconnected)
            .unwrap();
        tokio::time::advance(Duration::from_millis(1)).await;

        // Verify run_session returns Ok
        let result = session.await.unwrap();
        assert!(result.is_ok());
    }

    // ── Test 5: External commands ───────────────────────────────────────

    #[tokio::test]
    async fn test_external_commands() {
        tokio::time::pause();

        let (device, mut ctrl) = mock_device();
        let (audio_tx, _audio_rx) = tokio_mpsc::channel(64);
        let (state_tx, mut state_rx) = tokio::sync::watch::channel(State::Connected);
        let (cmd_tx, mut cmd_rx) = tokio_mpsc::channel::<ExternalCommand>(16);

        let timeouts = SessionTimeouts {
            frame_timeout: Duration::ZERO,

            keepalive: Duration::ZERO,
        };

        let caps = Capabilities {
            version: ProtocolVersion::V0_4,
            codecs: Codecs::ADPCM_8KHZ,
            interaction_model: InteractionModel::OnRequest,
            audio_frame_size: AudioFrameSize(134),
        };
        let mut protocol = ProtocolV04::new();
        protocol.on_caps_resp(&caps).unwrap();

        let ctl = device.ctl_notifications().await.unwrap();
        let rx = device.rx_notifications().await.unwrap();
        let events = device.connection_events().await.unwrap();
        let streams = BleStreams { ctl, rx, events };

        let session = tokio::spawn(async move {
            run_session(
                &device,
                &mut protocol,
                streams,
                SessionConfig {
                    audio_tx,

                    timeouts: &timeouts,
                    command_rx: Some(&mut cmd_rx),
                    state_tx: Some(&state_tx),
                    consumer_rx: None,
                    mic_on_demand: false,
                    trigger_cmd: None,
                },
            )
            .await
        });

        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(wait_for_state(&mut state_rx, State::Connected, Duration::from_millis(10)).await);

        // MicOpen from Ready → Opening
        cmd_tx.send(ExternalCommand::MicOpen).await.unwrap();
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(wait_for_state(&mut state_rx, State::Opening, Duration::from_millis(10)).await);
        let cmds = try_recv_all_commands(&mut ctrl.commands_rx).await;
        assert_eq!(cmds, vec![V04_MIC_OPEN.to_vec()]);

        // Transition to Streaming for close test
        ctrl.ctl_tx.send(vec![CTL_AUDIO_START]).unwrap();
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(wait_for_state(&mut state_rx, State::Streaming, Duration::from_millis(10)).await);

        // MicClose from Streaming → Ready
        cmd_tx.send(ExternalCommand::MicClose).await.unwrap();
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(wait_for_state(&mut state_rx, State::Connected, Duration::from_millis(10)).await);
        let cmds = try_recv_all_commands(&mut ctrl.commands_rx).await;
        assert_eq!(cmds, vec![V04_MIC_CLOSE.to_vec()]);

        // MicToggle from Ready → Opening
        cmd_tx.send(ExternalCommand::MicToggle).await.unwrap();
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(wait_for_state(&mut state_rx, State::Opening, Duration::from_millis(10)).await);
        let cmds = try_recv_all_commands(&mut ctrl.commands_rx).await;
        assert_eq!(cmds, vec![V04_MIC_OPEN.to_vec()]);

        // Transition to Streaming
        ctrl.ctl_tx.send(vec![CTL_AUDIO_START]).unwrap();
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(wait_for_state(&mut state_rx, State::Streaming, Duration::from_millis(10)).await);

        // MicToggle from Streaming → Ready
        cmd_tx.send(ExternalCommand::MicToggle).await.unwrap();
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(wait_for_state(&mut state_rx, State::Connected, Duration::from_millis(10)).await);
        let cmds = try_recv_all_commands(&mut ctrl.commands_rx).await;
        assert_eq!(cmds, vec![V04_MIC_CLOSE.to_vec()]);

        // Clean up
        ctrl.event_tx
            .send(DeviceConnectionEvent::Disconnected)
            .unwrap();
        let result = session.await.unwrap();
        assert!(result.is_ok());
    }

    // ── Test: Consumer connect triggers MIC_OPEN when Connected ────

    #[tokio::test]
    async fn test_mic_on_demand_auto_open() {
        tokio::time::pause();

        let (device, mut ctrl) = mock_device();
        let (audio_tx, _audio_rx) = tokio_mpsc::channel(64);
        let (state_tx, mut state_rx) = tokio::sync::watch::channel(State::Connected);
        let (consumer_tx, consumer_rx) = tokio_mpsc::channel(16);

        let timeouts = SessionTimeouts {
            frame_timeout: Duration::ZERO,

            keepalive: Duration::ZERO,
        };

        let caps = Capabilities {
            version: ProtocolVersion::V0_4,
            codecs: Codecs::ADPCM_8KHZ,
            interaction_model: InteractionModel::OnRequest,
            audio_frame_size: AudioFrameSize(134),
        };
        let mut protocol = ProtocolV04::new();
        protocol.on_caps_resp(&caps).unwrap();

        let ctl = device.ctl_notifications().await.unwrap();
        let rx = device.rx_notifications().await.unwrap();
        let events = device.connection_events().await.unwrap();
        let streams = BleStreams { ctl, rx, events };

        let session = tokio::spawn(async move {
            run_session(
                &device,
                &mut protocol,
                streams,
                SessionConfig {
                    audio_tx,
                    timeouts: &timeouts,
                    command_rx: None,
                    state_tx: Some(&state_tx),
                    consumer_rx: Some(consumer_rx),
                    mic_on_demand: true,
                    trigger_cmd: None,
                },
            )
            .await
        });

        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(wait_for_state(&mut state_rx, State::Connected, Duration::from_millis(10)).await);

        // Consumer connects → should trigger MIC_OPEN and transition to Opening
        consumer_tx
            .send(consumer::ConsumerEvent::Changed(1))
            .await
            .unwrap();
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(wait_for_state(&mut state_rx, State::Opening, Duration::from_millis(10)).await);

        let cmds = try_recv_all_commands(&mut ctrl.commands_rx).await;
        assert_eq!(cmds, vec![V04_MIC_OPEN.to_vec()]);

        // Clean up
        ctrl.event_tx
            .send(DeviceConnectionEvent::Disconnected)
            .unwrap();
        let result = session.await.unwrap();
        assert!(result.is_ok());
    }

    // ── Test: Consumer disconnect immediately closes mic ────────────────

    #[tokio::test]
    async fn test_mic_on_demand_auto_close() {
        tokio::time::pause();

        let (device, mut ctrl) = mock_device();
        let (audio_tx, _audio_rx) = tokio_mpsc::channel(64);
        let (state_tx, mut state_rx) = tokio::sync::watch::channel(State::Connected);
        let (consumer_tx, consumer_rx) = tokio_mpsc::channel(16);

        let timeouts = SessionTimeouts {
            frame_timeout: Duration::ZERO,
            keepalive: Duration::ZERO,
        };

        let caps = Capabilities {
            version: ProtocolVersion::V0_4,
            codecs: Codecs::ADPCM_8KHZ,
            interaction_model: InteractionModel::OnRequest,
            audio_frame_size: AudioFrameSize(134),
        };
        let mut protocol = ProtocolV04::new();
        protocol.on_caps_resp(&caps).unwrap();

        let ctl = device.ctl_notifications().await.unwrap();
        let rx = device.rx_notifications().await.unwrap();
        let events = device.connection_events().await.unwrap();
        let streams = BleStreams { ctl, rx, events };

        let session = tokio::spawn(async move {
            run_session(
                &device,
                &mut protocol,
                streams,
                SessionConfig {
                    audio_tx,
                    timeouts: &timeouts,
                    command_rx: None,
                    state_tx: Some(&state_tx),
                    consumer_rx: Some(consumer_rx),
                    mic_on_demand: true,
                    trigger_cmd: None,
                },
            )
            .await
        });

        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(wait_for_state(&mut state_rx, State::Connected, Duration::from_millis(10)).await);

        // Consumer connects → MIC_OPEN → Opening
        consumer_tx
            .send(consumer::ConsumerEvent::Changed(1))
            .await
            .unwrap();
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(wait_for_state(&mut state_rx, State::Opening, Duration::from_millis(10)).await);
        try_recv_all_commands(&mut ctrl.commands_rx).await;

        // AUDIO_START → Streaming
        ctrl.ctl_tx.send(vec![CTL_AUDIO_START]).unwrap();
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(wait_for_state(&mut state_rx, State::Streaming, Duration::from_millis(10)).await);

        // All consumers disconnect → immediate MIC_CLOSE
        consumer_tx
            .send(consumer::ConsumerEvent::Changed(0))
            .await
            .unwrap();
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(wait_for_state(&mut state_rx, State::Connected, Duration::from_millis(10)).await);

        let cmds = try_recv_all_commands(&mut ctrl.commands_rx).await;
        assert_eq!(cmds, vec![V04_MIC_CLOSE.to_vec()]);

        // Clean up
        ctrl.event_tx
            .send(DeviceConnectionEvent::Disconnected)
            .unwrap();
        let result = session.await.unwrap();
        assert!(result.is_ok());
    }

    // ── Test: Consumer event ignored when disabled ──────────────────────

    #[tokio::test]
    async fn test_mic_on_demand_disabled_ignores_consumers() {
        tokio::time::pause();

        let (device, mut ctrl) = mock_device();
        let (audio_tx, _audio_rx) = tokio_mpsc::channel(64);
        let (state_tx, mut state_rx) = tokio::sync::watch::channel(State::Connected);
        let (consumer_tx, consumer_rx) = tokio_mpsc::channel(16);

        let timeouts = SessionTimeouts {
            frame_timeout: Duration::ZERO,

            keepalive: Duration::ZERO,
        };

        let caps = Capabilities {
            version: ProtocolVersion::V0_4,
            codecs: Codecs::ADPCM_8KHZ,
            interaction_model: InteractionModel::OnRequest,
            audio_frame_size: AudioFrameSize(134),
        };
        let mut protocol = ProtocolV04::new();
        protocol.on_caps_resp(&caps).unwrap();

        let ctl = device.ctl_notifications().await.unwrap();
        let rx = device.rx_notifications().await.unwrap();
        let events = device.connection_events().await.unwrap();
        let streams = BleStreams { ctl, rx, events };

        let session = tokio::spawn(async move {
            run_session(
                &device,
                &mut protocol,
                streams,
                SessionConfig {
                    audio_tx,
                    timeouts: &timeouts,
                    command_rx: None,
                    state_tx: Some(&state_tx),
                    consumer_rx: Some(consumer_rx),
                    mic_on_demand: false, // disabled
                    trigger_cmd: None,
                },
            )
            .await
        });

        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(wait_for_state(&mut state_rx, State::Connected, Duration::from_millis(10)).await);

        // Consumer connects — should be ignored because mic_on_demand=false
        consumer_tx
            .send(consumer::ConsumerEvent::Changed(1))
            .await
            .unwrap();
        tokio::time::advance(Duration::from_millis(10)).await;

        // State should remain Connected
        assert_eq!(*state_rx.borrow(), State::Connected);

        // No commands sent
        let cmds = try_recv_all_commands(&mut ctrl.commands_rx).await;
        assert!(cmds.is_empty(), "Expected no commands, got: {cmds:?}");

        // Clean up
        ctrl.event_tx
            .send(DeviceConnectionEvent::Disconnected)
            .unwrap();
        let result = session.await.unwrap();
        assert!(result.is_ok());
    }

    // ── Test: Consumer event ignored when already streaming ─────────────

    #[tokio::test]
    async fn test_mic_on_demand_no_reopen_when_streaming() {
        tokio::time::pause();

        let (device, mut ctrl) = mock_device();
        let (audio_tx, _audio_rx) = tokio_mpsc::channel(64);
        let (state_tx, mut state_rx) = tokio::sync::watch::channel(State::Connected);
        let (consumer_tx, consumer_rx) = tokio_mpsc::channel(16);

        let timeouts = SessionTimeouts {
            frame_timeout: Duration::ZERO,

            keepalive: Duration::ZERO,
        };

        let caps = Capabilities {
            version: ProtocolVersion::V0_4,
            codecs: Codecs::ADPCM_8KHZ,
            interaction_model: InteractionModel::OnRequest,
            audio_frame_size: AudioFrameSize(134),
        };
        let mut protocol = ProtocolV04::new();
        protocol.on_caps_resp(&caps).unwrap();

        let ctl = device.ctl_notifications().await.unwrap();
        let rx = device.rx_notifications().await.unwrap();
        let events = device.connection_events().await.unwrap();
        let streams = BleStreams { ctl, rx, events };

        let session = tokio::spawn(async move {
            run_session(
                &device,
                &mut protocol,
                streams,
                SessionConfig {
                    audio_tx,
                    timeouts: &timeouts,
                    command_rx: None,
                    state_tx: Some(&state_tx),
                    consumer_rx: Some(consumer_rx),
                    mic_on_demand: true,
                    trigger_cmd: None,
                },
            )
            .await
        });

        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(wait_for_state(&mut state_rx, State::Connected, Duration::from_millis(10)).await);

        // Get into Streaming state via normal flow
        ctrl.ctl_tx.send(vec![CTL_START_SEARCH]).unwrap();
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(wait_for_state(&mut state_rx, State::Opening, Duration::from_millis(10)).await);
        try_recv_all_commands(&mut ctrl.commands_rx).await; // consume MIC_OPEN

        ctrl.ctl_tx.send(vec![CTL_AUDIO_START]).unwrap();
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(wait_for_state(&mut state_rx, State::Streaming, Duration::from_millis(10)).await);

        // Another consumer connects while already streaming — no additional MIC_OPEN
        consumer_tx
            .send(consumer::ConsumerEvent::Changed(2))
            .await
            .unwrap();
        tokio::time::advance(Duration::from_millis(10)).await;

        // State should remain Streaming
        assert_eq!(*state_rx.borrow(), State::Streaming);

        // No additional commands sent
        let cmds = try_recv_all_commands(&mut ctrl.commands_rx).await;
        assert!(
            cmds.is_empty(),
            "Expected no additional commands, got: {cmds:?}"
        );

        // Clean up
        ctrl.event_tx
            .send(DeviceConnectionEvent::Disconnected)
            .unwrap();
        let result = session.await.unwrap();
        assert!(result.is_ok());
    }

    // ── Test: Consumer disconnect in Opening state closes mic ──────────

    #[tokio::test]
    async fn test_mic_on_demand_close_while_opening() {
        tokio::time::pause();

        let (device, mut ctrl) = mock_device();
        let (audio_tx, _audio_rx) = tokio_mpsc::channel(64);
        let (state_tx, mut state_rx) = tokio::sync::watch::channel(State::Connected);
        let (consumer_tx, consumer_rx) = tokio_mpsc::channel(16);

        let timeouts = SessionTimeouts {
            frame_timeout: Duration::ZERO,
            keepalive: Duration::ZERO,
        };

        let caps = Capabilities {
            version: ProtocolVersion::V0_4,
            codecs: Codecs::ADPCM_8KHZ,
            interaction_model: InteractionModel::OnRequest,
            audio_frame_size: AudioFrameSize(134),
        };
        let mut protocol = ProtocolV04::new();
        protocol.on_caps_resp(&caps).unwrap();

        let ctl = device.ctl_notifications().await.unwrap();
        let rx = device.rx_notifications().await.unwrap();
        let events = device.connection_events().await.unwrap();
        let streams = BleStreams { ctl, rx, events };

        let session = tokio::spawn(async move {
            run_session(
                &device,
                &mut protocol,
                streams,
                SessionConfig {
                    audio_tx,
                    timeouts: &timeouts,
                    command_rx: None,
                    state_tx: Some(&state_tx),
                    consumer_rx: Some(consumer_rx),
                    mic_on_demand: true,
                    trigger_cmd: None,
                },
            )
            .await
        });

        tokio::time::advance(Duration::from_millis(1)).await;

        // Consumer connects → MIC_OPEN → Opening
        consumer_tx
            .send(consumer::ConsumerEvent::Changed(1))
            .await
            .unwrap();
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(wait_for_state(&mut state_rx, State::Opening, Duration::from_millis(10)).await);
        try_recv_all_commands(&mut ctrl.commands_rx).await;

        // Consumer disconnects BEFORE AUDIO_START → should close immediately
        consumer_tx
            .send(consumer::ConsumerEvent::Changed(0))
            .await
            .unwrap();
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(wait_for_state(&mut state_rx, State::Connected, Duration::from_millis(10)).await);

        let cmds = try_recv_all_commands(&mut ctrl.commands_rx).await;
        assert_eq!(cmds, vec![V04_MIC_CLOSE.to_vec()]);

        // Clean up
        ctrl.event_tx
            .send(DeviceConnectionEvent::Disconnected)
            .unwrap();
        let result = session.await.unwrap();
        assert!(result.is_ok());
    }

    // ── Test: Button-opened mic closes on consumer 1→0 transition ───────
    //
    // User presses mic button (manual open), then a PW consumer connects and
    // disconnects. Going from >0 to 0 consumers always closes the mic.

    #[tokio::test]
    async fn test_mic_on_demand_button_open_consumer_close() {
        tokio::time::pause();

        let (device, mut ctrl) = mock_device();
        let (audio_tx, _audio_rx) = tokio_mpsc::channel(64);
        let (state_tx, mut state_rx) = tokio::sync::watch::channel(State::Connected);
        let (consumer_tx, consumer_rx) = tokio_mpsc::channel(16);

        let timeouts = SessionTimeouts {
            frame_timeout: Duration::ZERO,
            keepalive: Duration::ZERO,
        };

        let caps = Capabilities {
            version: ProtocolVersion::V0_4,
            codecs: Codecs::ADPCM_8KHZ,
            interaction_model: InteractionModel::OnRequest,
            audio_frame_size: AudioFrameSize(134),
        };
        let mut protocol = ProtocolV04::new();
        protocol.on_caps_resp(&caps).unwrap();

        let ctl = device.ctl_notifications().await.unwrap();
        let rx = device.rx_notifications().await.unwrap();
        let events = device.connection_events().await.unwrap();
        let streams = BleStreams { ctl, rx, events };

        let session = tokio::spawn(async move {
            run_session(
                &device,
                &mut protocol,
                streams,
                SessionConfig {
                    audio_tx,
                    timeouts: &timeouts,
                    command_rx: None,
                    state_tx: Some(&state_tx),
                    consumer_rx: Some(consumer_rx),
                    mic_on_demand: true,
                    trigger_cmd: None,
                },
            )
            .await
        });

        tokio::time::advance(Duration::from_millis(1)).await;

        // User presses mic button → MIC_OPEN → Opening
        ctrl.ctl_tx.send(vec![CTL_START_SEARCH]).unwrap();
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(wait_for_state(&mut state_rx, State::Opening, Duration::from_millis(10)).await);
        try_recv_all_commands(&mut ctrl.commands_rx).await;

        // AUDIO_START → Streaming
        ctrl.ctl_tx.send(vec![CTL_AUDIO_START]).unwrap();
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(wait_for_state(&mut state_rx, State::Streaming, Duration::from_millis(10)).await);

        // PW consumer connects (count 0→1)
        consumer_tx
            .send(consumer::ConsumerEvent::Changed(1))
            .await
            .unwrap();
        tokio::time::advance(Duration::from_millis(1)).await;
        assert_eq!(*state_rx.borrow(), State::Streaming); // still streaming

        // PW consumer disconnects (count 1→0) → closes mic regardless of how it was opened
        consumer_tx
            .send(consumer::ConsumerEvent::Changed(0))
            .await
            .unwrap();
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(wait_for_state(&mut state_rx, State::Connected, Duration::from_millis(10)).await);

        let cmds = try_recv_all_commands(&mut ctrl.commands_rx).await;
        assert_eq!(cmds, vec![V04_MIC_CLOSE.to_vec()]);

        // Clean up
        ctrl.event_tx
            .send(DeviceConnectionEvent::Disconnected)
            .unwrap();
        let result = session.await.unwrap();
        assert!(result.is_ok());
    }

    // ── Test 7: HTT button release does NOT send MIC_CLOSE ──────────

    #[tokio::test]
    async fn test_htt_button_release_skips_mic_close() {
        use crate::protocol::v10::ProtocolV10;

        tokio::time::pause();

        let (device, mut ctrl) = mock_device();
        let (audio_tx, _audio_rx) = tokio_mpsc::channel(64);
        let (state_tx, mut state_rx) = tokio::sync::watch::channel(State::Connected);

        let timeouts = SessionTimeouts {
            frame_timeout: Duration::ZERO,

            keepalive: Duration::ZERO,
        };

        let caps = Capabilities {
            version: ProtocolVersion::V1_0,
            codecs: Codecs::ADPCM_8KHZ,
            interaction_model: InteractionModel::HoldToTalk,
            audio_frame_size: AudioFrameSize(20),
        };
        let mut protocol = ProtocolV10::new();
        protocol.on_caps_resp(&caps).unwrap();

        let ctl = device.ctl_notifications().await.unwrap();
        let rx = device.rx_notifications().await.unwrap();
        let events = device.connection_events().await.unwrap();
        let streams = BleStreams { ctl, rx, events };

        let session = tokio::spawn(async move {
            run_session(
                &device,
                &mut protocol,
                streams,
                SessionConfig {
                    audio_tx,

                    timeouts: &timeouts,
                    command_rx: None,
                    state_tx: Some(&state_tx),
                    consumer_rx: None,
                    mic_on_demand: false,
                    trigger_cmd: None,
                },
            )
            .await
        });

        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(wait_for_state(&mut state_rx, State::Connected, Duration::from_millis(10)).await);

        // START_SEARCH → Opening, MIC_OPEN sent
        ctrl.ctl_tx.send(vec![CTL_START_SEARCH]).unwrap();
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(wait_for_state(&mut state_rx, State::Opening, Duration::from_millis(10)).await);
        try_recv_all_commands(&mut ctrl.commands_rx).await; // consume MIC_OPEN

        // v1.0 AUDIO_START: [0x04, reason=HoldToTalk(0x03), codec=0x01, stream_id=0x00]
        ctrl.ctl_tx
            .send(vec![CTL_AUDIO_START, 0x03, 0x01, 0x00])
            .unwrap();
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(wait_for_state(&mut state_rx, State::Streaming, Duration::from_millis(10)).await);

        // AUDIO_STOP with reason=HttButtonRelease (0x02) → should NOT send MIC_CLOSE
        ctrl.ctl_tx.send(vec![CTL_AUDIO_STOP, 0x02]).unwrap();
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(wait_for_state(&mut state_rx, State::Connected, Duration::from_millis(10)).await);

        // Verify NO MIC_CLOSE was written
        let cmds = try_recv_all_commands(&mut ctrl.commands_rx).await;
        assert!(
            cmds.is_empty(),
            "Expected no commands after HttButtonRelease, got: {cmds:?}"
        );

        // Clean up
        ctrl.event_tx
            .send(DeviceConnectionEvent::Disconnected)
            .unwrap();
        let result = session.await.unwrap();
        assert!(result.is_ok());
    }
}
