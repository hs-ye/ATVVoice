//! PipeWire virtual microphone.
//!
//! Decoded PCM is played into a private null sink with `pw-cat`, and the
//! sink's monitor is republished by a remap source. That remapped source is
//! what applications record from, under the same node name the old
//! `Audio/Source` stream used.
//!
//! Why not a `media.class=Audio/Source` stream, which is what upstream does?
//! On this system such a stream is never scheduled: the node reports
//! `running`, the link is `active`, the format negotiates, and
//! `Paused -> Streaming` fires — but its process callback is never invoked, so
//! consumers only ever receive silence. A sink always has a driver, so playing
//! into one is reliable. (An in-process playback stream was tried first and
//! segfaulted inside libpipewire during setup, hence the `pw-cat` child.)
//!
//! This thread blocks. It also runs a PipeWire main loop purely to receive the
//! shutdown signal, and writes PCM to the child from a worker thread.

use std::io::Write;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::Duration;

use pipewire::main_loop::MainLoopRc;

/// Number of audio channels fed to the sink (mono).
const CHANNELS: u32 = 1;

/// How often the writer wakes when no audio is pending.
const TICK: Duration = Duration::from_millis(20);

/// Signal to shut down the PipeWire source cleanly.
#[derive(Debug)]
pub struct Shutdown;

/// Name of the private null sink that backs the virtual microphone.
fn sink_name(node_name: &str) -> String {
    format!("{node_name}-sink")
}

/// Run `pactl`, returning trimmed stdout on success.
fn pactl(args: &[&str]) -> Option<String> {
    match Command::new("pactl").args(args).output() {
        Ok(out) if out.status.success() => {
            Some(String::from_utf8_lossy(&out.stdout).trim().to_string())
        }
        Ok(out) => {
            tracing::warn!(
                "pactl {args:?} failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            );
            None
        }
        Err(e) => {
            tracing::warn!("could not run pactl {args:?}: {e}");
            None
        }
    }
}

/// Drop a sink/remap source left behind by a previous run so the names stay
/// predictable.
fn remove_stale(sink: &str, source: &str) {
    let Some(list) = pactl(&["list", "short", "modules"]) else {
        return;
    };
    let sink_arg = format!("sink_name={sink}");
    let source_arg = format!("source_name={source}");
    for line in list.lines() {
        let mut cols = line.split('\t');
        let (Some(id), Some(module), Some(args)) = (cols.next(), cols.next(), cols.next()) else {
            continue;
        };
        let stale = (module == "module-null-sink" && args.contains(&sink_arg))
            || (module == "module-remap-source" && args.contains(&source_arg));
        if stale {
            tracing::debug!("unloading stale {module} {id}");
            pactl(&["unload-module", id]);
        }
    }
}

/// Create the null sink backing the microphone; returns its module id.
fn create_sink(sink: &str, description: &str) -> Option<u32> {
    let id = pactl(&[
        "load-module",
        "module-null-sink",
        &format!("sink_name={sink}"),
        &format!("sink_properties=device.description=\"{description}\""),
    ])?
    .parse::<u32>()
    .ok()?;
    // WirePlumber persists per-device volume/mute; a muted sink would make the
    // microphone silent in a way that looks exactly like the bug we just fixed.
    pactl(&["set-sink-volume", sink, "100%"]);
    pactl(&["set-sink-mute", sink, "0"]);
    tracing::info!("Virtual sink '{sink}' created");
    Some(id)
}

/// Republish the sink monitor as an ordinary source, keeping the historical
/// device name: some applications hide sources that look like monitors.
fn create_remap(source: &str, master: &str, description: &str) -> Option<u32> {
    pactl(&[
        "load-module",
        "module-remap-source",
        &format!("master={master}"),
        &format!("source_name={source}"),
        &format!("source_properties=device.description=\"{description}\""),
    ])?
    .parse::<u32>()
    .ok()
}

fn unload(module: u32) {
    pactl(&["unload-module", &module.to_string()]);
}

/// Feed PCM to `pw-cat` so it plays into the null sink.
///
/// Writes silence when idle rather than nothing: a continuously fed sink keeps
/// the graph (and therefore the monitor) alive for consumers.
fn write_pcm_to_sink(
    audio_rx: mpsc::Receiver<Vec<i16>>,
    gain_db: f32,
    sample_rate: u32,
    sink: &str,
    stop: Arc<AtomicBool>,
) {
    let mut child = match Command::new("pw-cat")
        .args([
            "--playback",
            "--target",
            sink,
            "--format",
            "s16",
            "--rate",
            &sample_rate.to_string(),
            "--channels",
            &CHANNELS.to_string(),
            "-",
        ])
        .stdin(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(e) => {
            tracing::error!("could not start pw-cat: {e}");
            return;
        }
    };
    let Some(mut stdin) = child.stdin.take() else {
        tracing::error!("pw-cat stdin unavailable");
        return;
    };

    tracing::info!(
        "pw-cat playback into '{sink}' started ({}kHz S16LE mono, gain={gain_db}dB)",
        sample_rate / 1000
    );

    let gain_linear = 10f32.powf(gain_db / 20.0);
    let silence = vec![0u8; (sample_rate as usize / 1000) * TICK.as_millis() as usize * 2];
    let mut bytes: Vec<u8> = Vec::with_capacity(8192);

    loop {
        if stop.load(Ordering::Relaxed) {
            break;
        }
        match audio_rx.recv_timeout(TICK) {
            Ok(mut frame) => {
                crate::adpcm::apply_gain_linear(&mut frame, gain_linear);
                bytes.clear();
                for sample in frame {
                    bytes.extend_from_slice(&sample.to_le_bytes());
                }
                if let Err(e) = stdin.write_all(&bytes) {
                    tracing::warn!("pw-cat write failed: {e}");
                    break;
                }
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                if let Err(e) = stdin.write_all(&silence) {
                    tracing::warn!("pw-cat write failed: {e}");
                    break;
                }
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
        }
    }

    drop(stdin);
    let _ = child.wait();
    tracing::info!("pw-cat playback stopped");
}

/// Run the PipeWire microphone on the current thread (blocking).
///
/// Returns when `shutdown_rx` receives a [`Shutdown`] message.
///
/// Call from a dedicated `std::thread::spawn`.
pub fn run_pw_source(
    audio_rx: mpsc::Receiver<Vec<i16>>,
    gain_db: f32,
    sample_rate: u32,
    node_name: &str,
    node_description: &str,
    shutdown_rx: pipewire::channel::Receiver<Shutdown>,
    consumer_tx: Option<tokio::sync::mpsc::Sender<crate::consumer::ConsumerEvent>>,
) -> Result<(), pipewire::Error> {
    pipewire::init();

    if consumer_tx.is_some() {
        tracing::warn!("--mic-on-demand is not supported by the sink backend; ignoring");
    }

    // The microphone is the remapped monitor of this sink.
    let sink = sink_name(node_name);
    remove_stale(&sink, node_name);
    let Some(sink_module) = create_sink(&sink, node_description) else {
        tracing::error!("could not create virtual sink '{sink}' (is pactl available?)");
        return Ok(());
    };
    let master = format!("{sink}.monitor");
    let remap_module = create_remap(node_name, &master, node_description);
    match remap_module {
        Some(_) => tracing::info!("Microphone '{node_name}' is a remap of '{master}'"),
        None => tracing::warn!("remap source unavailable; record from '{master}' instead"),
    }

    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let stop = stop.clone();
        let sink = sink.clone();
        std::thread::spawn(move || {
            write_pcm_to_sink(audio_rx, gain_db, sample_rate, &sink, stop)
        })
    };

    // The main loop exists only to deliver the shutdown signal.
    let mainloop = MainLoopRc::new(None)?;
    let _receiver = shutdown_rx.attach(mainloop.loop_(), {
        let mainloop = mainloop.clone();
        let stop = stop.clone();
        move |_: Shutdown| {
            tracing::info!("PipeWire source shutting down");
            stop.store(true, Ordering::Relaxed);
            mainloop.quit();
        }
    });

    mainloop.run();

    let _ = writer.join();
    if let Some(module) = remap_module {
        unload(module);
    }
    unload(sink_module);

    drop(_receiver);
    tracing::info!("PipeWire source stopped");
    Ok(())
}
