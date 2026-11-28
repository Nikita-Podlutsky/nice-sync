use crate::{SyncOptions, run_sync};
use anyhow::{Context, Result, bail, ensure};
use nice_sync::store::{lock_directory, read_json, write_json};
use serde::{Deserialize, Serialize};
use std::{
    cell::RefCell,
    fs::{self, File, OpenOptions, TryLockError},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

#[derive(Default, Deserialize, Serialize)]
struct Progress {
    device: String,
    peer: String,
    share: Option<String>,
    phase: String,
    attempts: u64,
    last_success_unix: Option<u64>,
    last_error: Option<String>,
}

pub struct Worker {
    directory: PathBuf,
    progress: RefCell<Progress>,
}

impl Worker {
    pub fn ready(&self, device: &str, peer: &str, share: Option<String>) -> Result<()> {
        *self.progress.borrow_mut() = Progress {
            device: device.into(),
            peer: peer.into(),
            share,
            phase: "ready".into(),
            ..Progress::default()
        };
        self.save()
    }

    pub fn pass_started(&self) -> Result<()> {
        let mut progress = self.progress.borrow_mut();
        progress.phase = "syncing".into();
        progress.attempts += 1;
        drop(progress);
        self.save()
    }

    pub fn pass_finished(&self, result: &Result<()>) -> Result<()> {
        let mut progress = self.progress.borrow_mut();
        progress.phase = "waiting".into();
        progress.last_error = result.as_ref().err().map(|error| format!("{error:#}"));
        if result.is_ok() {
            progress.last_success_unix =
                Some(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs());
        }
        drop(progress);
        self.save()
    }

    fn save(&self) -> Result<()> {
        write_json(
            &self.directory.join("status.json"),
            &*self.progress.borrow(),
        )
    }

    pub fn stopping(&self) -> bool {
        self.directory.join("stop").is_file()
    }

    pub async fn wait(&self, interval: Duration) -> bool {
        let began = Instant::now();
        while !self.stopping() {
            let remaining = interval.saturating_sub(began.elapsed());
            if remaining.is_zero() {
                return false;
            }
            tokio::time::sleep(remaining.min(Duration::from_millis(200))).await;
        }
        true
    }
}

pub async fn run(state: &Path) -> Result<()> {
    let directory = state.join("background");
    let _worker_lock = lock_directory(&directory.join("worker"))?;
    let options: Option<SyncOptions> = read_json(&directory.join("options.json"))?;
    let options = options.context("background sync has no settings")?;
    ensure!(
        options.every.is_some(),
        "background sync requires an interval"
    );
    let worker = Worker {
        directory,
        progress: RefCell::default(),
    };
    run_sync(state, options, Some(&worker)).await
}

fn running(directory: &Path) -> Result<bool> {
    let lock = match OpenOptions::new()
        .read(true)
        .write(true)
        .open(directory.join("worker/lock"))
    {
        Ok(lock) => lock,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    match lock.try_lock() {
        Ok(()) => Ok(false),
        Err(TryLockError::WouldBlock) => Ok(true),
        Err(error) => Err(error.into()),
    }
}

fn remove(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

pub fn start(state: &Path, options: &SyncOptions) -> Result<()> {
    let directory = state.join("background");
    let _control = lock_directory(&directory.join("control"))?;
    if running(&directory)? {
        println!("Background sync is already running");
        return Ok(());
    }
    remove(&directory.join("stop"))?;
    let ready = directory.join("status.json");
    remove(&ready)?;
    write_json(&directory.join("options.json"), options)?;
    let log_path = directory.join("sync.log");
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)?;
    let mut command = Command::new(std::env::current_exe()?);
    command
        .arg("--state")
        .arg(state)
        .arg("background-worker")
        .stdin(Stdio::null())
        .stderr(log.try_clone()?)
        .stdout(log);
    detach(&mut command)?;
    let mut child = command.spawn().context("starting background sync")?;
    let start = Instant::now();
    let result = loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                break Err(anyhow::anyhow!(
                    "background sync exited ({status}); see {}",
                    log_path.display()
                ));
            }
            Ok(None) => {}
            Err(error) => break Err(error.into()),
        }
        if ready.is_file() {
            println!("Background sync started. Log: {}", log_path.display());
            return Ok(());
        }
        if start.elapsed() > Duration::from_secs(5) {
            break Err(anyhow::anyhow!(
                "background sync startup timed out; see {}",
                log_path.display()
            ));
        }
        std::thread::sleep(Duration::from_millis(25));
    };
    let _ = child.kill();
    let _ = child.wait();
    result
}

#[cfg(unix)]
fn detach(command: &mut Command) -> Result<()> {
    use std::os::unix::process::CommandExt;

    // SAFETY: setsid is async-signal-safe and only modifies the child session.
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
    Ok(())
}

#[cfg(windows)]
fn detach(command: &mut Command) -> Result<()> {
    use std::ffi::c_void;
    use std::os::windows::process::CommandExt;

    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetStdHandle(kind: u32) -> *mut c_void;
        fn SetHandleInformation(handle: *mut c_void, mask: u32, flags: u32) -> i32;
    }

    const HANDLE_FLAG_INHERIT: u32 = 1;
    // Prevent an inherited launcher pipe from keeping a caller blocked on EOF.
    for kind in [-10_i32, -11, -12] {
        // SAFETY: These are the three Win32 standard handle identifiers.
        let handle = unsafe { GetStdHandle(kind as u32) };
        if !handle.is_null() && handle.addr() != usize::MAX {
            // SAFETY: The handle belongs to this process and only its inherit bit is cleared.
            if unsafe { SetHandleInformation(handle, HANDLE_FLAG_INHERIT, 0) } == 0 {
                return Err(std::io::Error::last_os_error()).context("detaching launcher handles");
            }
        }
    }

    const DETACHED_PROCESS: u32 = 0x0000_0008;
    const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
    command.creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP);
    Ok(())
}

pub fn status(state: &Path) -> Result<()> {
    let directory = state.join("background");
    let active = running(&directory)?;
    let mut progress: Progress = read_json(&directory.join("status.json"))?;
    if !active {
        progress.phase = "stopped".into();
    } else if directory.join("stop").is_file() {
        progress.phase = "stopping".into();
    } else if progress.phase.is_empty() {
        progress.phase = "starting".into();
    }
    println!("{}", serde_json::to_string_pretty(&progress)?);
    Ok(())
}

pub fn stop(state: &Path) -> Result<()> {
    let directory = state.join("background");
    let _control = lock_directory(&directory.join("control"))?;
    if !running(&directory)? {
        bail!("background sync is not running");
    }
    File::create(directory.join("stop"))?;
    println!("Stop requested. The current synchronization pass can finish.");
    Ok(())
}
