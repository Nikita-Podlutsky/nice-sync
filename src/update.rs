use anyhow::Result;

#[cfg(target_os = "linux")]
pub fn run() -> Result<()> {
    use anyhow::{Context, ensure};
    use nice_sync::store::lock_directory;
    use std::{env, fs, path::PathBuf, process::Command};

    const REPOSITORY: &str = "https://github.com/Nikita-Podlutsky/nice-sync.git";
    let executable = env::current_exe()?;
    let bin = executable.parent().context("executable has no directory")?;
    ensure!(
        executable
            .file_name()
            .is_some_and(|name| name == "nice-sync")
            && bin.file_name().is_some_and(|name| name == "bin"),
        "install Nice Sync in a bin directory before running update"
    );

    let cache = bin.join(".nice-sync-update");
    let _lock = lock_directory(&cache)?;
    let staging = tempfile::tempdir_in(bin).context("creating update staging directory")?;
    let cargo = env::home_dir()
        .map(|home| home.join(".cargo/bin/cargo"))
        .filter(|path| path.is_file())
        .unwrap_or_else(|| PathBuf::from("cargo"));
    println!("Building Nice Sync from GitHub (main)…");
    let status = Command::new(cargo)
        .args([
            "install", "--git", REPOSITORY, "--branch", "main", "--locked", "--root",
        ])
        .arg(staging.path())
        .arg("nice-sync")
        .env("CARGO_TARGET_DIR", cache.join("target"))
        .status()
        .context("starting Cargo")?;
    ensure!(status.success(), "Cargo update failed ({status})");
    let built = staging.path().join("bin/nice-sync");
    fs::File::open(&built)?.sync_all()?;
    fs::rename(built, &executable)
        .with_context(|| format!("installing {}", executable.display()))?;
    println!("Updated {}", executable.display());
    Ok(())
}

#[cfg(not(target_os = "linux"))]
pub fn run() -> Result<()> {
    anyhow::bail!("automatic updates are available on Linux");
}
