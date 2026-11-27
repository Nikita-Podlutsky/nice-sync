use crate::{
    language::{Applied, Mode, Network},
    peer::{Answer, Client, Request},
    published::{install, local_blob, remove},
    store::{Blob, Version, copy_verified, fingerprint, is_link, read_json, scan, write_json},
    transfer::{PartialFile, read_chunk},
};
use anyhow::{Context, Result, bail, ensure};
use futures_util::future::join_all;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

const DOWNLOAD_WINDOW: usize = 8;

#[derive(Default, Deserialize, Serialize)]
struct Tracked {
    local: Option<Blob>,
    remote: Option<u64>,
}

type Tracking = BTreeMap<String, Tracked>;

pub async fn sync(
    client: &Client,
    state: &Path,
    applied: &Applied,
    network: &Network,
    only: Option<&str>,
) -> Result<()> {
    let mut shares = Vec::new();
    for (name, share) in &network.shares {
        if only.is_some_and(|only| only != name) {
            continue;
        }
        if let Some(rule) = share.rules.get(&applied.device) {
            let configured = applied.base.join(&rule.path);
            if rule.mode == Mode::Checkout {
                fs::create_dir_all(&configured)?;
            }
            let metadata = fs::symlink_metadata(&configured)
                .with_context(|| format!("opening local folder {}", configured.display()))?;
            ensure!(
                metadata.is_dir() && !is_link(&metadata),
                "unsafe local folder: {}",
                configured.display()
            );
            let root = configured.canonicalize()?;
            let state = state.canonicalize()?;
            ensure!(
                !root.starts_with(&state) && !state.starts_with(&root),
                "local folder and Nice Sync state overlap"
            );
            ensure!(
                !shares
                    .iter()
                    .any(|(_, _, other): &(_, _, PathBuf)| root.starts_with(other)
                        || other.starts_with(&root)),
                "local share roots overlap"
            );
            shares.push((name, share, root));
        }
    }
    if let Some(name) = only {
        ensure!(
            shares.iter().any(|(candidate, _, _)| *candidate == name),
            "no sync rule for '{name}'"
        );
    }
    ensure!(!shares.is_empty(), "the local device has no sync rules");
    let mut failures = Vec::new();
    for (name, share, root) in shares {
        let rule = &share.rules[&applied.device];
        let ignored = |path: &str, directory| network.ignored(share, Some(rule), path, directory);
        let mut location = blake3::Hasher::new();
        location.update(root.as_os_str().as_encoded_bytes());
        location.update(format!("{:?}", rule.mode).as_bytes());
        let job = Job {
            client,
            share: name,
            directory: state
                .join("sync")
                .join(name)
                .join(location.finalize().to_hex().as_str()),
            root,
            mode: rule.mode,
            propagate: rule.propagate_delete,
        };
        match job.run(ignored).await {
            Ok(count) => println!("{name}: {count} changes"),
            Err(error) => failures.push(format!("{name}: {error:#}")),
        }
    }
    ensure!(failures.is_empty(), "{}", failures.join("\n"));
    Ok(())
}

struct Job<'a> {
    client: &'a Client,
    share: &'a str,
    directory: PathBuf,
    root: PathBuf,
    mode: Mode,
    propagate: bool,
}

impl Job<'_> {
    async fn run(&self, ignored: impl Fn(&str, bool) -> bool) -> Result<usize> {
        fs::create_dir_all(&self.directory)?;
        let tracking_file = self.directory.join("tracking.json");
        let mut tracking: Tracking = read_json(&tracking_file)?;
        if tracking.is_empty() {
            eprintln!("{}: scanning local files", self.share);
        }
        let files = scan(&self.root, &ignored)?;
        let before = files
            .iter()
            .map(|(path, file)| Ok((path.clone(), fingerprint(&mut File::open(file)?)?)))
            .collect::<Result<BTreeMap<_, _>>>()?;
        let mut count = 0;
        if self.propagate {
            for (path, entry) in &tracking {
                if entry.local.is_some() && !before.contains_key(path) && !ignored(path, false) {
                    let actual = local_blob(&self.root, path)?;
                    ensure!(actual.is_none(), "file reappeared during scan: {path}");
                    commit(self.client, self.share, path, None, entry.remote).await?;
                    count += 1;
                }
            }
        }
        for (path, file) in &files {
            let blob = &before[path];
            if self.mode.uploads()
                && tracking.get(path).and_then(|entry| entry.local.as_ref()) != Some(blob)
            {
                let base = tracking.get(path).and_then(|entry| entry.remote);
                let target = upload(self.client, self.share, path, file, blob, base).await?;
                if target != *path {
                    eprintln!("Conflict in {path}: preserved incoming bytes as {target}");
                }
                if !self.mode.downloads() {
                    tracking.insert(
                        path.clone(),
                        Tracked {
                            local: Some(blob.clone()),
                            remote: None,
                        },
                    );
                    write_json(&tracking_file, &tracking)?;
                }
                count += 1;
            }
        }
        if !self.mode.downloads() {
            return Ok(count);
        }
        if tracking.is_empty() {
            eprintln!("{}: requesting the file list from the host", self.share);
        }
        let (answer, _) = self
            .client
            .call(
                Request::Manifest {
                    share: self.share.to_owned(),
                },
                &[],
            )
            .await?;
        let Answer::Manifest(heads) = answer else {
            bail!("expected publication manifest")
        };
        // Remove files first so a former file can become a parent directory.
        let should_download = |path: &str, version: &Version, tracking: &Tracking| {
            let prior = before.get(path);
            version
                .blob
                .as_ref()
                .is_some_and(|blob| Some(blob) != prior)
                && (self.mode != Mode::Checkout
                    || prior.is_none()
                    || tracking.get(path).and_then(|entry| entry.local.as_ref()) == prior)
        };
        let total = heads
            .iter()
            .filter(|(path, version)| {
                !ignored(path, false) && should_download(path, version, &tracking)
            })
            .count();
        if total > 0 {
            eprintln!("{}: downloading {total} files", self.share);
        }
        let download_directory = self.directory.join("download");
        let mut completed = 0;
        let mut bytes = 0;
        let mut last_progress = Instant::now();
        let mut changes = heads
            .iter()
            .filter(|(_, version)| version.blob.is_none())
            .chain(heads.iter().filter(|(_, version)| version.blob.is_some()))
            .filter(|(path, _)| !ignored(path, false))
            .peekable();
        let mut skipped = Vec::new();
        while changes.peek().is_some() {
            let mut batch = Vec::with_capacity(DOWNLOAD_WINDOW);
            let mut hashes = BTreeSet::new();
            while batch.len() < DOWNLOAD_WINDOW {
                let Some(&(path, version)) = changes.peek() else {
                    break;
                };
                let download = should_download(path, version, &tracking);
                // The temporary file is shared by all paths with the same content.
                if download
                    && let Some(blob) = &version.blob
                    && !hashes.insert(&blob.hash)
                {
                    break;
                }
                changes.next();
                batch.push((path, version, download));
            }
            let incoming = join_all(batch.iter().map(|&(path, version, download)| {
                let directory = &download_directory;
                async move {
                    if download {
                        fetch(self.client, directory, self.share, path, version, true)
                            .await
                            .map(Some)
                    } else {
                        Ok(None)
                    }
                }
            }))
            .await;
            for ((path, version, _), partial) in batch.into_iter().zip(incoming) {
                let prior = before.get(path);
                if prior == version.blob.as_ref()
                    && tracking.get(path).is_some_and(|entry| {
                        entry.local.as_ref() == prior && entry.remote == Some(version.id)
                    })
                {
                    continue;
                }
                let actual = local_blob(&self.root, path)?;
                if actual.as_ref() != prior {
                    skipped.push(format!("{path} changed during synchronization"));
                    continue;
                }
                let keep = self.mode == Mode::Mirror && !self.propagate && version.blob.is_none();
                if !keep && actual != version.blob {
                    let previous = tracking.get(path).and_then(|entry| entry.local.as_ref());
                    if self.mode == Mode::Checkout
                        && actual.is_some()
                        && actual.as_ref() != previous
                    {
                        skipped.push(format!(
                            "{path} has local changes; move it aside before retrying"
                        ));
                        continue;
                    }
                    match &version.blob {
                        Some(blob) => {
                            let partial = partial?.context("missing downloaded file")?;
                            if local_blob(&self.root, path)? != actual {
                                skipped.push(format!("{path} changed during download"));
                                continue;
                            }
                            install(&self.root, path, partial.verified()?, blob)?;
                            partial.remove()?;
                            completed += 1;
                            bytes += blob.size;
                            if completed == total
                                || last_progress.elapsed() >= Duration::from_secs(2)
                            {
                                eprintln!(
                                    "{}: downloaded {completed}/{total} files ({:.1} MiB)",
                                    self.share,
                                    bytes as f64 / (1024.0 * 1024.0)
                                );
                                last_progress = Instant::now();
                            }
                        }
                        None => remove(&self.root, path)?,
                    }
                    count += 1;
                }
                let local = if keep { actual } else { version.blob.clone() };
                if tracking
                    .get(path)
                    .is_none_or(|entry| entry.local != local || entry.remote != Some(version.id))
                {
                    tracking.insert(
                        path.clone(),
                        Tracked {
                            local,
                            remote: Some(version.id),
                        },
                    );
                    write_json(&tracking_file, &tracking)?;
                }
            }
        }
        ensure!(skipped.is_empty(), "{}", skipped.join("\n"));
        Ok(count)
    }
}

async fn upload(
    client: &Client,
    share: &str,
    path: &str,
    source: &Path,
    blob: &Blob,
    base: Option<u64>,
) -> Result<String> {
    let (answer, _) = client
        .call(
            Request::Prepare {
                share: share.to_owned(),
                path: path.to_owned(),
                blob: blob.clone(),
            },
            &[],
        )
        .await?;
    let Answer::Offset(mut offset) = answer else {
        bail!("expected transfer offset")
    };
    ensure!(offset <= blob.size, "invalid server offset");
    while offset < blob.size {
        let data = read_chunk(source, offset, blob.size)?;
        let (answer, _) = client
            .call(
                Request::Chunk {
                    share: share.to_owned(),
                    path: path.to_owned(),
                    blob: blob.clone(),
                    offset,
                },
                &data,
            )
            .await?;
        let Answer::Offset(next) = answer else {
            bail!("expected transfer offset")
        };
        ensure!(
            next == offset + data.len() as u64,
            "unexpected server offset"
        );
        offset = next;
    }
    commit(client, share, path, Some(blob.clone()), base).await
}

async fn commit(
    client: &Client,
    share: &str,
    path: &str,
    blob: Option<Blob>,
    base: Option<u64>,
) -> Result<String> {
    let (answer, _) = client
        .call(
            Request::Commit {
                share: share.to_owned(),
                path: path.to_owned(),
                blob,
                base,
            },
            &[],
        )
        .await?;
    let Answer::Committed(path) = answer else {
        bail!("expected commit confirmation")
    };
    Ok(path)
}

pub async fn get(
    client: &Client,
    state: &Path,
    share: &str,
    path: &str,
    revision: Option<u64>,
    destination: &Path,
) -> Result<()> {
    let (answer, chunk) = client
        .call(
            Request::Read {
                share: share.to_owned(),
                path: path.to_owned(),
                version: revision,
                offset: 0,
                sync: false,
            },
            &[],
        )
        .await?;
    let Answer::File(version) = answer else {
        bail!("expected file information")
    };
    let blob = version.blob.as_ref().context("file has been deleted")?;
    let directory = state.join("downloads");
    let partial = PartialFile::new(&directory, blob)?;
    if partial.offset()? == 0 && !chunk.is_empty() {
        partial.append(0, &chunk)?;
    }
    let partial = fetch(client, &directory, share, path, &version, false).await?;
    copy_verified(partial.verified()?, destination, blob)?;
    partial.remove()?;
    Ok(())
}

async fn fetch(
    client: &Client,
    directory: &Path,
    share: &str,
    path: &str,
    version: &Version,
    sync: bool,
) -> Result<PartialFile> {
    let blob = version.blob.as_ref().context("file has been deleted")?;
    let partial = PartialFile::new(directory, blob)?;
    let mut offset = partial.offset()?;
    let mut last_progress = Instant::now();
    while offset < blob.size {
        let (answer, data) = client
            .call(
                Request::Read {
                    share: share.to_owned(),
                    path: path.to_owned(),
                    version: Some(version.id),
                    offset,
                    sync,
                },
                &[],
            )
            .await?;
        let Answer::File(current) = answer else {
            bail!("expected file bytes")
        };
        ensure!(current == *version, "unexpected file revision");
        offset = partial.append(offset, &data)?;
        if blob.size >= 32 * 1024 * 1024 && last_progress.elapsed() >= Duration::from_secs(2) {
            eprintln!(
                "{share}/{path}: {:.0}/{:.0} MiB",
                offset as f64 / (1024.0 * 1024.0),
                blob.size as f64 / (1024.0 * 1024.0)
            );
            last_progress = Instant::now();
        }
    }
    Ok(partial)
}
