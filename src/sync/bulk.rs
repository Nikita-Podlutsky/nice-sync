use super::{FolderProgress, Job, Tracked, Tracking};
use crate::{
    language::Mode,
    peer::{Answer, ReadPart, Request, WritePart},
    published::{Change, Heads, install, install_bytes, local_blob, remove},
    store::{Blob, read_json, write_json},
    transfer::{PACK_BYTES, PACK_ENTRIES, PartialFile, read_range},
};
use anyhow::{Result, bail, ensure};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::PathBuf,
    time::Instant,
};

impl Job<'_> {
    pub(super) async fn run_bulk(
        &self,
        files: BTreeMap<String, PathBuf>,
        before: BTreeMap<String, Blob>,
        mut tracking: Tracking,
        mut heads: Heads,
        ignored: &impl Fn(&str, bool) -> bool,
        status: &mut FolderProgress,
    ) -> Result<usize> {
        let pending = self.directory.join("pending.json");
        let saved = self.directory.join("tracking.json");
        if pending.exists() {
            for (path, record) in read_json::<Tracking>(&pending)? {
                if local_blob(&self.root, &path)? == record.local {
                    tracking.insert(path, record);
                }
            }
            write_json(&saved, &tracking)?;
            fs::remove_file(&pending)?;
        }

        let mut changes = Vec::new();
        if self.propagate {
            for (path, entry) in &tracking {
                if entry.local.is_some() && !before.contains_key(path) && !ignored(path, false) {
                    ensure!(
                        local_blob(&self.root, path)?.is_none(),
                        "file reappeared during scan: {path}"
                    );
                    changes.push(Change {
                        path: path.clone(),
                        base: entry.remote,
                        blob: None,
                    });
                }
            }
        }
        if self.mode.uploads() {
            for (path, blob) in &before {
                let tracked = tracking.get(path);
                if tracked.and_then(|entry| entry.local.as_ref()) != Some(blob)
                    && heads.get(path).and_then(|version| version.blob.as_ref()) != Some(blob)
                {
                    changes.push(Change {
                        path: path.clone(),
                        base: tracked.and_then(|entry| entry.remote),
                        blob: Some(blob.clone()),
                    });
                }
            }
        }
        let mut meter = Meter::new(
            "upload",
            status,
            changes
                .iter()
                .filter_map(|c| c.blob.as_ref())
                .map(|b| b.size)
                .sum(),
            changes.len(),
            0,
        );
        for (index, batch) in changes.chunks(PACK_ENTRIES).enumerate() {
            meter.status.upcoming = changes[index * PACK_ENTRIES..]
                .iter()
                .take(5)
                .map(|change| change.path.clone())
                .collect();
            self.report(meter.status);
            self.send_pack(batch, &files, &mut meter).await?;
            if !self.mode.downloads() {
                for change in batch {
                    tracking.insert(
                        change.path.clone(),
                        Tracked {
                            local: change.blob.clone(),
                            remote: None,
                        },
                    );
                }
                write_json(&saved, &tracking)?;
            }
        }
        if !self.mode.downloads() {
            return Ok(changes.len());
        }
        if !changes.is_empty() {
            let (Answer::Manifest(current), _) = self
                .client
                .call(
                    Request::BulkHeads {
                        share: self.share.to_owned(),
                    },
                    &[],
                )
                .await?
            else {
                bail!("expected the committed index");
            };
            heads = current;
        }
        self.receive_packs(heads, &before, &mut tracking, ignored, status)
            .await
            .map(|count| count + changes.len())
    }

    async fn send_pack(
        &self,
        changes: &[Change],
        files: &BTreeMap<String, PathBuf>,
        meter: &mut Meter<'_>,
    ) -> Result<()> {
        let (Answer::BulkOffsets(offsets), _) = self
            .client
            .call(
                Request::BulkPrepare {
                    share: self.share.to_owned(),
                    changes: clone_changes(changes),
                },
                &[],
            )
            .await?
        else {
            bail!("expected upload offsets")
        };
        ensure!(
            offsets.len() == changes.len(),
            "unexpected upload offset count"
        );
        let mut parts = Vec::new();
        let mut bytes = Vec::new();
        let mut sent = BTreeSet::new();
        let previous = meter.status.completed_bytes;
        let batch_bytes: u64 = changes
            .iter()
            .filter_map(|c| c.blob.as_ref())
            .map(|b| b.size)
            .sum();
        for (change, mut offset) in changes.iter().zip(offsets) {
            let Some(blob) = &change.blob else { continue };
            if !sent.insert(&blob.hash) {
                continue;
            }
            ensure!(offset <= blob.size, "upload offset exceeds file size");
            while offset < blob.size {
                let length = (blob.size - offset).min((PACK_BYTES - bytes.len()) as u64) as usize;
                bytes.extend(read_range(&files[&change.path], offset, length)?);
                parts.push(WritePart {
                    path: change.path.clone(),
                    blob: blob.clone(),
                    offset,
                    length,
                });
                offset += length as u64;
                if bytes.len() == PACK_BYTES || parts.len() == PACK_ENTRIES {
                    self.send_bytes(std::mem::take(&mut parts), &bytes, meter)
                        .await?;
                    bytes.clear();
                }
            }
        }
        if !parts.is_empty() {
            self.send_bytes(parts, &bytes, meter).await?;
        }
        let (Answer::BulkCommitted(paths), _) = self
            .client
            .call(
                Request::BulkCommit {
                    share: self.share.to_owned(),
                    changes: clone_changes(changes),
                },
                &[],
            )
            .await?
        else {
            bail!("expected upload confirmation")
        };
        ensure!(
            paths.len() == changes.len(),
            "unexpected upload confirmation count"
        );
        meter.status.completed_bytes = previous + batch_bytes;
        meter.status.completed_files += changes.len();
        for (change, path) in changes.iter().zip(paths) {
            if path != change.path {
                meter.status.conflicts += 1;
                eprintln!(
                    "Conflict in {}: preserved incoming bytes as {path}",
                    change.path
                );
            }
        }
        if meter.status.completed_files == meter.status.total_files {
            meter.status.upcoming.clear();
        }
        meter.update(self, 0, None);
        Ok(())
    }

    async fn send_bytes(
        &self,
        parts: Vec<WritePart>,
        bytes: &[u8],
        meter: &mut Meter<'_>,
    ) -> Result<()> {
        let path = parts.last().map(|part| part.path.clone());
        let (answer, _) = self
            .client
            .call(
                Request::BulkWrite {
                    share: self.share.to_owned(),
                    parts,
                },
                bytes,
            )
            .await?;
        ensure!(
            matches!(answer, Answer::BulkData),
            "expected upload packet confirmation"
        );
        meter.update(self, bytes.len() as u64, path);
        eprintln!(
            "{}: sent {:.2} MiB",
            self.share,
            bytes.len() as f64 / (1024.0 * 1024.0)
        );
        Ok(())
    }

    async fn receive_packs(
        &self,
        heads: Heads,
        before: &BTreeMap<String, Blob>,
        tracking: &mut Tracking,
        ignored: &impl Fn(&str, bool) -> bool,
        status: &mut FolderProgress,
    ) -> Result<usize> {
        let mut records = Tracking::new();
        let mut removals = Vec::new();
        let mut downloads = BTreeMap::<String, Download>::new();
        let mut skipped = Vec::new();
        let staging = self.directory.join("download");
        for (path, version) in heads {
            if ignored(&path, false) {
                continue;
            }
            let prior = before.get(&path);
            let keep = self.mode == Mode::Mirror && !self.propagate && version.blob.is_none();
            if !keep && version.blob.as_ref() != prior {
                if self.mode == Mode::Checkout
                    && prior.is_some()
                    && tracking.get(&path).and_then(|entry| entry.local.as_ref()) != prior
                {
                    skipped.push(path);
                    continue;
                }
                if let Some(blob) = &version.blob {
                    let download = match downloads.entry(blob.hash.clone()) {
                        std::collections::btree_map::Entry::Occupied(entry) => entry.into_mut(),
                        std::collections::btree_map::Entry::Vacant(entry) => {
                            let partial = PartialFile::new(&staging, blob)?;
                            entry.insert(Download {
                                offset: partial.offset()?,
                                partial,
                                blob: blob.clone(),
                                paths: Vec::new(),
                                bytes: None,
                            })
                        }
                    };
                    ensure!(download.blob == *blob, "inconsistent blob metadata");
                    download.paths.push((path, version.id));
                    continue;
                }
                if local_blob(&self.root, &path)?.as_ref() != prior {
                    skipped.push(path);
                    continue;
                }
                removals.push(path.clone());
            }
            let record = Tracked {
                local: if keep { prior.cloned() } else { version.blob },
                remote: Some(version.id),
            };
            if tracking.get(&path) != Some(&record) {
                records.insert(path, record);
            }
        }
        let mut count = removals.len();
        self.checkpoint(tracking, records, || {
            for path in &removals {
                ensure!(
                    local_blob(&self.root, path)?.as_ref() == before.get(path),
                    "file changed during sync: {path}"
                );
                remove(&self.root, path)?;
            }
            Ok(())
        })?;

        let mut downloads = downloads.into_values().collect::<Vec<_>>();
        let mut meter = Meter::new(
            "download",
            status,
            downloads.iter().map(|file| file.blob.size).sum(),
            downloads.iter().map(|file| file.paths.len()).sum(),
            downloads.iter().map(|file| file.offset).sum(),
        );
        meter.status.upcoming = downloads
            .iter()
            .flat_map(|file| &file.paths)
            .take(5)
            .map(|(path, _)| path.clone())
            .collect();
        self.report(meter.status);
        let mut cursor = 0;
        while cursor < downloads.len() {
            let mut packet = Vec::new();
            let mut indices = Vec::new();
            let mut remaining = PACK_BYTES;
            let start = cursor;
            while cursor < downloads.len()
                && packet.len() < PACK_ENTRIES
                && cursor - start < PACK_ENTRIES
                && remaining > 0
            {
                let file = &downloads[cursor];
                let length = (file.blob.size - file.offset).min(remaining as u64) as usize;
                if length > 0 {
                    let (path, version) = &file.paths[0];
                    packet.push(ReadPart {
                        path: path.clone(),
                        version: *version,
                        offset: file.offset,
                        length,
                    });
                    indices.push((cursor, length));
                    remaining -= length;
                }
                if file.offset + length as u64 != file.blob.size {
                    break;
                }
                cursor += 1;
            }
            if !packet.is_empty() {
                let path = packet.last().map(|part| part.path.clone());
                let (answer, data) = self
                    .client
                    .call(
                        Request::BulkRead {
                            share: self.share.to_owned(),
                            parts: packet,
                        },
                        &[],
                    )
                    .await?;
                ensure!(
                    matches!(answer, Answer::BulkData) && data.len() == PACK_BYTES - remaining,
                    "invalid download packet"
                );
                let mut position = 0;
                for (index, length) in indices {
                    let file = &mut downloads[index];
                    let bytes = &data[position..position + length];
                    if file.offset == 0 && length as u64 == file.blob.size {
                        file.bytes = Some(bytes.to_vec());
                        file.offset = file.blob.size;
                    } else {
                        file.offset = file.partial.append(file.offset, bytes)?;
                    }
                    position += length;
                }
                meter.update(self, data.len() as u64, path);
                eprintln!(
                    "{}: received {:.2} MiB",
                    self.share,
                    data.len() as f64 / (1024.0 * 1024.0)
                );
            }
            let mut records = Tracking::new();
            for file in &downloads[start..cursor] {
                for (path, version) in &file.paths {
                    if local_blob(&self.root, path)?.as_ref() == before.get(path) {
                        records.insert(
                            path.clone(),
                            Tracked {
                                local: Some(file.blob.clone()),
                                remote: Some(*version),
                            },
                        );
                    } else {
                        skipped.push(path.clone());
                    }
                }
            }
            let installed = records.len();
            count += installed;
            self.checkpoint(tracking, records.clone(), || {
                for file in &downloads[start..cursor] {
                    let source = if file.bytes.is_none() {
                        Some(file.partial.verified()?)
                    } else {
                        None
                    };
                    for (path, _) in &file.paths {
                        if records.contains_key(path) {
                            ensure!(
                                local_blob(&self.root, path)?.as_ref() == before.get(path),
                                "file changed during sync: {path}"
                            );
                            if let Some(bytes) = &file.bytes {
                                install_bytes(&self.root, path, bytes, &file.blob)?;
                            } else if let Some(source) = source {
                                install(&self.root, path, source, &file.blob)?;
                            }
                        }
                    }
                    file.partial.remove()?;
                }
                Ok(())
            })?;
            if installed > 0 {
                meter.status.completed_files += installed;
                meter.status.upcoming = downloads[cursor..]
                    .iter()
                    .flat_map(|file| &file.paths)
                    .take(5)
                    .map(|(path, _)| path.clone())
                    .collect();
                meter.update(self, 0, None);
            }
            for file in &mut downloads[start..cursor] {
                file.bytes = None;
            }
        }
        meter.status.conflicts += skipped.len();
        ensure!(
            skipped.is_empty(),
            "local files changed or need to be moved aside: {}",
            skipped.join(", ")
        );
        Ok(count)
    }

    fn checkpoint(
        &self,
        tracking: &mut Tracking,
        records: Tracking,
        apply: impl FnOnce() -> Result<()>,
    ) -> Result<()> {
        if records.is_empty() {
            return apply();
        }
        let pending = self.directory.join("pending.json");
        write_json(&pending, &records)?;
        apply()?;
        tracking.extend(records);
        write_json(&self.directory.join("tracking.json"), tracking)?;
        fs::remove_file(pending)?;
        Ok(())
    }
}

struct Download {
    partial: PartialFile,
    offset: u64,
    blob: Blob,
    paths: Vec<(String, u64)>,
    bytes: Option<Vec<u8>>,
}

struct Meter<'a> {
    status: &'a mut FolderProgress,
    started: Instant,
    transferred: u64,
}

impl<'a> Meter<'a> {
    fn new(
        direction: &str,
        status: &'a mut FolderProgress,
        total: u64,
        files: usize,
        resumed: u64,
    ) -> Self {
        status.phase = "transferring".into();
        status.direction = Some(direction.into());
        status.path = None;
        status.upcoming.clear();
        status.total_bytes = Some(total);
        status.completed_bytes = resumed;
        status.total_files = files;
        status.completed_files = 0;
        status.bytes_per_second = 0;
        status.eta_seconds = None;
        Self {
            status,
            started: Instant::now(),
            transferred: 0,
        }
    }

    fn update(&mut self, job: &Job<'_>, bytes: u64, path: Option<String>) {
        self.transferred += bytes;
        self.status.completed_bytes += bytes;
        if path.is_some() {
            self.status.path = path;
        }
        self.status.bytes_per_second =
            (self.transferred as f64 / self.started.elapsed().as_secs_f64().max(0.001)) as u64;
        let remaining = self
            .status
            .total_bytes
            .unwrap_or_default()
            .saturating_sub(self.status.completed_bytes);
        self.status.eta_seconds = (self.status.bytes_per_second > 0)
            .then(|| remaining.div_ceil(self.status.bytes_per_second));
        job.report(self.status);
    }
}

fn clone_changes(changes: &[Change]) -> Vec<Change> {
    changes
        .iter()
        .map(|change| Change {
            path: change.path.clone(),
            base: change.base,
            blob: change.blob.clone(),
        })
        .collect()
}
