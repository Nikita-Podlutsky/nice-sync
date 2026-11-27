use super::{Job, Tracked, Tracking};
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
};

impl Job<'_> {
    pub(super) async fn run_bulk(
        &self,
        files: BTreeMap<String, PathBuf>,
        before: BTreeMap<String, Blob>,
        mut tracking: Tracking,
        mut heads: Heads,
        ignored: &impl Fn(&str, bool) -> bool,
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
        for batch in changes.chunks(PACK_ENTRIES) {
            self.send_pack(batch, &files).await?;
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
        self.receive_packs(heads, &before, &mut tracking, ignored)
            .await
            .map(|count| count + changes.len())
    }

    async fn send_pack(&self, changes: &[Change], files: &BTreeMap<String, PathBuf>) -> Result<()> {
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
                    self.send_bytes(std::mem::take(&mut parts), &bytes).await?;
                    bytes.clear();
                }
            }
        }
        if !parts.is_empty() {
            self.send_bytes(parts, &bytes).await?;
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
        for (change, path) in changes.iter().zip(paths) {
            if path != change.path {
                eprintln!(
                    "Conflict in {}: preserved incoming bytes as {path}",
                    change.path
                );
            }
        }
        Ok(())
    }

    async fn send_bytes(&self, parts: Vec<WritePart>, bytes: &[u8]) -> Result<()> {
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
            count += records.len();
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
            for file in &mut downloads[start..cursor] {
                file.bytes = None;
            }
        }
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
