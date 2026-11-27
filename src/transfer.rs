use crate::store::{Blob, fingerprint};
use anyhow::{Context, Result, ensure};
use std::{
    fs::{self, File},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

pub const CHUNK: usize = 1024 * 1024;
pub const SMALL_FILE: u64 = 16 * 1024;
pub const SMALL_BATCH: usize = CHUNK / SMALL_FILE as usize;

pub struct PartialFile {
    blob: Blob,
    path: PathBuf,
}

impl PartialFile {
    pub fn new(directory: &Path, blob: &Blob) -> Result<Self> {
        blob.validate()?;
        fs::create_dir_all(directory)?;
        Ok(Self {
            blob: blob.clone(),
            path: directory.join(&blob.hash),
        })
    }

    pub fn offset(&self) -> Result<u64> {
        match fs::metadata(&self.path) {
            Ok(metadata) if metadata.len() <= self.blob.size => Ok(metadata.len()),
            Ok(_) => {
                fs::remove_file(&self.path)?;
                Ok(0)
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(0),
            Err(error) => Err(error.into()),
        }
    }

    pub fn append(&self, offset: u64, data: &[u8]) -> Result<u64> {
        let length = self.offset()?;
        ensure!(
            length == offset,
            "incorrect transfer offset, expected {length}"
        );
        ensure!(
            !data.is_empty() && data.len() <= CHUNK && data.len() as u64 <= self.blob.size - offset,
            "invalid chunk length"
        );
        let mut file = File::options()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&self.path)?;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(data)?;
        file.sync_data()?;
        Ok(offset + data.len() as u64)
    }

    pub fn verified(&self) -> Result<&Path> {
        if self.blob.size == 0 && !self.path.exists() {
            File::create_new(&self.path)?.sync_all()?;
        }
        let mut file = File::open(&self.path).context("transfer has not started")?;
        ensure!(
            file.metadata()?.len() == self.blob.size,
            "transfer is incomplete"
        );
        let actual = fingerprint(&mut file)?;
        drop(file);
        if actual != self.blob {
            self.remove()?;
            anyhow::bail!("transfer hash mismatch");
        }
        Ok(&self.path)
    }

    pub fn remove(&self) -> Result<()> {
        match fs::remove_file(&self.path) {
            Err(error) if error.kind() != io::ErrorKind::NotFound => Err(error.into()),
            _ => Ok(()),
        }
    }
}

pub fn read_chunk(source: &Path, offset: u64, size: u64) -> Result<Vec<u8>> {
    ensure!(offset <= size, "offset exceeds size");
    let mut file = File::open(source)?;
    file.seek(SeekFrom::Start(offset))?;
    let mut data = vec![0; (size - offset).min(CHUNK as u64) as usize];
    file.read_exact(&mut data)?;
    Ok(data)
}
