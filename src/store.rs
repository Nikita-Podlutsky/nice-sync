use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{
    collections::BTreeMap,
    fs::{self, File},
    io::{self, Seek, SeekFrom, Write},
    path::{Component, Path, PathBuf},
};
use tempfile::NamedTempFile;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Blob {
    pub hash: String,
    pub size: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Version {
    pub id: u64,
    pub parent: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_path: Option<String>,
    pub author: String,
    pub blob: Option<Blob>,
}

pub type Catalog = BTreeMap<String, Vec<Version>>;

pub struct Store {
    root: PathBuf,
    catalog: Catalog,
    _lock: File,
}

pub fn lock_directory(path: &Path) -> Result<File> {
    fs::create_dir_all(path)?;
    let lock = File::options()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path.join("lock"))?;
    lock.try_lock()
        .with_context(|| format!("state directory is in use: {}", path.display()))?;
    Ok(lock)
}

pub fn read_json<T: DeserializeOwned + Default>(path: &Path) -> Result<T> {
    match File::open(path) {
        Ok(file) => serde_json::from_reader(io::BufReader::new(file))
            .with_context(|| format!("invalid state: {}", path.display())),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(T::default()),
        Err(error) => Err(error.into()),
    }
}

pub fn write_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let mut temporary = temporary_at(path)?;
    serde_json::to_writer(&mut temporary, value)?;
    persist(temporary, path)
}

pub fn write_text(path: &Path, source: &str) -> Result<()> {
    let mut temporary = temporary_at(path)?;
    temporary.write_all(source.as_bytes())?;
    persist(temporary, path)
}

fn temporary_at(path: &Path) -> Result<NamedTempFile> {
    let directory = path
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    Ok(NamedTempFile::new_in(directory)?)
}

fn persist(temporary: NamedTempFile, path: &Path) -> Result<()> {
    temporary.as_file().sync_all()?;
    temporary.persist(path)?;
    #[cfg(unix)]
    if let Some(directory) = path.parent().filter(|path| !path.as_os_str().is_empty()) {
        File::open(directory)?.sync_all()?;
    }
    Ok(())
}

pub fn validate_path(path: &str) -> Result<()> {
    ensure!(
        !path.is_empty() && path.len() <= 4096,
        "invalid path length"
    );
    for name in path.split('/') {
        let stem = name
            .split('.')
            .next()
            .unwrap()
            .trim_end()
            .to_ascii_uppercase();
        let numbered_device = ["COM", "LPT"].iter().any(|prefix| {
            stem.strip_prefix(prefix).is_some_and(|suffix| {
                [
                    "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "¹", "²", "³",
                ]
                .contains(&suffix)
            })
        });
        ensure!(
            !name.is_empty()
                && name.len() <= 255
                && !name.ends_with(['.', ' '])
                && !name
                    .chars()
                    .any(|c| c.is_control() || "<>:\"\\|?*".contains(c))
                && !matches!(
                    stem.as_str(),
                    "CON" | "PRN" | "AUX" | "NUL" | "CONIN$" | "CONOUT$"
                )
                && !numbered_device,
            "invalid portable path: {path}"
        );
    }
    Ok(())
}

pub fn is_link(metadata: &fs::Metadata) -> bool {
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        metadata.file_attributes() & 0x400 != 0
    }
    #[cfg(not(windows))]
    metadata.file_type().is_symlink()
}

pub fn local_path(root: &Path, name: &str) -> Result<PathBuf> {
    validate_path(name)?;
    let mut path = root.to_owned();
    if let Ok(metadata) = fs::symlink_metadata(root) {
        ensure!(
            metadata.is_dir() && !is_link(&metadata),
            "unsafe root: {}",
            root.display()
        );
    }
    for component in name.split('/') {
        path.push(component);
        match fs::symlink_metadata(&path) {
            Ok(metadata) => ensure!(!is_link(&metadata), "refusing link: {}", path.display()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(path)
}

pub fn scan(
    root: &Path,
    ignored: impl Fn(&str, bool) -> bool,
) -> Result<BTreeMap<String, PathBuf>> {
    let metadata =
        fs::symlink_metadata(root).with_context(|| format!("opening {}", root.display()))?;
    ensure!(
        metadata.is_dir() && !is_link(&metadata),
        "expected a directory: {}",
        root.display()
    );
    let mut files = BTreeMap::new();
    let mut directories = vec![root.to_owned()];
    let mut spellings = BTreeMap::new();
    while let Some(directory) = directories.pop() {
        for entry in fs::read_dir(directory)? {
            let file = entry?.path();
            let metadata = fs::symlink_metadata(&file)?;
            if is_link(&metadata) || !(metadata.is_dir() || metadata.is_file()) {
                continue;
            }
            let name = file
                .strip_prefix(root)?
                .components()
                .map(|component| match component {
                    Component::Normal(name) => name.to_str().context("path is not UTF-8"),
                    _ => bail!("unexpected path component"),
                })
                .collect::<Result<Vec<_>>>()?
                .join("/");
            if ignored(&name, metadata.is_dir()) {
                continue;
            }
            validate_path(&name)?;
            if let Some(other) = spellings.insert(name.to_lowercase(), name.clone()) {
                bail!("paths differ only by case: {other} and {name}");
            }
            if metadata.is_dir() {
                directories.push(file);
            } else {
                files.insert(name, file);
            }
        }
    }
    Ok(files)
}

pub fn fingerprint(reader: &mut impl io::Read) -> Result<Blob> {
    let mut hasher = blake3::Hasher::new();
    let size = io::copy(reader, &mut hasher)?;
    Ok(Blob {
        hash: hasher.finalize().to_hex().to_string(),
        size,
    })
}

impl Blob {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.hash.len() == 64
                && self
                    .hash
                    .bytes()
                    .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c)),
            "invalid blob hash"
        );
        Ok(())
    }
}

impl Store {
    pub fn open(root: &Path) -> Result<Self> {
        let lock = lock_directory(root)?;
        fs::create_dir_all(root.join("objects"))?;
        let catalog = read_json(&root.join("catalog.json"))?;
        Ok(Self {
            root: root.to_owned(),
            catalog,
            _lock: lock,
        })
    }

    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    pub fn ingest(&self, source: &Path) -> Result<Blob> {
        let mut source = File::open(source)?;
        let mut temporary = NamedTempFile::new_in(self.root.join("objects"))?;
        io::copy(&mut source, &mut temporary)?;
        temporary.seek(SeekFrom::Start(0))?;
        let blob = fingerprint(&mut temporary)?;
        let destination = self.object(&blob)?;
        if !destination.is_file() {
            persist(temporary, &destination)?;
        }
        Ok(blob)
    }

    pub fn index(&mut self, source: &Path) -> Result<usize> {
        let root = self.root.canonicalize()?;
        let source = source.canonicalize()?;
        ensure!(
            !root.starts_with(&source) && !source.starts_with(&root),
            "archive state and source must be separate"
        );
        let files = scan(&source, |_, _| false)?;
        let mut catalog = self.catalog.clone();
        let mut changed = 0;
        for (path, file) in files {
            let blob = self.ingest(&file)?;
            ensure_portable_catalog(&catalog, &path)?;
            let versions = catalog.entry(path).or_default();
            if versions.last().and_then(|version| version.blob.as_ref()) == Some(&blob) {
                continue;
            }
            let parent = versions.last().map(|version| version.id);
            push_version(versions, parent, "local", Some(blob));
            changed += 1;
        }
        if changed > 0 {
            self.save(catalog)?;
        }
        Ok(changed)
    }

    pub fn save(&mut self, catalog: Catalog) -> Result<()> {
        write_json(&self.root.join("catalog.json"), &catalog)?;
        self.catalog = catalog;
        Ok(())
    }

    pub fn version(&self, name: &str, id: Option<u64>) -> Result<&Version> {
        validate_path(name)?;
        let versions = self.catalog.get(name).context("file not found")?;
        match id {
            Some(id) => versions.iter().find(|version| version.id == id),
            None => versions.last(),
        }
        .context("version not found")
    }

    pub fn object(&self, blob: &Blob) -> Result<PathBuf> {
        blob.validate()?;
        Ok(self.root.join("objects").join(&blob.hash))
    }

    pub fn restore(&self, name: &str, id: Option<u64>, destination: &Path) -> Result<()> {
        let blob = self
            .version(name, id)?
            .blob
            .as_ref()
            .context("file was deleted")?;
        copy_verified(&self.object(blob)?, destination, blob)
    }
}

pub fn copy_verified(source: &Path, destination: &Path, blob: &Blob) -> Result<()> {
    let directory = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let mut temporary = NamedTempFile::new_in(directory)?;
    io::copy(&mut File::open(source)?, &mut temporary)?;
    temporary.seek(SeekFrom::Start(0))?;
    ensure!(
        &fingerprint(&mut temporary)? == blob,
        "size or hash mismatch"
    );
    persist(temporary, destination)
}

pub fn write_verified(bytes: &[u8], destination: &Path, blob: &Blob) -> Result<()> {
    ensure!(
        &fingerprint(&mut io::Cursor::new(bytes))? == blob,
        "size or hash mismatch"
    );
    let directory = destination
        .parent()
        .context("file must have a parent directory")?;
    let mut temporary = NamedTempFile::new_in(directory)?;
    std::io::Write::write_all(&mut temporary, bytes)?;
    persist(temporary, destination)
}

pub fn ensure_portable_catalog(catalog: &Catalog, path: &str) -> Result<()> {
    let folded = path.to_lowercase();
    for other in catalog.keys() {
        for (left, right) in other.split('/').zip(path.split('/')) {
            if left == right {
                continue;
            }
            ensure!(
                left.to_lowercase() != right.to_lowercase(),
                "paths differ only by case: {other} and {path}"
            );
            break;
        }
        let live = catalog[other]
            .last()
            .is_some_and(|version| version.blob.is_some());
        if live {
            let other_folded = other.to_lowercase();
            ensure!(
                !folded
                    .strip_prefix(&other_folded)
                    .is_some_and(|rest| rest.starts_with('/'))
                    && !other_folded
                        .strip_prefix(&folded)
                        .is_some_and(|rest| rest.starts_with('/')),
                "file and directory names collide: {other} and {path}"
            );
        }
    }
    Ok(())
}

pub fn push_version(
    versions: &mut Vec<Version>,
    parent: Option<u64>,
    author: &str,
    blob: Option<Blob>,
) -> Version {
    let version = Version {
        id: versions.last().map_or(1, |current| current.id + 1),
        parent,
        base_path: None,
        author: author.to_owned(),
        blob,
    };
    versions.push(version.clone());
    version
}
