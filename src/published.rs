use crate::store::{
    Blob, Catalog, Store, Version, copy_verified, ensure_portable_catalog, fingerprint, is_link,
    local_path, push_version, read_json, scan, validate_path, write_json,
};
use anyhow::{Context, Result, ensure};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io,
    path::{Path, PathBuf},
};

pub type Heads = BTreeMap<String, Version>;

pub struct Publication {
    pub store: Store,
    root: PathBuf,
    projection_file: PathBuf,
    projection: BTreeMap<String, Blob>,
}

impl Publication {
    pub fn open(state: &Path, name: &str, path: &Path) -> Result<Self> {
        fs::create_dir_all(path)?;
        let metadata = fs::symlink_metadata(path)?;
        ensure!(
            metadata.is_dir() && !is_link(&metadata),
            "unsafe publication root: {}",
            path.display()
        );
        let root = path.canonicalize()?;
        let state = state.canonicalize()?;
        ensure!(
            !root.starts_with(&state) && !state.starts_with(&root),
            "publication root and Nice Sync state overlap"
        );
        let directory = state.join("shares").join(name);
        let store = Store::open(&directory)?;
        let root_id = blake3::hash(root.as_os_str().as_encoded_bytes());
        let projection_file = directory.join(format!("projection-{root_id}.json"));
        let projection = read_json(&projection_file)?;
        Ok(Self {
            store,
            root,
            projection_file,
            projection,
        })
    }

    pub fn heads(&self) -> Heads {
        self.store
            .catalog()
            .iter()
            .filter_map(|(path, history)| {
                history
                    .last()
                    .map(|version| (path.clone(), version.clone()))
            })
            .collect()
    }

    pub fn refresh(&mut self, host: &str, ignored: impl Fn(&str, bool) -> bool) -> Result<()> {
        let files = scan(&self.root, &ignored)?;
        let names = files
            .keys()
            .chain(self.store.catalog().keys())
            .cloned()
            .collect::<BTreeSet<_>>();
        // Process removals before additions, including a file replaced by a directory.
        for name in names
            .iter()
            .filter(|name| !files.contains_key(*name) && !ignored(name, false))
            .chain(files.keys())
        {
            let file = files.get(name);
            let actual = file
                .map(|path| fingerprint(&mut File::open(path)?))
                .transpose()?;
            self.refresh_path(name, actual, host)?;
        }
        Ok(())
    }

    pub fn refresh_one(&mut self, name: &str, host: &str) -> Result<()> {
        let actual = local_blob(&self.root, name)?;
        self.refresh_path(name, actual, host)
    }

    fn refresh_path(&mut self, name: &str, actual: Option<Blob>, host: &str) -> Result<()> {
        let current = self
            .store
            .catalog()
            .get(name)
            .and_then(|versions| versions.last())
            .cloned();
        let head = current.as_ref().and_then(|version| version.blob.as_ref());
        if actual.as_ref() == head {
            return self.remember(name, actual);
        }
        if actual.as_ref() == self.projection.get(name) {
            return self.project(name, head, actual.as_ref());
        }
        let blob = actual
            .as_ref()
            .map(|actual| {
                let blob = self.store.ingest(&local_path(&self.root, name)?)?;
                ensure!(&blob == actual, "file changed during scan: {name}");
                Ok::<_, anyhow::Error>(blob)
            })
            .transpose()?;
        let parent = current.map(|version| version.id);
        let mut catalog = self.store.catalog().clone();
        if blob.is_some() {
            ensure_portable_catalog(&catalog, name)?;
        }
        push_version(
            catalog.entry(name.to_owned()).or_default(),
            parent,
            host,
            blob,
        );
        self.store.save(catalog)?;
        self.remember(name, actual)
    }

    pub fn commit(&mut self, edit: Edit<'_>) -> Result<String> {
        let Edit {
            name,
            base,
            author,
            host,
            blob,
            mirror,
        } = edit;
        validate_path(name)?;
        self.refresh_one(name, host)?;
        let current = self
            .store
            .catalog()
            .get(name)
            .and_then(|versions| versions.last());
        let head = current.and_then(|version| version.blob.as_ref());
        if blob.as_ref() == head {
            return Ok(name.to_owned());
        }
        let current_id = current.map(|version| version.id);
        let stale = mirror && base != current_id;
        if let Some(base) = base {
            self.store
                .version(name, Some(base))
                .context("unknown base revision")?;
        }
        if stale && blob.is_none() {
            return Ok(name.to_owned());
        }
        let mut catalog = self.store.catalog().clone();
        let target = match (stale, head, &blob) {
            (true, Some(_), Some(blob)) => conflict_path(&catalog, name, blob)?,
            _ => name.to_owned(),
        };
        let previous = if target == name {
            head.cloned()
        } else {
            self.refresh_one(&target, host)?;
            catalog = self.store.catalog().clone();
            if let Ok(current) = self.store.version(&target, None) {
                if current.blob == blob {
                    return Ok(target);
                }
                anyhow::bail!("Conflict file {target} changed. Retry synchronization.");
            }
            None
        };
        ensure_portable_catalog(&catalog, &target)?;
        let history = catalog.entry(target.clone()).or_default();
        push_version(
            history,
            if mirror { base } else { current_id },
            author,
            blob.clone(),
        );
        if target != name {
            history.last_mut().expect("new revision").base_path = Some(name.to_owned());
        }
        self.store.save(catalog)?;
        self.project(&target, blob.as_ref(), previous.as_ref())?;
        Ok(target)
    }

    fn project(&mut self, name: &str, blob: Option<&Blob>, previous: Option<&Blob>) -> Result<()> {
        ensure!(
            local_blob(&self.root, name)?.as_ref() == previous,
            "host file changed during sync: {name}"
        );
        match blob {
            Some(blob) => install(&self.root, name, &self.store.object(blob)?, blob)?,
            None => remove(&self.root, name)?,
        }
        self.remember(name, blob.cloned())
    }

    fn remember(&mut self, name: &str, blob: Option<Blob>) -> Result<()> {
        if self.projection.get(name) == blob.as_ref() {
            return Ok(());
        }
        match blob {
            Some(blob) => {
                self.projection.insert(name.to_owned(), blob);
            }
            None => {
                self.projection.remove(name);
            }
        }
        write_json(&self.projection_file, &self.projection)
    }
}

pub struct Edit<'a> {
    pub name: &'a str,
    pub base: Option<u64>,
    pub author: &'a str,
    pub host: &'a str,
    pub blob: Option<Blob>,
    pub mirror: bool,
}

pub fn local_blob(root: &Path, name: &str) -> Result<Option<Blob>> {
    let path = match local_path(root, name) {
        Ok(path) => path,
        Err(error)
            if error
                .downcast_ref::<io::Error>()
                .is_some_and(|error| error.kind() == io::ErrorKind::NotADirectory) =>
        {
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    match File::open(path) {
        Ok(mut file) if file.metadata()?.is_file() => Ok(Some(fingerprint(&mut file)?)),
        Ok(_) => Ok(None),
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::NotADirectory
            ) =>
        {
            Ok(None)
        }
        Err(error)
            if error.kind() == io::ErrorKind::PermissionDenied && root.join(name).is_dir() =>
        {
            Ok(None)
        }
        Err(error) => Err(error.into()),
    }
}

pub fn install(root: &Path, name: &str, source: &Path, blob: &Blob) -> Result<()> {
    let destination = local_path(root, name)?;
    let parent = destination
        .parent()
        .context("file must have a parent directory")?;
    fs::create_dir_all(parent)?;
    if destination.is_dir() {
        fs::remove_dir(&destination).context("destination directory is not empty")?;
    }
    copy_verified(source, &destination, blob)
}

pub fn remove(root: &Path, name: &str) -> Result<()> {
    let path = local_path(root, name)?;
    if !path.is_file() {
        return Ok(());
    }
    fs::remove_file(&path)?;
    let mut directory = path.parent();
    while let Some(parent) = directory.filter(|parent| *parent != root) {
        if fs::remove_dir(parent).is_err() {
            break;
        }
        directory = parent.parent();
    }
    Ok(())
}

fn conflict_path(catalog: &Catalog, name: &str, blob: &Blob) -> Result<String> {
    let (directory, file) = name
        .rsplit_once('/')
        .map_or(("", name), |(directory, file)| (directory, file));
    let (stem, extension) = file
        .rsplit_once('.')
        .filter(|(stem, extension)| !stem.is_empty() && extension.len() <= 50)
        .map_or((file, ""), |(stem, _)| (stem, &file[stem.len()..]));
    let base = stem.floor_char_boundary(stem.len().min(160));
    for number in 1..=1000 {
        let sibling = format!(
            "{}.nice-conflict-{}-{number}{extension}",
            &stem[..base],
            &blob.hash[..12]
        );
        let target = if directory.is_empty() {
            sibling
        } else {
            format!("{directory}/{sibling}")
        };
        validate_path(&target)?;
        if let Some(versions) = catalog.get(&target) {
            if versions.last().is_some_and(|version| {
                version.blob.as_ref() == Some(blob) && version.base_path.as_deref() == Some(name)
            }) {
                return Ok(target);
            }
            continue;
        }
        if ensure_portable_catalog(catalog, &target).is_ok() {
            return Ok(target);
        }
    }
    anyhow::bail!("cannot allocate a conflict filename for {name}")
}
