use crate::store::{
    Blob, Catalog, Store, Version, copy_verified, ensure_portable_catalog, fingerprint, is_link,
    local_path, push_version, read_json, scan, validate_path, write_json, write_verified,
};
use anyhow::{Context, Result, ensure};
use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io,
    path::{Path, PathBuf},
};

pub type Heads = BTreeMap<String, Version>;

pub struct Publication {
    pub store: Store,
    pub(crate) location: Location,
}

pub(crate) struct Location {
    pub(crate) root: PathBuf,
    pub(crate) directory: PathBuf,
    projection_file: PathBuf,
    projection: BTreeMap<String, Blob>,
}

impl Location {
    pub(crate) fn prepare(state: &Path, name: &str, path: &Path) -> Result<Self> {
        let state = state.canonicalize()?;
        let base = state.join("shares").join(name);
        let path_id = blake3::hash(path.as_os_str().as_encoded_bytes()).to_hex();
        let reset = base.join(format!("recreate-{path_id}.json"));
        if let Err(error) = fs::symlink_metadata(path) {
            if error.kind() != io::ErrorKind::NotFound {
                return Err(error.into());
            }
            fs::create_dir_all(&base)?;
            write_json(&reset, &true)?;
            fs::create_dir_all(path)?;
        }
        let metadata = fs::symlink_metadata(path)?;
        ensure!(
            metadata.is_dir() && !is_link(&metadata),
            "unsafe publication root: {}",
            path.display()
        );
        let root = path.canonicalize()?;
        ensure!(
            !root.starts_with(&state) && !state.starts_with(&root),
            "publication root and Nice Sync state overlap"
        );
        let root_id = blake3::hash(root.as_os_str().as_encoded_bytes()).to_hex();
        let directory = archive_directory(&base, root_id.as_str())?;
        let projection_file = directory.join(format!("projection-{root_id}.json"));
        if reset.try_exists()? {
            fs::create_dir_all(&directory)?;
            write_json(&projection_file, &BTreeMap::<String, Blob>::new())?;
            fs::remove_file(reset)?;
        }
        let projection = read_json(&projection_file)?;
        Ok(Self {
            root,
            directory,
            projection_file,
            projection,
        })
    }
}

fn archive_directory(base: &Path, root_id: &str) -> Result<PathBuf> {
    fs::create_dir_all(base)?;
    let binding = base.join("primary-root.json");
    let mut primary: Option<String> = read_json(&binding)?;
    if primary.is_none() {
        let mut legacy_roots = Vec::new();
        for entry in fs::read_dir(base)? {
            let entry = entry?;
            let file = entry.file_name();
            if let Some(id) = file
                .to_str()
                .and_then(|file| file.strip_prefix("projection-"))
                .and_then(|file| file.strip_suffix(".json"))
                && id.len() == 64
            {
                legacy_roots.push(id.to_owned());
            }
        }
        primary = match legacy_roots.as_slice() {
            [id] => Some(id.clone()),
            [] if !base.join("catalog.json").exists() => Some(root_id.to_owned()),
            _ => None,
        };
        if let Some(id) = &primary {
            write_json(&binding, id)?;
        }
    }
    if primary.as_deref() == Some(root_id) {
        Ok(base.to_owned())
    } else {
        Ok(base.join("roots").join(root_id))
    }
}

impl Publication {
    pub fn list_directory(
        &self,
        directory: &str,
        ignored: impl Fn(&str, bool) -> bool,
    ) -> Result<Vec<String>> {
        let root = &self.location.root;
        let target = if directory.is_empty() {
            root.clone()
        } else {
            ensure!(!ignored(directory, true), "directory not found");
            local_path(root, directory)?
        };
        let metadata = fs::symlink_metadata(&target).context("directory not found")?;
        ensure!(
            metadata.is_dir() && !is_link(&metadata),
            "expected a directory"
        );
        let target = target.canonicalize()?;
        ensure!(
            target.starts_with(root),
            "directory is outside the publication"
        );
        let mut names = Vec::new();
        for entry in fs::read_dir(target)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            let path = if directory.is_empty() {
                name.to_owned()
            } else {
                format!("{directory}/{name}")
            };
            if validate_path(&path).is_err() {
                continue;
            }
            let meta = fs::symlink_metadata(entry.path())?;
            if is_link(&meta) || !(meta.is_dir() || meta.is_file()) || ignored(&path, meta.is_dir())
            {
                continue;
            }
            names.push(if meta.is_dir() {
                format!("{name}/")
            } else {
                name.to_owned()
            });
        }
        names.sort_unstable();
        Ok(names)
    }

    pub(crate) fn open(location: Location) -> Result<Self> {
        let store = Store::open(&location.directory)?;
        Ok(Self { store, location })
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
        self.ensure_root()?;
        let files = scan(&self.location.root, &ignored)?;
        let names = files
            .keys()
            .chain(self.store.catalog().keys())
            .cloned()
            .collect::<BTreeSet<_>>();
        // Process removals before additions, including a file replaced by a directory.
        let paths = names
            .iter()
            .filter(|name| !files.contains_key(*name) && !ignored(name, false))
            .chain(files.keys())
            .map(|name| {
                let actual = files
                    .get(name)
                    .map(|path| fingerprint(&mut File::open(path)?))
                    .transpose()?;
                Ok((name.as_str(), actual))
            });
        self.refresh_paths(host, paths)
    }

    pub fn refresh_one(&mut self, name: &str, host: &str) -> Result<()> {
        self.ensure_root()?;
        let actual = local_blob(&self.location.root, name)?;
        self.refresh_paths(host, [Ok((name, actual))])
    }

    fn refresh_paths<'a>(
        &mut self,
        host: &str,
        paths: impl IntoIterator<Item = Result<(&'a str, Option<Blob>)>>,
    ) -> Result<()> {
        let mut catalog = Cow::Borrowed(self.store.catalog());
        let mut projection = Cow::Borrowed(&self.location.projection);
        for entry in paths {
            let (name, actual) = entry?;
            let current = catalog.get(name).and_then(|versions| versions.last());
            let head = current.and_then(|version| version.blob.as_ref());
            let next = if actual.as_ref() == head {
                actual
            } else if actual.as_ref() == projection.get(name) {
                self.project_file(name, head, actual.as_ref())?;
                head.cloned()
            } else {
                if let Some(actual) = &actual {
                    ensure_portable_catalog(&catalog, name)?;
                    let blob = self.store.ingest(&local_path(&self.location.root, name)?)?;
                    ensure!(&blob == actual, "file changed during scan: {name}");
                }
                let parent = current.map(|version| version.id);
                push_version(
                    catalog.to_mut().entry(name.to_owned()).or_default(),
                    parent,
                    host,
                    actual.clone(),
                );
                actual
            };
            if projection.get(name) != next.as_ref() {
                update_projection(projection.to_mut(), name, next);
            }
        }
        if let Cow::Owned(catalog) = catalog {
            self.store.save(catalog)?;
        }
        if let Cow::Owned(projection) = projection {
            self.save_projection(projection)?;
        }
        Ok(())
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
        Ok(self
            .commit_many(
                &[Change {
                    path: name.to_owned(),
                    base,
                    blob,
                }],
                author,
                host,
                mirror,
            )?
            .remove(0))
    }

    pub fn commit_many(
        &mut self,
        changes: &[Change],
        author: &str,
        host: &str,
        mirror: bool,
    ) -> Result<Vec<String>> {
        self.ensure_root()?;
        let root = self.location.root.clone();
        self.refresh_paths(
            host,
            changes
                .iter()
                .map(|change| Ok((change.path.as_str(), local_blob(&root, &change.path)?))),
        )?;
        let mut catalog = Cow::Borrowed(self.store.catalog());
        let mut results = Vec::with_capacity(changes.len());
        let mut projections = Vec::new();
        for Change { path, base, blob } in changes {
            let current = catalog.get(path).and_then(|versions| versions.last());
            let head = current.and_then(|version| version.blob.as_ref());
            let current_id = current.map(|version| version.id);
            if blob.as_ref() == head {
                results.push(path.clone());
                continue;
            }
            if let Some(base) = base {
                ensure!(
                    catalog
                        .get(path)
                        .is_some_and(|versions| versions.iter().any(|v| v.id == *base)),
                    "unknown base revision"
                );
            }
            let stale = mirror && *base != current_id;
            if stale && blob.is_none() {
                results.push(path.clone());
                continue;
            }
            let target = match (stale, head, blob) {
                (true, Some(_), Some(blob)) => conflict_path(&catalog, path, blob)?,
                _ => path.clone(),
            };
            let previous = if target == *path {
                head.cloned()
            } else if catalog
                .get(&target)
                .and_then(|versions| versions.last())
                .is_some_and(|version| version.blob == *blob)
            {
                results.push(target);
                continue;
            } else {
                ensure!(
                    local_blob(&root, &target)?.is_none(),
                    "conflict file already exists: {target}"
                );
                None
            };
            ensure_portable_catalog(&catalog, &target)?;
            let history = catalog.to_mut().entry(target.clone()).or_default();
            push_version(
                history,
                if mirror { *base } else { current_id },
                author,
                blob.clone(),
            );
            if target != *path {
                history.last_mut().expect("new revision").base_path = Some(path.clone());
            }
            projections.push((target.clone(), blob.clone(), previous));
            results.push(target);
        }
        if let Cow::Owned(catalog) = catalog {
            self.store.save(catalog)?;
            let mut projection = self.location.projection.clone();
            for (target, blob, previous) in projections {
                self.project_file(&target, blob.as_ref(), previous.as_ref())?;
                update_projection(&mut projection, &target, blob);
            }
            self.save_projection(projection)?;
        }
        Ok(results)
    }

    fn project_file(&self, name: &str, blob: Option<&Blob>, previous: Option<&Blob>) -> Result<()> {
        ensure!(
            local_blob(&self.location.root, name)?.as_ref() == previous,
            "host file changed during sync: {name}"
        );
        match blob {
            Some(blob) => install(&self.location.root, name, &self.store.object(blob)?, blob),
            None => remove(&self.location.root, name),
        }
    }

    fn ensure_root(&mut self) -> Result<()> {
        let root = &self.location.root;
        match fs::symlink_metadata(root) {
            Ok(metadata) => ensure!(
                metadata.is_dir() && !is_link(&metadata),
                "unsafe publication root: {}",
                root.display()
            ),
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                self.save_projection(BTreeMap::new())?;
                fs::create_dir_all(&self.location.root)?;
            }
            Err(error) => return Err(error.into()),
        }
        Ok(())
    }

    fn save_projection(&mut self, projection: BTreeMap<String, Blob>) -> Result<()> {
        write_json(&self.location.projection_file, &projection)?;
        self.location.projection = projection;
        Ok(())
    }
}

fn update_projection(projection: &mut BTreeMap<String, Blob>, name: &str, blob: Option<Blob>) {
    if let Some(blob) = blob {
        projection.insert(name.to_owned(), blob);
    } else {
        projection.remove(name);
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

#[derive(serde::Deserialize, serde::Serialize)]
pub struct Change {
    pub path: String,
    pub base: Option<u64>,
    pub blob: Option<Blob>,
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
    copy_verified(source, &install_destination(root, name)?, blob)
}

pub fn install_bytes(root: &Path, name: &str, bytes: &[u8], blob: &Blob) -> Result<()> {
    write_verified(bytes, &install_destination(root, name)?, blob)
}

fn install_destination(root: &Path, name: &str) -> Result<PathBuf> {
    let destination = local_path(root, name)?;
    let parent = destination
        .parent()
        .context("file must have a parent directory")?;
    fs::create_dir_all(parent)?;
    if destination.is_dir() {
        fs::remove_dir(&destination).context("destination directory is not empty")?;
    }
    Ok(destination)
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
