use crate::{
    access::{self, Destination, Device},
    language::{Applied, Mode, Network, Right, Rule, Share},
    published::{Edit, Heads, Location, Publication},
    store::{Blob, Version, lock_directory, validate_path, write_json},
    transfer::{PartialFile, SMALL_BATCH, SMALL_FILE, read_chunk},
    wire,
};
use anyhow::{Context, Result, ensure};
use futures_util::StreamExt;
use iroh::{Endpoint, EndpointAddr, EndpointId, TransportAddr, endpoint::Connection};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Deserialize, Serialize)]
pub enum Request {
    SharedConfig,
    Enroll {
        token: [u8; 32],
        device: String,
    },
    Issue {
        template: String,
        device: String,
    },
    Roots,
    List {
        share: String,
        directory: String,
    },
    History {
        share: String,
        path: String,
    },
    Read {
        share: String,
        path: String,
        version: Option<u64>,
        offset: u64,
        sync: bool,
    },
    Manifest {
        share: String,
    },
    ReadSmall {
        share: String,
        files: Vec<(String, u64)>,
    },
    Prepare {
        share: String,
        path: String,
        blob: Blob,
    },
    Chunk {
        share: String,
        path: String,
        blob: Blob,
        offset: u64,
    },
    Commit {
        share: String,
        path: String,
        blob: Option<Blob>,
        base: Option<u64>,
    },
}

#[derive(Deserialize, Serialize)]
pub enum Answer {
    SharedConfig(String),
    Enrolled(String),
    Code(String),
    Names(Vec<String>),
    History(Vec<Version>),
    File(Version),
    SmallFiles(Vec<usize>),
    Manifest(Heads),
    Offset(u64),
    Committed(String),
}

pub struct Client {
    endpoint: Endpoint,
    connection: Connection,
}

impl Client {
    pub async fn connect(state: &Path, destination: &Destination) -> Result<Self> {
        let direct = if destination.direct {
            let target = destination
                .endpoint
                .ip_addrs()
                .next()
                .context("direct connection has no address")?;
            let ip = if target.is_ipv4() {
                IpAddr::V4(Ipv4Addr::UNSPECIFIED)
            } else {
                IpAddr::V6(Ipv6Addr::UNSPECIFIED)
            };
            Some(SocketAddr::new(ip, 0))
        } else {
            None
        };
        let endpoint = wire::bind(access::key(state)?, direct).await?;
        let connection = wire::connect(&endpoint, destination.endpoint.clone()).await?;
        Ok(Self {
            endpoint,
            connection,
        })
    }

    pub async fn connect_p2p(state: &Path, destination: &Destination) -> Result<Self> {
        if destination.direct {
            return Self::connect(state, destination).await;
        }
        let discovery = Self::connect(state, destination).await?;
        let mut paths = discovery.connection.paths_stream();
        let next_direct = async {
            let mut waiting = false;
            while let Some(snapshot) = paths.next().await {
                for path in snapshot.iter().filter(|path| path.is_selected()) {
                    if let TransportAddr::Ip(address) = path.remote_addr() {
                        return Some(*address);
                    }
                }
                if !waiting {
                    eprintln!("Waiting for a direct P2P path");
                    waiting = true;
                }
            }
            None
        };
        let address = tokio::time::timeout(Duration::from_secs(20), next_direct).await;
        drop(paths);
        let local_sockets = discovery.endpoint.bound_sockets();
        discovery.close().await;
        let address = address
            .context("no direct P2P path after 20 seconds")?
            .context("connection closed before a direct P2P path was established")?;
        let local = local_sockets
            .into_iter()
            .find(|socket| socket.is_ipv4() == address.is_ipv4())
            .context("no local UDP socket for the direct P2P path")?;
        let endpoint = wire::bind(access::key(state)?, Some(local))
            .await
            .context("reopening the local UDP socket for P2P transfer")?;
        let direct = EndpointAddr::new(destination.endpoint.id).with_ip_addr(address);
        let connection = wire::connect(&endpoint, direct)
            .await
            .context("the discovered P2P path did not survive direct reconnect")?;
        eprintln!("P2P-only transfer via {address}");
        Ok(Self {
            endpoint,
            connection,
        })
    }

    pub async fn call(&self, request: Request, data: &[u8]) -> Result<(Answer, Vec<u8>)> {
        wire::call(&self.connection, &request, data).await
    }

    pub fn trace_route(&self) -> tokio::task::JoinHandle<()> {
        let connection = self.connection.clone();
        let mut last = None;
        let mut report = move |paths: iroh::endpoint::PathList<'_>| {
            let selected = paths.iter().find(|path| path.is_selected());
            let address = selected.as_ref().map(|path| path.remote_addr().to_string());
            if address != last {
                if let Some(path) = selected {
                    eprintln!(
                        "Nice Sync route: {} (RTT {} ms)",
                        path.remote_addr(),
                        path.rtt().as_millis()
                    );
                }
                last = address;
            }
        };
        report(connection.paths());
        tokio::spawn(async move {
            let paths = connection.paths_stream();
            tokio::pin!(paths);
            while let Some(snapshot) = paths.next().await {
                report(snapshot);
            }
        })
    }

    pub async fn close(self) {
        self.endpoint.close().await;
    }
}

pub struct Server {
    share_config: bool,
    state: PathBuf,
    applied: Applied,
    config_hash: blake3::Hash,
    network: Network,
    destination: Destination,
    publications: BTreeMap<String, Publication>,
}

impl Server {
    async fn listen(
        state: &Path,
        direct: Option<SocketAddr>,
        share_config: bool,
    ) -> Result<(Self, Endpoint)> {
        let applied = Applied::load(state)?;
        let network = applied.network()?;
        ensure!(
            network.host == applied.device,
            "serve must run on '{}'",
            network.host
        );
        println!("Opening publications for '{}'", network.name);
        let publications = publication_locations(state, &applied, &network)?
            .into_iter()
            .map(|(name, location)| Ok((name, Publication::open(location)?)))
            .collect::<Result<BTreeMap<_, _>>>()?;
        if let Some(address) = direct {
            ensure!(
                !address.ip().is_unspecified(),
                "provide a concrete IP address for --direct"
            );
        }
        let endpoint = wire::bind(access::key(state)?, direct).await?;
        if direct.is_none() {
            // A reachable direct address can be used while the relay is still connecting.
            let _ = tokio::time::timeout(Duration::from_secs(4), endpoint.online()).await;
        }
        let address = if let Some(address) = direct {
            let port = endpoint
                .bound_sockets()
                .first()
                .context("direct socket is unavailable")?
                .port();
            iroh::EndpointAddr::new(endpoint.id()).with_ip_addr(SocketAddr::new(address.ip(), port))
        } else {
            endpoint.addr()
        };
        let destination = Destination {
            endpoint: address,
            direct: direct.is_some(),
            network: network.name.clone(),
            host: network.host.clone(),
        };
        write_json(&state.join("listener.json"), &destination)?;
        Ok((
            Self {
                share_config,
                state: state.to_owned(),
                config_hash: blake3::hash(applied.source.as_bytes()),
                applied,
                network,
                destination,
                publications,
            },
            endpoint,
        ))
    }

    fn reload(&mut self, path: &Path) -> Result<bool> {
        let source = fs::read_to_string(path)?;
        if source == self.applied.source {
            return Ok(false);
        }
        let candidate = Applied {
            source,
            base: self.applied.base.clone(),
            device: self.applied.device.clone(),
            file: Some(path.to_owned()),
        };
        let network = candidate.network()?;
        ensure!(
            network.host == self.network.host && network.name == self.network.name,
            "a live update must use the same network and host"
        );

        let mut added = BTreeMap::new();
        for (name, location) in publication_locations(&self.state, &candidate, &network)? {
            if self
                .publications
                .get(&name)
                .is_none_or(|existing| existing.location.root != location.root)
            {
                added.insert(name, Publication::open(location)?);
            }
        }
        candidate.save(&self.state)?;
        self.publications
            .retain(|name, _| network.shares.contains_key(name));
        self.publications.extend(added);
        self.config_hash = blake3::hash(candidate.source.as_bytes());
        self.applied = candidate;
        self.network = network;
        Ok(true)
    }

    fn handle(
        &mut self,
        peer: EndpointId,
        request: Request,
        data: Vec<u8>,
        sync_config: &mut Option<blake3::Hash>,
    ) -> Result<(Answer, Vec<u8>)> {
        if !matches!(request, Request::Chunk { .. }) {
            ensure!(data.is_empty(), "unexpected request body");
        }
        if let Some(hash) = sync_config
            && matches!(
                request,
                Request::Manifest { .. }
                    | Request::Prepare { .. }
                    | Request::Chunk { .. }
                    | Request::Commit { .. }
                    | Request::Read { sync: true, .. }
                    | Request::ReadSmall { .. }
            )
        {
            ensure!(
                *hash == self.config_hash,
                "configuration changed during synchronization; retry sync"
            );
        }
        if let Request::Enroll { token, device } = request {
            ensure!(
                self.network.devices.contains(&device) && device != self.network.host,
                "unknown device"
            );
            let registered =
                access::enroll(&self.state, peer, &token, &device, access::epoch_seconds()?)?;
            return Ok((Answer::Enrolled(registered.name), Vec::new()));
        }
        let device = access::registered(&self.state, peer)?;
        ensure!(
            self.network.devices.contains(&device.name),
            "device has been removed from the network"
        );
        let empty = Vec::new();
        Ok(match request {
            Request::SharedConfig => {
                ensure!(
                    self.share_config,
                    "host has not enabled shared configuration"
                );
                *sync_config = Some(self.config_hash);
                (Answer::SharedConfig(self.applied.source.clone()), empty)
            }
            Request::Enroll { .. } => unreachable!(),
            Request::Issue { template, device } => (
                Answer::Code(
                    access::issue(
                        &self.state,
                        &self.network,
                        self.destination.clone(),
                        Some(peer),
                        Some(&template),
                        &device,
                    )?
                    .encode()?,
                ),
                empty,
            ),
            Request::Roots => {
                let names = self
                    .publications
                    .keys()
                    .filter(|share| permitted(&device, share, Right::Browse))
                    .cloned()
                    .collect();
                (Answer::Names(names), empty)
            }
            Request::List { share, directory } => {
                require(&device, &share, Right::Browse)?;
                if !directory.is_empty() {
                    validate_path(&directory)?;
                }
                let host = self.applied.device.clone();
                let definition = self
                    .network
                    .shares
                    .get(&share)
                    .context("unknown publication")?;
                let publication = self
                    .publications
                    .get_mut(&share)
                    .context("unknown publication")?;
                publication.refresh(&host, |path, directory| {
                    self.network.ignored(definition, None, path, directory)
                })?;
                let prefix = if directory.is_empty() {
                    String::new()
                } else {
                    format!("{directory}/")
                };
                let mut names = BTreeMap::new();
                for (path, entry) in publication.heads() {
                    if entry.blob.is_some()
                        && let Some(child) = path.strip_prefix(&prefix)
                    {
                        match child.split_once('/') {
                            Some((first, _)) => {
                                names.insert(first.to_owned(), true);
                            }
                            None => {
                                names.insert(child.to_owned(), false);
                            }
                        }
                    }
                }
                ensure!(
                    directory.is_empty() || !names.is_empty(),
                    "directory not found"
                );
                let names = names
                    .into_iter()
                    .map(|(name, dir)| if dir { format!("{name}/") } else { name })
                    .collect();
                (Answer::Names(names), empty)
            }
            Request::History { share, path } => {
                require(&device, &share, Right::Browse)?;
                validate_path(&path)?;
                let host = self.applied.device.clone();
                let publication = self.publication(&share)?;
                publication.refresh_one(&path, &host)?;
                let versions = publication
                    .store
                    .catalog()
                    .get(&path)
                    .context("file not found")?;
                (Answer::History(versions.clone()), empty)
            }
            Request::Read {
                share,
                path,
                version,
                offset,
                sync,
            } => {
                if sync {
                    let (definition, rule) = self.rule(&device, &share)?;
                    ensure!(
                        rule.mode.downloads()
                            && !self.network.ignored(definition, Some(rule), &path, false),
                        "file is excluded from synchronization"
                    );
                } else {
                    require(&device, &share, Right::Download)?;
                }
                let host = self.applied.device.clone();
                let publication = self.publication(&share)?;
                if version.is_none() {
                    publication.refresh_one(&path, &host)?;
                }
                let version = publication.store.version(&path, version)?;
                let blob = version.blob.as_ref().context("revision is a deletion")?;
                (
                    Answer::File(version.clone()),
                    read_chunk(&publication.store.object(blob)?, offset, blob.size)?,
                )
            }
            Request::ReadSmall { share, files } => {
                ensure!(
                    !files.is_empty() && files.len() <= SMALL_BATCH,
                    "invalid small-file batch size"
                );
                let (definition, rule) = self.rule(&device, &share)?;
                ensure!(
                    rule.mode.downloads(),
                    "share is upload-only for this device"
                );
                let publication = self
                    .publications
                    .get(&share)
                    .context("unknown publication")?;
                let mut lengths = Vec::with_capacity(files.len());
                let mut bytes = Vec::new();
                for (path, revision) in files {
                    ensure!(
                        !self.network.ignored(definition, Some(rule), &path, false),
                        "file is excluded from synchronization"
                    );
                    let version = publication.store.version(&path, Some(revision))?;
                    let blob = version.blob.as_ref().context("revision is a deletion")?;
                    ensure!(blob.size <= SMALL_FILE, "file exceeds the small-file limit");
                    lengths.push(blob.size as usize);
                    bytes.extend(read_chunk(&publication.store.object(blob)?, 0, blob.size)?);
                }
                (Answer::SmallFiles(lengths), bytes)
            }
            Request::Manifest { share } => {
                let (_, rule) = self.rule(&device, &share)?;
                ensure!(
                    rule.mode.downloads(),
                    "share is upload-only for this device"
                );
                let host = self.applied.device.clone();
                let publication = self
                    .publications
                    .get_mut(&share)
                    .context("unknown publication")?;
                publication.refresh(&host, |path, directory| {
                    self.network
                        .ignored(&self.network.shares[&share], None, path, directory)
                })?;
                let mut heads = publication.heads();
                let (definition, rule) = self.rule(&device, &share)?;
                heads.retain(|path, _| !self.network.ignored(definition, Some(rule), path, false));
                (Answer::Manifest(heads), empty)
            }
            Request::Prepare { share, path, blob } => {
                self.upload_rule(&device, &share, &path)?;
                let object = self.publication(&share)?.store.object(&blob)?;
                let offset = if object.is_file() {
                    ensure!(
                        object.metadata()?.len() == blob.size,
                        "object size mismatch"
                    );
                    blob.size
                } else {
                    self.partial(peer, &share, &blob)?.offset()?
                };
                (Answer::Offset(offset), empty)
            }
            Request::Chunk {
                share,
                path,
                blob,
                offset,
            } => {
                self.upload_rule(&device, &share, &path)?;
                (
                    Answer::Offset(self.partial(peer, &share, &blob)?.append(offset, &data)?),
                    empty,
                )
            }
            Request::Commit {
                share,
                path,
                blob,
                base,
            } => {
                let rule = self.upload_rule(&device, &share, &path)?;
                ensure!(
                    blob.is_some() || rule.propagate_delete,
                    "delete is disabled for this device"
                );
                let mirror = rule.mode == Mode::Mirror;
                let partial = blob
                    .as_ref()
                    .map(|blob| self.partial(peer, &share, blob))
                    .transpose()?;
                let host = self.applied.device.clone();
                let publication = self.publication(&share)?;
                if let Some((blob, partial)) = blob.as_ref().zip(partial.as_ref()) {
                    let object = publication.store.object(blob)?;
                    if !object.is_file() {
                        let verified = publication.store.ingest(partial.verified()?)?;
                        ensure!(&verified == blob, "uploaded file changed before commit");
                    }
                    ensure!(
                        object.metadata()?.len() == blob.size,
                        "object size mismatch"
                    );
                    partial.remove()?;
                }
                let path = publication.commit(Edit {
                    name: &path,
                    base,
                    author: &device.name,
                    host: &host,
                    blob,
                    mirror,
                })?;
                (Answer::Committed(path), empty)
            }
        })
    }

    fn publication(&mut self, name: &str) -> Result<&mut Publication> {
        self.publications
            .get_mut(name)
            .context("unknown publication")
    }

    fn rule(&self, device: &Device, name: &str) -> Result<(&Share, &Rule)> {
        let share = self
            .network
            .shares
            .get(name)
            .context("unknown publication")?;
        let rule = share
            .rules
            .get(&device.name)
            .context("device has no synchronization rule")?;
        require(device, name, rule.mode.right())?;
        Ok((share, rule))
    }

    fn upload_rule(&self, device: &Device, name: &str, path: &str) -> Result<&Rule> {
        validate_path(path)?;
        let (share, rule) = self.rule(device, name)?;
        ensure!(
            rule.mode.uploads(),
            "share is download-only for this device"
        );
        ensure!(
            !self.network.ignored(share, Some(rule), path, false),
            "file is excluded from synchronization"
        );
        Ok(rule)
    }

    fn partial(&self, peer: EndpointId, share: &str, blob: &Blob) -> Result<PartialFile> {
        PartialFile::new(
            &self
                .publications
                .get(share)
                .context("unknown publication")?
                .location
                .directory
                .join("incoming")
                .join(peer.to_string()),
            blob,
        )
    }
}

fn publication_locations(
    state: &Path,
    applied: &Applied,
    network: &Network,
) -> Result<BTreeMap<String, Location>> {
    let mut locations = BTreeMap::<String, Location>::new();
    for (name, share) in &network.shares {
        let location = Location::prepare(state, name, &applied.base.join(&share.path))?;
        ensure!(
            !locations
                .values()
                .any(|other| location.root.starts_with(&other.root)
                    || other.root.starts_with(&location.root)),
            "published roots overlap"
        );
        locations.insert(name.clone(), location);
    }
    Ok(locations)
}

fn permitted(device: &Device, share: &str, right: Right) -> bool {
    device
        .grants
        .get(share)
        .is_some_and(|rights| rights.contains(&right))
}

fn require(device: &Device, share: &str, right: Right) -> Result<()> {
    ensure!(
        permitted(device, share, right),
        "permission denied for '{share}': {right:?}"
    );
    Ok(())
}

pub async fn serve(state: &Path, direct: Option<SocketAddr>, share_config: bool) -> Result<()> {
    let _lock = lock_directory(&state.join("runtime"))?;
    let (server, endpoint) = Server::listen(state, direct, share_config).await?;
    println!("Serving '{}' as {}", server.network.name, endpoint.id());
    let source = server.applied.file.clone().filter(|_| share_config);
    if share_config && source.is_none() {
        eprintln!("Live configuration requires applying the source file once with this version.");
    }
    let server = Arc::new(Mutex::new(server));
    let watcher = source.map(|source| {
        println!("Watching {}", source.display());
        tokio::spawn(watch_config(Arc::clone(&server), source))
    });
    let connections = Arc::new(tokio::sync::Semaphore::new(64));
    while let Some(incoming) = endpoint.accept().await {
        let Ok(slot) = connections.clone().try_acquire_owned() else {
            incoming.refuse();
            continue;
        };
        let server = Arc::clone(&server);
        tokio::spawn(async move {
            let _slot = slot;
            if let Err(error) = handle_connection(incoming.await?, server).await {
                eprintln!("Connection: {error:#}");
            }
            Ok::<_, anyhow::Error>(())
        });
    }
    if let Some(watcher) = watcher {
        watcher.abort();
    }
    fs::remove_file(state.join("listener.json"))?;
    Ok(())
}

async fn watch_config(server: Arc<Mutex<Server>>, source: PathBuf) {
    let mut last_error = None;
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let server = Arc::clone(&server);
        let path = source.clone();
        let result = tokio::task::spawn_blocking(move || {
            server
                .lock()
                .map_err(|_| anyhow::anyhow!("server state unavailable"))?
                .reload(&path)
        })
        .await;
        match result {
            Ok(Ok(changed)) => {
                last_error = None;
                if changed {
                    println!("Reloaded {}", source.display());
                }
            }
            Ok(Err(error)) => {
                let message = format!("{error:#}");
                if last_error.as_ref() != Some(&message) {
                    eprintln!(
                        "Configuration reload failed for {} ({message})",
                        source.display()
                    );
                    last_error = Some(message);
                }
            }
            Err(_) => return,
        }
    }
}

async fn handle_connection(connection: Connection, server: Arc<Mutex<Server>>) -> Result<()> {
    let peer = connection.remote_id();
    let mut sync_config = None;
    loop {
        let Ok(Ok((mut send, mut receive))) =
            tokio::time::timeout(wire::TIMEOUT, connection.accept_bi()).await
        else {
            return Ok(());
        };
        let invocation =
            tokio::time::timeout(wire::TIMEOUT, wire::read_request(&mut receive)).await;
        let result = match invocation {
            Ok(Ok((request, data))) => {
                let server = Arc::clone(&server);
                let (result, current_config) = tokio::task::spawn_blocking(move || {
                    let result = server
                        .lock()
                        .map_err(|_| anyhow::anyhow!("server state unavailable"))
                        .and_then(|mut server| {
                            server.handle(peer, request, data, &mut sync_config)
                        });
                    (result, sync_config)
                })
                .await?;
                sync_config = current_config;
                result
            }
            Ok(Err(error)) => Err(error),
            Err(error) => Err(error.into()),
        };
        tokio::time::timeout(wire::TIMEOUT, wire::reply(&mut send, result)).await??;
    }
}
