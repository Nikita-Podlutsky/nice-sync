use anyhow::{Context, Result, bail, ensure};
use clap::{Parser, Subcommand};
use nice_sync::{
    access::{self, Code, Destination, Membership},
    language::{self, Applied},
    peer::{self, Answer, Client, Request},
    store::{Store, lock_directory, read_json, validate_path, write_json, write_text},
    sync,
};
use std::{
    fs,
    io::{self, IsTerminal, Write},
    net::SocketAddr,
    path::{Path, PathBuf},
    time::Duration,
};

mod background;
mod terminal;
mod update;

#[derive(Parser)]
#[command(
    version,
    about = "Local archives and file synchronization between your devices"
)]
struct Cli {
    /// State directory for this device.
    #[arg(long, global = true)]
    state: Option<PathBuf>,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    /// Open the terminal menu (also the default with no subcommand).
    Menu,
    /// Build and install the latest GitHub version on Linux.
    Update,
    /// Keep and inspect local file versions.
    Archive {
        #[command(subcommand)]
        operation: Archive,
    },
    /// Validate a network.sync file.
    Check {
        #[arg(default_value = "network.sync")]
        file: PathBuf,
    },
    /// Preview policy and folders for a device.
    Plan {
        #[arg(default_value = "network.sync")]
        file: PathBuf,
        #[arg(long)]
        device: String,
    },
    /// Save the checked policy for a device.
    Apply {
        #[arg(default_value = "network.sync")]
        file: PathBuf,
        #[arg(long)]
        device: String,
    },
    /// Run the host in the foreground.
    Serve {
        /// Publish the applied network.sync to registered devices.
        #[arg(long)]
        share_config: bool,
        /// Allow this already registered device to publish shared configuration.
        #[arg(long, requires = "share_config")]
        config_editor: Option<String>,
        /// Listen on this existing LAN, VPN or loopback address without Iroh discovery.
        #[arg(long)]
        direct: Option<SocketAddr>,
    },
    /// Generate a single-use invitation for a device declared in network.sync.
    Invite {
        #[arg(long)]
        device: String,
        #[arg(
            long,
            conflicts_with_all = ["configured", "device_only"],
            required_unless_present_any = ["configured", "device_only"]
        )]
        template: Option<String>,
        /// Use this device's permit declarations. Available on the host.
        #[arg(long)]
        configured: bool,
        /// Register an identity first and manage folder permissions later with permit.
        #[arg(long, conflicts_with = "configured")]
        device_only: bool,
        /// Also display the invitation as a QR code in the terminal.
        #[arg(long)]
        qr: bool,
    },
    /// Join automatically with an invitation. Reads one line from stdin when omitted.
    Join {
        code: Option<String>,
        /// Trust the host's shared configuration and run the first sync.
        #[arg(long)]
        sync: bool,
    },
    /// Revoke access and outstanding invitations for a device. Run on the host.
    Revoke { device: String },
    /// List available publications or files in a published directory.
    Ls { path: Option<String> },
    /// Browse published folders interactively with cd, ls and pwd.
    Browse { path: Option<String> },
    /// Read the revision history for a published file.
    History { path: String },
    /// Download the current or a selected revision.
    Get {
        path: String,
        destination: PathBuf,
        #[arg(long)]
        version: Option<u64>,
    },
    /// Exchange files for the local device according to the applied policy.
    Sync(SyncOptions),
    /// Start periodic synchronization independently of this terminal.
    Start {
        #[command(flatten)]
        options: SyncOptions,
        /// Read the configuration snapshot saved by apply.
        #[arg(long, conflicts_with_all = ["config", "publish_config", "follow_config"])]
        applied: bool,
    },
    /// Show the current background synchronization status.
    Status,
    /// Request a stop after the current synchronization pass.
    Stop,
    #[command(hide = true)]
    BackgroundWorker,
}

#[derive(clap::Args, serde::Serialize, serde::Deserialize)]
struct SyncOptions {
    /// Read this local configuration before each synchronization pass.
    #[arg(long, conflicts_with = "follow_config")]
    config: Option<PathBuf>,
    /// Edit this shared configuration locally and publish changes before each pass.
    #[arg(long, conflicts_with_all = ["config", "follow_config"])]
    publish_config: Option<PathBuf>,
    /// Use relay for discovery only and transfer files over a direct connection.
    #[arg(long)]
    p2p_only: bool,
    /// Connect directly to this IP and UDP port using the registered host key.
    #[arg(long)]
    direct: Option<SocketAddr>,
    /// Print the selected Iroh path and changes to it.
    #[arg(long)]
    show_route: bool,
    /// Trust and refresh the host's shared configuration before each sync.
    #[arg(long)]
    follow_config: bool,
    #[arg(long)]
    share: Option<String>,
    /// Repeat synchronization at this interval in seconds.
    #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
    every: Option<u64>,
}

#[derive(Subcommand)]
enum Archive {
    Index {
        source: PathBuf,
    },
    List,
    History {
        path: String,
    },
    Get {
        path: String,
        destination: PathBuf,
        #[arg(long)]
        version: Option<u64>,
    },
}

async fn run() -> Result<()> {
    let Cli { state, command } = Cli::parse();
    let command = command.unwrap_or(Command::Menu);
    let home = std::env::var_os("NICE_SYNC_HOME")
        .map(PathBuf::from)
        .or_else(std::env::home_dir);
    if matches!(command, Command::Menu) {
        return terminal::run(state, home);
    }
    let state = state.unwrap_or_else(|| {
        if matches!(
            command,
            Command::Start { .. } | Command::Status | Command::Stop
        ) && let Some(home) = home
        {
            return home.join(".nice-sync");
        }
        PathBuf::from(".nice-sync")
    });
    match command {
        Command::Menu => unreachable!(),
        Command::Update => update::run()?,
        Command::Archive { operation } => {
            let mut store = Store::open(&state)?;
            match operation {
                Archive::Index { source } => println!("Saved versions: {}", store.index(&source)?),
                Archive::List => {
                    for path in store.catalog().keys() {
                        println!("{path}");
                    }
                }
                Archive::History { path } => {
                    println!(
                        "{}",
                        serde_json::to_string_pretty(
                            store.catalog().get(&path).context("file not found")?
                        )?
                    );
                }
                Archive::Get {
                    path,
                    destination,
                    version,
                } => store.restore(&path, version, &destination)?,
            }
        }
        Command::Check { file } => {
            let model = language::parse(&fs::read_to_string(&file)?)
                .with_context(|| file.display().to_string())?;
            println!(
                "Valid network '{}': {} shares, {} devices",
                model.name,
                model.shares.len(),
                model.devices.len()
            );
        }
        Command::Plan { file, device } => {
            let candidate =
                Applied::read(&file, device).with_context(|| file.display().to_string())?;
            println!("{}", candidate.plan()?);
        }
        Command::Apply { file, device } => {
            let candidate =
                Applied::read(&file, device).with_context(|| file.display().to_string())?;
            let _lock = lock_directory(&state.join("runtime"))?;
            if state.join("joined.json").exists() {
                Membership::load(&state)?.check(&candidate.network()?, &candidate.device)?;
            }
            candidate.save(&state)?;
            println!("{}", candidate.plan()?);
        }
        Command::Serve {
            direct,
            share_config,
            config_editor,
        } => peer::serve(&state, direct, share_config, config_editor.as_deref()).await?,
        Command::Invite {
            device,
            template,
            device_only,
            qr,
            ..
        } => {
            let code = if state.join("joined.json").exists() {
                let template =
                    template.context("only the host can issue invitations without a template")?;
                let member = Membership::load(&state)?;
                let client = Client::connect(&state, &member.destination).await?;
                let (answer, _) = client
                    .call(Request::Issue { template, device }, &[])
                    .await?;
                let Answer::Code(code) = answer else {
                    bail!("expected invitation code")
                };
                client.close().await;
                code
            } else {
                let local = Applied::load(&state)?;
                let network = local.network()?;
                ensure!(local.device == network.host, "join the network first");
                let destination = read_json::<Option<Destination>>(&state.join("listener.json"))?
                    .context("start serve before issuing invitations")?;
                access::issue(
                    &state,
                    &network,
                    destination,
                    None,
                    if device_only {
                        access::InvitationPolicy::DeviceOnly
                    } else if let Some(template) = template.as_deref() {
                        access::InvitationPolicy::Template(template)
                    } else {
                        access::InvitationPolicy::Configured
                    },
                    &device,
                )?
                .encode()?
            };
            println!("{code}");
            if qr {
                let qr = qrcode::QrCode::new(code.as_bytes())?;
                print!(
                    "{}",
                    qr.render::<qrcode::render::unicode::Dense1x2>().build()
                );
                println!();
            }
        }
        Command::Join { code, sync } => {
            let code = match code {
                Some(code) => code,
                None => {
                    let mut input = String::new();
                    if io::stdin().is_terminal() {
                        eprint!("Paste the invitation code and press Enter: ");
                    }
                    io::stdin().read_line(&mut input)?;
                    input
                }
            };
            let code = Code::read(&code)?;
            let _lock = lock_directory(&state.join("runtime"))?;
            let member = Membership {
                destination: code.destination,
                device: code.device,
            };
            if state.join("applied.json").exists() {
                let local = Applied::load(&state)?;
                member.check(&local.network()?, &local.device)?;
            }
            let file = state.join("joined.json");
            if let Some(existing) = read_json::<Option<Membership>>(&file)? {
                ensure!(
                    existing.destination.endpoint.id == member.destination.endpoint.id
                        && existing.device == member.device,
                    "state is already joined as another device or to another host"
                );
            }
            let client = Client::connect(&state, &member.destination).await?;
            let (answer, _) = client
                .call(
                    Request::Enroll {
                        token: code.token,
                        device: member.device.clone(),
                    },
                    &[],
                )
                .await?;
            ensure!(
                matches!(answer, Answer::Enrolled(ref name) if *name == member.device),
                "unexpected enrollment response"
            );
            write_json(&file, &member)?;
            println!(
                "Joined '{}' as {} (host {})",
                member.destination.network, member.device, member.destination.host
            );
            if sync {
                sync_once(&client, &state, &member, SyncConfig::Follow, None, None).await?;
            }
            client.close().await;
        }
        Command::Revoke { device } => {
            let local = Applied::load(&state)?;
            ensure!(
                local.device == local.network()?.host,
                "run revoke on the host"
            );
            access::revoke(&state, &device)?;
            println!("Revoked {device}");
        }
        Command::Ls { path } => {
            let member = Membership::load(&state)?;
            let client = Client::connect(&state, &member.destination).await?;
            list_remote(&client, &member.destination.host, path.as_deref()).await?;
            client.close().await;
        }
        Command::Browse { path } => browse(&state, path.as_deref()).await?,
        Command::History { path } => {
            let member = Membership::load(&state)?;
            let (share, path) = remote_file(&member.destination.host, &path)?;
            let client = Client::connect(&state, &member.destination).await?;
            let (answer, _) = client
                .call(
                    Request::History {
                        share: share.into(),
                        path: path.into(),
                    },
                    &[],
                )
                .await?;
            let Answer::History(versions) = answer else {
                bail!("expected version history")
            };
            println!("{}", serde_json::to_string_pretty(&versions)?);
            client.close().await;
        }
        Command::Get {
            path,
            destination,
            version,
        } => {
            let _lock = lock_directory(&state.join("downloads"))?;
            let member = Membership::load(&state)?;
            let (share, path) = remote_file(&member.destination.host, &path)?;
            let client = Client::connect(&state, &member.destination).await?;
            sync::get(&client, &state, share, path, version, &destination).await?;
            client.close().await;
            println!("Saved {}", destination.display());
        }
        Command::Sync(options) => run_sync(&state, options, None).await?,
        Command::Start {
            mut options,
            applied,
        } => {
            options.every.get_or_insert(10);
            if !applied && options.config.is_none() && options.publish_config.is_none() {
                options.follow_config = true;
            }
            background::start(&state, &options)?;
        }
        Command::Status => background::status(&state)?,
        Command::Stop => background::stop(&state)?,
        Command::BackgroundWorker => background::run(&state).await?,
    }
    Ok(())
}

async fn run_sync(
    state: &Path,
    options: SyncOptions,
    worker: Option<&background::Worker>,
) -> Result<()> {
    let SyncOptions {
        config,
        publish_config,
        share,
        every,
        follow_config,
        direct,
        show_route,
        p2p_only,
    } = options;
    let _lock = lock_directory(&state.join("runtime"))?;
    let member = Membership::load(state)?;
    let mut destination = member.destination.clone();
    if let Some(address) = direct {
        ensure!(
            !address.ip().is_unspecified() && address.port() != 0,
            "provide a concrete host IP and nonzero UDP port"
        );
        destination.endpoint =
            iroh::EndpointAddr::new(destination.endpoint.id).with_ip_addr(address);
        destination.direct = true;
    }
    let config = if let Some(path) = publish_config.as_deref() {
        SyncConfig::Editor(path)
    } else if let Some(path) = config.as_deref() {
        SyncConfig::Local(path)
    } else if follow_config {
        SyncConfig::Follow
    } else {
        SyncConfig::Applied
    };
    if let Some(worker) = worker {
        worker.ready(&member.device, &member.destination.host, share.clone())?;
    }
    loop {
        if let Some(worker) = worker {
            if worker.stopping() {
                return Ok(());
            }
            worker.pass_started()?;
        }
        let result = async {
            let client = if p2p_only {
                Client::connect_p2p(state, &destination).await?
            } else {
                Client::connect(state, &destination).await?
            };
            let route = show_route.then(|| client.trace_route());
            let result = sync_once(&client, state, &member, config, share.as_deref(), worker).await;
            if let Some(route) = route {
                route.abort();
            }
            client.close().await;
            result
        }
        .await;
        let Some(seconds) = every else { return result };
        if let Err(error) = &result {
            eprintln!("{error:#}");
        }
        if let Some(worker) = worker {
            worker.pass_finished(&result, seconds)?;
            if worker.wait(Duration::from_secs(seconds)).await {
                return Ok(());
            }
        } else {
            tokio::time::sleep(Duration::from_secs(seconds)).await;
        }
    }
}

async fn list_remote(client: &Client, host: &str, path: Option<&str>) -> Result<()> {
    let request = match path.map(|path| remote_path(host, path)).transpose()? {
        Some((Some(share), directory)) => Request::List {
            share: share.to_owned(),
            directory: directory.to_owned(),
        },
        _ => Request::Roots,
    };
    let (answer, _) = client.call(request, &[]).await?;
    let Answer::Names(names) = answer else {
        bail!("expected directory listing")
    };
    for name in names {
        println!("{name}");
    }
    Ok(())
}

async fn browse(state: &Path, start: Option<&str>) -> Result<()> {
    let member = Membership::load(state)?;
    let host = &member.destination.host;
    let mut current = browse_path(host, "", start.unwrap_or("/"))?;
    let mut client = Client::connect(state, &member.destination).await?;
    list_remote(&client, host, Some(&format!("{host}:/{current}"))).await?;
    let stdin = io::stdin();
    let terminal = stdin.is_terminal();
    loop {
        if terminal {
            print!("{host}:/{current}> ");
            io::stdout().flush()?;
        }
        let mut input = String::new();
        if stdin.read_line(&mut input)? == 0 {
            break;
        }
        let input = input.trim();
        let (command, argument) = input.split_once(char::is_whitespace).unwrap_or((input, ""));
        let argument = argument.trim();
        match command {
            "exit" | "quit" => break,
            "" => continue,
            "pwd" => println!("{host}:/{current}"),
            "help" => println!("ls [directory]   cd [directory]   pwd   exit"),
            "ls" | "cd" => {
                let result = async {
                    let next = if command == "ls" && argument.is_empty() {
                        current.clone()
                    } else {
                        browse_path(host, &current, argument)?
                    };
                    if client.is_closed() {
                        client = Client::connect(state, &member.destination).await?;
                    }
                    list_remote(&client, host, Some(&format!("{host}:/{next}"))).await?;
                    if command == "cd" {
                        current = next;
                    }
                    Ok::<_, anyhow::Error>(())
                }
                .await;
                if let Err(error) = result {
                    eprintln!("{error:#}");
                }
            }
            _ => eprintln!("Unknown browse command. Type help."),
        }
    }
    client.close().await;
    Ok(())
}

fn browse_path(host: &str, current: &str, input: &str) -> Result<String> {
    let input = input
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .or_else(|| {
            input
                .strip_prefix('\'')
                .and_then(|value| value.strip_suffix('\''))
        })
        .unwrap_or(input);
    let (base, path) = if let Some((device, path)) = input.split_once(":/") {
        ensure!(device == host, "expected the host name '{host}'");
        ("", path)
    } else if input.is_empty() || input.starts_with('/') {
        ("", input)
    } else {
        (current, input)
    };
    let mut parts = base
        .split('/')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    for part in path.split('/') {
        match part {
            "" | "." => {}
            ".." => {
                parts.pop();
            }
            part => {
                validate_path(part)?;
                parts.push(part);
            }
        }
    }
    Ok(parts.join("/"))
}

#[derive(Clone, Copy)]
enum SyncConfig<'a> {
    Applied,
    Follow,
    Local(&'a Path),
    Editor(&'a Path),
}

async fn sync_once(
    client: &Client,
    state: &std::path::Path,
    member: &Membership,
    config: SyncConfig<'_>,
    share: Option<&str>,
    worker: Option<&background::Worker>,
) -> Result<()> {
    let local = match config {
        SyncConfig::Follow => follow_config(client, state, member).await?,
        SyncConfig::Local(path) => Applied::read(path, member.device.clone())?,
        SyncConfig::Editor(path) => edit_config(client, state, member, path).await?,
        SyncConfig::Applied => Applied::load(state)?,
    };
    let network = local.network()?;
    member.check(&network, &local.device)?;
    let report = |folder: &str, progress: &sync::FolderProgress| {
        if let Some(worker) = worker {
            worker.folder(folder, progress);
        }
    };
    if let Some(worker) = worker {
        worker.config_applied(&local.source)?;
    }
    sync::sync(
        client,
        state,
        &local,
        &network,
        share,
        worker.map(|_| &report as sync::ProgressSink<'_>),
    )
    .await
}

async fn shared_config(client: &Client, request: Request) -> Result<String> {
    let (answer, _) = client.call(request, &[]).await?;
    match answer {
        Answer::SharedConfig(source) => Ok(source),
        _ => bail!("expected shared configuration"),
    }
}

async fn follow_config(client: &Client, state: &Path, member: &Membership) -> Result<Applied> {
    let mut applied = Applied {
        source: shared_config(client, Request::SharedConfig).await?,
        base: std::env::current_dir()?,
        device: member.device.clone(),
        file: None,
    };
    member.check(&applied.network()?, &applied.device)?;
    let path = state.join("shared.sync");
    let baseline = state.join("shared-baseline.json");
    let previous: Option<String> = read_json(&baseline)?;
    let current = read_optional_text(&path)?;
    if let Some(local) = &current
        && local != &applied.source
        && previous.as_deref() != Some(config_digest(local).as_str())
    {
        if previous.is_some() {
            let (answer, _) = client.call(Request::ConfigWritable, &[]).await?;
            if matches!(answer, Answer::ConfigWritable(true)) {
                applied.source = publish_config(
                    client,
                    state,
                    &path,
                    previous.as_deref(),
                    &applied.source,
                    local,
                )
                .await?;
                save_config_baseline(&baseline, &applied.source)?;
                return Ok(applied);
            }
        }
        let snapshot = state.join("host.sync");
        if read_optional_text(&snapshot)?.as_deref() != Some(&applied.source) {
            write_text(&snapshot, &applied.source)?;
            eprintln!(
                "Local edits kept in {}. Host configuration saved in {}.",
                path.display(),
                snapshot.display()
            );
        }
        return Ok(applied);
    } else if current.as_deref() != Some(&applied.source) {
        replace_config(&path, &current, &applied.source)?;
    }
    save_config_baseline(&baseline, &applied.source)?;
    Ok(applied)
}

async fn edit_config(
    client: &Client,
    state: &Path,
    member: &Membership,
    path: &Path,
) -> Result<Applied> {
    let path = std::path::absolute(path)?;
    let parent = path
        .parent()
        .context("configuration file needs a parent directory")?;
    fs::create_dir_all(parent)?;
    let base = parent.canonicalize()?;
    let path = base.join(path.file_name().context("expected a configuration file")?);
    let id = blake3::hash(path.as_os_str().as_encoded_bytes()).to_hex();
    let editor_baseline = state.join(format!("editor-{id}.json"));
    let baseline = if path == state.canonicalize()?.join("shared.sync") {
        state.join("shared-baseline.json")
    } else {
        editor_baseline.clone()
    };
    let mut previous: Option<String> = read_json(&baseline)?;
    if previous.is_none() && baseline != editor_baseline {
        previous = read_json(&editor_baseline)?;
    }
    let remote = shared_config(client, Request::SharedConfig).await?;
    let current = read_optional_text(&path)?;
    let mut applied = Applied {
        source: remote,
        base,
        device: member.device.clone(),
        file: None,
    };
    member.check(&applied.network()?, &applied.device)?;
    match current.as_deref() {
        Some(local) if local == applied.source => {}
        Some(local) if previous.as_deref() != Some(config_digest(local).as_str()) => {
            applied.source = publish_config(
                client,
                state,
                &path,
                previous.as_deref(),
                &applied.source,
                local,
            )
            .await?;
        }
        _ => replace_config(&path, &current, &applied.source)?,
    }
    save_config_baseline(&baseline, &applied.source)?;
    Ok(applied)
}

async fn publish_config(
    client: &Client,
    state: &Path,
    path: &Path,
    previous: Option<&str>,
    remote: &str,
    local: &str,
) -> Result<String> {
    let expected = config_digest(remote);
    if previous.is_some_and(|hash| hash != expected) {
        let snapshot = state.join("host.sync");
        write_text(&snapshot, remote)?;
        bail!(
            "configuration changed on both devices; merge {} into {}",
            snapshot.display(),
            path.display()
        );
    }
    ensure!(
        read_optional_text(path)?.as_deref() == Some(local),
        "configuration changed while reading; retry sync"
    );
    let published = shared_config(
        client,
        Request::PublishConfig {
            expected,
            source: local.to_owned(),
        },
    )
    .await?;
    println!("Published {}", path.display());
    Ok(published)
}

fn replace_config(path: &Path, current: &Option<String>, source: &str) -> Result<()> {
    ensure!(
        &read_optional_text(path)? == current,
        "configuration changed while reading; retry sync"
    );
    write_text(path, source)
}

fn save_config_baseline(path: &Path, source: &str) -> Result<()> {
    let digest = config_digest(source);
    if read_json::<Option<String>>(path)?.as_ref() != Some(&digest) {
        write_json(path, &digest)?;
    }
    Ok(())
}

fn config_digest(source: &str) -> String {
    blake3::hash(source.as_bytes()).to_hex().to_string()
}

fn read_optional_text(path: &Path) -> Result<Option<String>> {
    match fs::read_to_string(path) {
        Ok(text) => Ok(Some(text)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn remote_path<'a>(host: &str, input: &'a str) -> Result<(Option<&'a str>, &'a str)> {
    let (device, path) = input
        .split_once(":/")
        .context("expected a path like server:/share/file")?;
    ensure!(device == host, "expected the host name '{host}'");
    if path.is_empty() {
        return Ok((None, ""));
    }
    let path = path.strip_suffix('/').unwrap_or(path);
    validate_path(path)?;
    let (share, rest) = path.split_once('/').unwrap_or((path, ""));
    Ok((Some(share), rest))
}

fn remote_file<'a>(host: &str, input: &'a str) -> Result<(&'a str, &'a str)> {
    let (share, path) = remote_path(host, input)?;
    ensure!(
        !path.is_empty() && !input.ends_with('/'),
        "expected a file path"
    );
    Ok((share.context("missing share name")?, path))
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("{error:#}");
        std::process::exit(1);
    }
}
