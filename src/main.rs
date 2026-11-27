use anyhow::{Context, Result, bail, ensure};
use clap::{Parser, Subcommand};
use nice_sync::{
    access::{self, Code, Destination, Membership},
    language::{self, Applied},
    peer::{self, Answer, Client, Request},
    store::{Store, lock_directory, read_json, validate_path, write_json},
    sync,
};
use std::{
    fs,
    io::{self, IsTerminal},
    net::SocketAddr,
    path::PathBuf,
    time::Duration,
};

#[derive(Parser)]
#[command(
    version,
    about = "Local archives and file synchronization between your devices"
)]
struct Cli {
    /// State directory for this device.
    #[arg(long, global = true, default_value = ".nice-sync")]
    state: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
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
            conflicts_with = "configured",
            required_unless_present = "configured"
        )]
        template: Option<String>,
        /// Use this device's permit declarations. Available on the host.
        #[arg(long)]
        configured: bool,
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
    Sync {
        /// Trust and refresh the host's shared configuration before each sync.
        #[arg(long)]
        follow_config: bool,
        #[arg(long)]
        share: Option<String>,
        /// Repeat synchronization at this interval in seconds.
        #[arg(long, value_parser = clap::value_parser!(u64).range(1..))]
        every: Option<u64>,
    },
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
    match command {
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
        } => peer::serve(&state, direct, share_config).await?,
        Command::Invite {
            device,
            template,
            qr,
            ..
        } => {
            let code = if state.join("joined.json").exists() {
                let template =
                    template.context("only the host can issue configured permissions")?;
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
                    template.as_deref(),
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
                sync_once(&client, &state, &member, true, None).await?;
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
            let location = path
                .as_deref()
                .map(|path| remote_path(&member.destination.host, path))
                .transpose()?;
            let request = match location {
                Some((Some(share), directory)) => Request::List {
                    share: share.to_owned(),
                    directory: directory.to_owned(),
                },
                _ => Request::Roots,
            };
            let client = Client::connect(&state, &member.destination).await?;
            let (answer, _) = client.call(request, &[]).await?;
            let Answer::Names(names) = answer else {
                bail!("expected directory listing")
            };
            for name in names {
                println!("{name}");
            }
            client.close().await;
        }
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
        Command::Sync {
            share,
            every,
            follow_config,
        } => {
            let _lock = lock_directory(&state.join("runtime"))?;
            let member = Membership::load(&state)?;
            loop {
                let result = async {
                    let client = Client::connect(&state, &member.destination).await?;
                    let result =
                        sync_once(&client, &state, &member, follow_config, share.as_deref()).await;
                    client.close().await;
                    result
                }
                .await;
                let Some(seconds) = every else { return result };
                if let Err(error) = result {
                    eprintln!("{error:#}");
                }
                tokio::time::sleep(Duration::from_secs(seconds)).await;
            }
        }
    }
    Ok(())
}

async fn sync_once(
    client: &Client,
    state: &std::path::Path,
    member: &Membership,
    follow_config: bool,
    share: Option<&str>,
) -> Result<()> {
    let local = if follow_config {
        let (answer, _) = client.call(Request::SharedConfig, &[]).await?;
        let Answer::SharedConfig(source) = answer else {
            bail!("expected shared configuration")
        };
        Applied {
            source,
            base: std::env::current_dir()?,
            device: member.device.clone(),
        }
    } else {
        Applied::load(state)?
    };
    let network = local.network()?;
    member.check(&network, &local.device)?;
    if follow_config {
        use std::io::Write;
        let mut snapshot = tempfile::NamedTempFile::new_in(state)?;
        snapshot.write_all(local.source.as_bytes())?;
        snapshot.as_file().sync_all()?;
        snapshot.persist(state.join("shared.sync"))?;
    }
    sync::sync(client, state, &local, &network, share).await
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
