use anyhow::{Context, Result, ensure};
use nice_sync::{
    access::{self, Code, Destination},
    language::Applied,
    peer::{Answer, Client, Request},
    store::{Version, fingerprint},
    transfer::CHUNK,
};
use std::{
    fs,
    io::Write,
    path::PathBuf,
    process::{Child, Command, Output, Stdio},
    thread,
    time::Duration,
};

const SAMPLE: &str = include_str!("../examples/network.sync");

struct Fixture {
    root: tempfile::TempDir,
    config: PathBuf,
}

struct Host(Child);

impl Drop for Host {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

impl Fixture {
    fn new(source: &str) -> Result<Self> {
        let root = tempfile::tempdir()?;
        let config = root.path().join("network.sync");
        fs::write(&config, source)?;
        Ok(Self { root, config })
    }

    fn path(&self, path: &str) -> PathBuf {
        self.root.path().join(path)
    }

    fn command(&self, device: &str) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_nice-sync"));
        command
            .arg("--state")
            .arg(self.path(&format!("state-{device}")))
            .current_dir(self.root.path());
        command
    }

    fn run(&self, device: &str, args: &[&str]) -> Result<String> {
        let output = self.command(device).args(args).output()?;
        success(output, args[0])
    }

    fn failure(&self, device: &str, args: &[&str], expected: &str) -> Result<()> {
        let output = self.command(device).args(args).output()?;
        let message = String::from_utf8_lossy(&output.stderr);
        ensure!(
            !output.status.success() && message.contains(expected),
            "{args:?}, expected '{expected}', got {message}"
        );
        Ok(())
    }

    fn apply(&self, device: &str) -> Result<()> {
        self.run(
            device,
            &[
                "apply",
                self.config.to_str().context("UTF-8 test path")?,
                "--device",
                device,
            ],
        )?;
        Ok(())
    }

    fn host(&self) -> Result<Host> {
        self.listen("127.0.0.1:0", false)
    }

    fn listen(&self, address: &str, share_config: bool) -> Result<Host> {
        self.apply("server")?;
        let mut command = self.command("server");
        command.args(["serve", "--direct", address]);
        if share_config {
            command.arg("--share-config");
        }
        let mut host = Host(command.stdout(Stdio::null()).spawn()?);
        for _ in 0..250 {
            if self.path("state-server/listener.json").exists() {
                return Ok(host);
            }
            ensure!(host.0.try_wait()?.is_none(), "host exited before listening");
            thread::sleep(Duration::from_millis(40));
        }
        anyhow::bail!("host did not start")
    }

    fn write(&self, name: &str, bytes: impl AsRef<[u8]>) -> Result<()> {
        let path = self.path(name);
        fs::create_dir_all(path.parent().context("fixture path needs parent")?)?;
        fs::write(path, bytes)?;
        Ok(())
    }

    fn join(&self, device: &str) -> Result<String> {
        let code = self.run("server", &["invite", "--configured", "--device", device])?;
        let code = code.trim();
        let mut join = self
            .command(device)
            .arg("join")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;
        writeln!(join.stdin.take().context("no stdin for join")?, "{code}")?;
        success(join.wait_with_output()?, "join via stdin")?;
        self.apply(device)?;
        Ok(code.to_owned())
    }

    fn assert_contents(&self, name: &str, expected: &str) -> Result<()> {
        assert_eq!(fs::read_to_string(self.path(name))?, expected, "{name}");
        Ok(())
    }
}

#[test]
fn join_uses_host_policy_and_follows_updates() -> Result<()> {
    let fixture = Fixture::new(SAMPLE)?;
    fixture.write("server-files/docs/notes.txt", "from the host")?;
    for number in 0..80 {
        fixture.write(
            &format!("server-files/docs/item-{number:02}.txt"),
            format!("item {number}"),
        )?;
    }
    fixture.write("server-files/docs/copy-a.txt", "same bytes")?;
    fixture.write("server-files/docs/copy-b.txt", "same bytes")?;
    fs::create_dir_all(fixture.path("laptop-files/docs"))?;
    let mut host = fixture.listen("127.0.0.1:0", true)?;
    let invitation = fixture.run("server", &["invite", "--configured", "--device", "laptop"])?;
    fixture.run("laptop", &["join", "--sync", invitation.trim()])?;
    assert!(!fixture.path("state-laptop/applied.json").exists());
    fixture.assert_contents("laptop-files/docs/notes.txt", "from the host")?;
    for number in 0..80 {
        fixture.assert_contents(
            &format!("laptop-files/docs/item-{number:02}.txt"),
            &format!("item {number}"),
        )?;
    }
    fixture.assert_contents("laptop-files/docs/copy-a.txt", "same bytes")?;
    fixture.assert_contents("laptop-files/docs/copy-b.txt", "same bytes")?;
    assert!(
        fixture
            .run("laptop", &["sync", "--follow-config", "--share", "docs"])?
            .contains("0 changes")
    );
    fixture.assert_contents("state-laptop/shared.sync", SAMPLE)?;

    fs::write(&fixture.config, "network home {")?;
    thread::sleep(Duration::from_millis(1200));
    fixture.run("laptop", &["sync", "--follow-config"])?;
    fixture.assert_contents("state-laptop/shared.sync", SAMPLE)?;
    let updated = SAMPLE.replace(
        "ignore directory \"**/.venv\"",
        "ignore directory \"**/.venv\"\n    ignore file \"**/skip.txt\"",
    );
    fs::write(&fixture.config, &updated)?;
    fixture.write("server-files/docs/skip.txt", "excluded by the new policy")?;
    fixture.write("server-files/docs/notes.txt", "updated")?;
    wait_for_config(&fixture, &mut host, &updated)?;
    fixture.run("laptop", &["sync", "--follow-config"])?;
    fixture.assert_contents("state-laptop/shared.sync", &updated)?;
    fixture.assert_contents("laptop-files/docs/notes.txt", "updated")?;
    assert!(!fixture.path("laptop-files/docs/skip.txt").exists());

    let moved = updated
        .replace("\"server-files/docs\"", "\"server-files/Учеба\"")
        .replace("\"laptop-files/docs\"", "\"laptop-files/Учеба\"");
    fixture.write("server-files/Учеба/lesson.txt", "new publication root")?;
    fs::create_dir_all(fixture.path("laptop-files/Учеба"))?;
    fs::write(&fixture.config, &moved)?;
    wait_for_config(&fixture, &mut host, &moved)?;
    fixture.run("laptop", &["sync", "--follow-config"])?;
    fixture.assert_contents("laptop-files/Учеба/lesson.txt", "new publication root")?;
    assert!(!fixture.path("server-files/Учеба/notes.txt").exists());
    assert!(!fixture.path("laptop-files/Учеба/notes.txt").exists());
    fixture.assert_contents("server-files/docs/notes.txt", "updated")?;

    fs::write(&fixture.config, &updated)?;
    wait_for_config(&fixture, &mut host, &updated)?;
    fixture.run("laptop", &["sync", "--follow-config"])?;
    fixture.assert_contents("laptop-files/docs/notes.txt", "updated")?;
    assert!(!fixture.path("server-files/docs/lesson.txt").exists());
    Ok(())
}

fn wait_for_config(fixture: &Fixture, host: &mut Host, source: &str) -> Result<()> {
    let active = fixture.path("state-server/applied.json");
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let applied: Applied = nice_sync::store::read_json(&active)?;
        if applied.source == source {
            return Ok(());
        }
        ensure!(host.0.try_wait()?.is_none(), "host exited during reload");
        ensure!(
            std::time::Instant::now() < deadline,
            "host did not reload the configuration"
        );
        thread::sleep(Duration::from_millis(50));
    }
}

#[test]
fn changing_root_at_start_keeps_the_legacy_archive_separate() -> Result<()> {
    let fixture = Fixture::new(SAMPLE)?;
    fixture.write("server-files/docs/old.txt", "legacy archive")?;
    let host = fixture.host()?;
    let invitation = Code::read(&fixture.join("laptop")?)?;
    let address = invitation
        .destination
        .endpoint
        .ip_addrs()
        .next()
        .context("missing listening address")?
        .to_string();
    assert_eq!(
        fixture.run("laptop", &["ls", "server:/docs"])?.trim(),
        "old.txt"
    );
    drop(host);
    fs::remove_file(fixture.path("state-server/listener.json"))?;
    fs::remove_file(fixture.path("state-server/shares/docs/primary-root.json"))?;

    let moved = SAMPLE.replace("\"server-files/docs\"", "\"server-files/Учеба\"");
    fs::write(&fixture.config, &moved)?;
    fixture.write("server-files/Учеба/lesson.txt", "new archive")?;
    let _host = fixture.listen(&address, false)?;
    assert_eq!(
        fixture.run("laptop", &["ls", "server:/docs"])?.trim(),
        "lesson.txt"
    );
    assert!(!fixture.path("server-files/Учеба/old.txt").exists());
    fixture.assert_contents("server-files/docs/old.txt", "legacy archive")?;
    let legacy = fs::read_to_string(fixture.path("state-server/shares/docs/catalog.json"))?;
    assert!(legacy.contains("old.txt"));
    assert!(!legacy.contains("lesson.txt"));
    Ok(())
}

#[test]
fn directs_sync_to_an_explicit_socket_without_changing_registration() -> Result<()> {
    let fixture = Fixture::new(SAMPLE)?;
    fixture.write("server-files/docs/notes.txt", "downloaded directly")?;
    fs::create_dir_all(fixture.path("laptop-files/docs"))?;
    let _host = fixture.listen("127.0.0.1:0", true)?;
    let code = fixture.join("laptop")?;
    let address = Code::read(&code)?
        .destination
        .endpoint
        .ip_addrs()
        .next()
        .context("missing listening address")?
        .to_string();

    let state = fixture.path("state-laptop");
    let mut member = access::Membership::load(&state)?;
    member.destination.endpoint = iroh::EndpointAddr::new(member.destination.endpoint.id)
        .with_ip_addr("127.0.0.1:1".parse()?);
    let file = state.join("joined.json");
    nice_sync::store::write_json(&file, &member)?;
    let stored = fs::read(&file)?;

    let result = fixture
        .command("laptop")
        .args([
            "sync",
            "--follow-config",
            "--share",
            "docs",
            "--direct",
            &address,
            "--show-route",
        ])
        .output()?;
    assert!(
        String::from_utf8_lossy(&result.stderr).contains(&format!("Nice Sync route: ip:{address}"))
    );
    success(result, "sync through the explicit socket")?;
    fixture.assert_contents("laptop-files/docs/notes.txt", "downloaded directly")?;
    assert_eq!(fs::read(file)?, stored);
    Ok(())
}

#[test]
fn p2p_sync_reconnects_after_discovery_and_preserves_membership() -> Result<()> {
    let fixture = Fixture::new(SAMPLE)?;
    fixture.write("server-files/docs/notes.txt", "downloaded peer to peer")?;
    fs::create_dir_all(fixture.path("laptop-files/docs"))?;
    let _host = fixture.listen("127.0.0.1:0", true)?;
    fixture.join("laptop")?;
    let state = fixture.path("state-laptop");
    let mut member = access::Membership::load(&state)?;
    member.destination.direct = false;
    let file = state.join("joined.json");
    nice_sync::store::write_json(&file, &member)?;
    let stored = fs::read(&file)?;

    let result = fixture
        .command("laptop")
        .args([
            "sync",
            "--follow-config",
            "--share",
            "docs",
            "--p2p-only",
            "--show-route",
        ])
        .output()?;
    let stderr = String::from_utf8_lossy(&result.stderr).into_owned();
    success(result, "sync using discovered P2P path")?;
    assert!(stderr.contains("P2P-only transfer via 127.0.0.1:"));
    assert!(stderr.contains("Nice Sync route: ip:127.0.0.1:"));
    fixture.assert_contents("laptop-files/docs/notes.txt", "downloaded peer to peer")?;
    assert_eq!(fs::read(file)?, stored);
    Ok(())
}

fn success(output: Output, operation: &str) -> Result<String> {
    ensure!(
        output.status.success(),
        "{operation}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(String::from_utf8(output.stdout)?)
}

#[test]
fn bulk_sync_exchanges_multiple_packets_in_both_directions() -> Result<()> {
    let fixture = Fixture::new(SAMPLE)?;
    for number in 0..520 {
        fixture.write(
            &format!("server-files/docs/remote/{number}.txt"),
            format!("remote {number}"),
        )?;
        fixture.write(
            &format!("laptop-files/docs/local/{number}.txt"),
            format!("local {number}"),
        )?;
    }
    let large = vec![57; 9 * 1024 * 1024];
    fixture.write("server-files/docs/remote.bin", &large)?;
    fixture.write("laptop-files/docs/local.bin", &large)?;
    let _host = fixture.listen("127.0.0.1:0", true)?;
    fixture.join("laptop")?;
    fixture.run("laptop", &["sync", "--follow-config", "--share", "docs"])?;
    for number in 0..520 {
        fixture.assert_contents(
            &format!("laptop-files/docs/remote/{number}.txt"),
            &format!("remote {number}"),
        )?;
        fixture.assert_contents(
            &format!("server-files/docs/local/{number}.txt"),
            &format!("local {number}"),
        )?;
    }
    assert_eq!(
        fs::read(fixture.path("laptop-files/docs/remote.bin"))?,
        large
    );
    assert_eq!(
        fs::read(fixture.path("server-files/docs/local.bin"))?,
        large
    );
    assert!(
        fixture
            .run("laptop", &["sync", "--follow-config", "--share", "docs"])?
            .contains("0 changes")
    );
    Ok(())
}

#[tokio::test]
async fn resumes_after_disconnect_and_rejects_invalid_or_reused_invitations() -> Result<()> {
    let fixture = Fixture::new(SAMPLE)?;
    let _host = fixture.host()?;
    let code = fixture.join("phone")?;
    fixture.run("phone", &["join", &code])?;
    let parsed = Code::read(&code)?;
    let state = fixture.path("state-phone");
    let outsider = fixture.path("outside");
    let client = Client::connect(&outsider, &parsed.destination).await?;
    let error = client
        .call(
            Request::Enroll {
                token: parsed.token,
                device: "phone".into(),
            },
            &[],
        )
        .await
        .err()
        .context("reused invitation accepted")?;
    assert!(error.to_string().contains("already used"));
    client.close().await;

    let data = vec![42u8; CHUNK + 173];
    let blob = fingerprint(&mut data.as_slice())?;
    let prepare = || Request::Prepare {
        share: "image".into(),
        path: "camera.jpg".into(),
        blob: blob.clone(),
    };
    let chunk = |offset| Request::Chunk {
        share: "image".into(),
        path: "camera.jpg".into(),
        blob: blob.clone(),
        offset,
    };
    let commit = || Request::Commit {
        share: "image".into(),
        path: "camera.jpg".into(),
        blob: Some(blob.clone()),
        base: None,
    };
    let client = Client::connect(&state, &parsed.destination).await?;
    assert!(matches!(
        client.call(prepare(), &[]).await?.0,
        Answer::Offset(0)
    ));
    assert!(client.call(commit(), &[]).await.is_err());
    assert!(
        matches!(client.call(chunk(0), &data[..CHUNK]).await?.0, Answer::Offset(offset) if offset == CHUNK as u64)
    );
    assert!(!fixture.path("server-files/photos/camera.jpg").exists());
    client.close().await;

    let client = Client::connect(&state, &parsed.destination).await?;
    assert!(
        matches!(client.call(prepare(), &[]).await?.0, Answer::Offset(offset) if offset == CHUNK as u64)
    );
    assert!(client.call(chunk(0), &data[CHUNK..]).await.is_err());
    client.call(chunk(CHUNK as u64), &[0; 173]).await?;
    assert!(
        client
            .call(commit(), &[])
            .await
            .err()
            .context("wrong hash was accepted")?
            .to_string()
            .contains("hash mismatch")
    );
    assert!(matches!(
        client.call(prepare(), &[]).await?.0,
        Answer::Offset(0)
    ));
    client.call(chunk(0), &data[..CHUNK]).await?;
    client.call(chunk(CHUNK as u64), &data[CHUNK..]).await?;
    client.call(commit(), &[]).await?;
    assert_eq!(
        fs::read(fixture.path("server-files/photos/camera.jpg"))?,
        data
    );
    assert!(
        client
            .call(
                Request::History {
                    share: "image".into(),
                    path: "../docs/private".into()
                },
                &[]
            )
            .await
            .is_err()
    );
    assert!(
        client
            .call(
                Request::List {
                    share: "docs".into(),
                    directory: String::new()
                },
                &[]
            )
            .await
            .is_err()
    );
    fixture.run(
        "phone",
        &[
            "get",
            "server:/image/camera.jpg",
            "download.jpg",
            "--version",
            "1",
        ],
    )?;
    assert_eq!(fs::read(fixture.path("download.jpg"))?, data);
    let bulk = vec![17; CHUNK + 129];
    let bulk_blob = fingerprint(&mut bulk.as_slice())?;
    let write = |offset: usize, length| Request::BulkWrite {
        share: "image".into(),
        parts: vec![nice_sync::peer::WritePart {
            path: "bulk.jpg".into(),
            blob: bulk_blob.clone(),
            offset: offset as u64,
            length,
        }],
    };
    let changes = || {
        vec![nice_sync::published::Change {
            path: "bulk.jpg".into(),
            base: None,
            blob: Some(bulk_blob.clone()),
        }]
    };
    client.call(write(0, CHUNK), &bulk[..CHUNK]).await?;
    client.close().await;
    let client = Client::connect(&state, &parsed.destination).await?;
    let (answer, _) = client
        .call(
            Request::BulkPrepare {
                share: "image".into(),
                changes: changes(),
            },
            &[],
        )
        .await?;
    assert!(matches!(answer, Answer::BulkOffsets(offsets) if offsets == [CHUNK as u64]));
    client
        .call(write(CHUNK, bulk.len() - CHUNK), &bulk[CHUNK..])
        .await?;
    client
        .call(
            Request::BulkCommit {
                share: "image".into(),
                changes: changes(),
            },
            &[],
        )
        .await?;
    assert_eq!(
        fs::read(fixture.path("server-files/photos/bulk.jpg"))?,
        bulk
    );
    fixture.run("server", &["revoke", "phone"])?;
    assert!(client.call(Request::Roots, &[]).await.is_err());
    client.close().await;
    fixture.failure("phone", &["join", &code], "invalid invitation")?;
    Ok(())
}

#[test]
fn expires_invitations_without_registering_a_device() -> Result<()> {
    let fixture = Fixture::new(SAMPLE)?;
    let state = fixture.path("state-server");
    let model = Applied::read(&fixture.config, "server".into())?.network()?;
    let server = iroh::SecretKey::generate();
    let node = iroh::SecretKey::generate().public();
    let destination = Destination {
        endpoint: server.public().into(),
        direct: false,
        network: "home".into(),
        host: "server".into(),
    };
    let code = access::issue(
        &state,
        &model,
        destination,
        None,
        Some("docs-laptop"),
        "laptop",
    )?;
    let now = access::epoch_seconds()?;
    assert!(access::enroll(&state, node, &code.token, "laptop", now + 900).is_err());
    assert!(access::registered(&state, node).is_err());
    Ok(())
}

#[test]
fn runs_archive_receive_checkout_and_mirror_from_the_cli() -> Result<()> {
    let fixture = Fixture::new(&SAMPLE.replace("\"**/*.gguf\"", "\"**/*.tmp\""))?;
    fixture.run("laptop", &["check"])?;
    fixture.run("laptop", &["plan", "--device", "laptop"])?;
    assert!(!fixture.path("state-laptop").exists());
    fixture.write("camera/2026/sun.jpg", "first")?;
    fixture.write("pc-files/work/.venv/bin/python", "ignored")?;
    fixture.write("pc-files/work/code.rs", "fn main() {}")?;
    fixture.write("pc-files/work/empty.txt", "")?;
    fixture.write("pc-files/work/old.gguf", "old weights")?;
    fixture.write("laptop-files/docs/notes.txt", "shared")?;
    let _host = fixture.host()?;
    fixture.join("phone")?;
    fixture.join("pc")?;
    fixture.join("laptop")?;
    fixture.run("phone", &["sync"])?;
    fixture.write("camera/2026/sun.jpg", "second")?;
    fixture.run("phone", &["sync"])?;
    fs::remove_file(fixture.path("camera/2026/sun.jpg"))?;
    fixture.run("phone", &["sync"])?;
    assert_eq!(fixture.run("phone", &["ls", "server:/"])?.trim(), "image");
    assert_eq!(
        fixture.run("phone", &["ls", "server:/image"])?.trim(),
        "2026/"
    );
    assert_eq!(
        fixture.run("phone", &["ls", "server:/image/2026/"])?.trim(),
        "sun.jpg"
    );
    fixture.run(
        "phone",
        &[
            "get",
            "server:/image/2026/sun.jpg",
            "download.jpg",
            "--version",
            "1",
        ],
    )?;
    fixture.assert_contents("download.jpg", "first")?;
    fixture.assert_contents("server-files/photos/2026/sun.jpg", "second")?;

    fixture.run("pc", &["sync"])?;
    fixture.run("laptop", &["sync"])?;
    fixture.assert_contents("laptop-files/code/old.gguf", "old weights")?;
    fixture.assert_contents("laptop-files/code/code.rs", "fn main() {}")?;
    fixture.assert_contents("laptop-files/code/empty.txt", "")?;
    assert!(!fixture.path("server-files/projects/.venv").exists());
    fs::write(&fixture.config, SAMPLE)?;
    fixture.apply("laptop")?;
    fixture.write("pc-files/work/new.gguf", "new weights")?;
    fixture.write("pc-files/work/code.rs", "fn main() { }")?;
    fixture.run("pc", &["sync"])?;
    fixture.run("laptop", &["sync"])?;
    fixture.assert_contents("laptop-files/code/old.gguf", "old weights")?;
    fixture.assert_contents("laptop-files/code/code.rs", "fn main() { }")?;
    assert!(!fixture.path("laptop-files/code/new.gguf").exists());
    fixture.run(
        "laptop",
        &["get", "server:/projects/new.gguf", "manual.gguf"],
    )?;
    fixture.assert_contents("manual.gguf", "new weights")?;

    fixture.write("server-files/docs/notes.txt", "host edit")?;
    fixture.write("laptop-files/docs/notes.txt", "laptop edit")?;
    fixture.run("laptop", &["sync", "--share", "docs"])?;
    fixture.assert_contents("laptop-files/docs/notes.txt", "host edit")?;
    let conflicts = fs::read_dir(fixture.path("laptop-files/docs"))?
        .collect::<std::io::Result<Vec<_>>>()?
        .into_iter()
        .map(|entry| entry.path())
        .filter(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .contains(".nice-conflict-")
        })
        .collect::<Vec<_>>();
    assert_eq!(conflicts.len(), 1);
    assert_eq!(fs::read_to_string(&conflicts[0])?, "laptop edit");
    assert!(
        fixture
            .run("laptop", &["sync", "--share", "docs"])?
            .contains("0 changes")
    );

    fs::remove_file(fixture.path("laptop-files/docs/notes.txt"))?;
    fixture.write("server-files/docs/notes.txt", "new host edit")?;
    fixture.run("laptop", &["sync", "--share", "docs"])?;
    fixture.assert_contents("laptop-files/docs/notes.txt", "new host edit")?;
    fs::remove_file(fixture.path("server-files/docs/notes.txt"))?;
    fixture.write("laptop-files/docs/notes.txt", "survives host deletion")?;
    fixture.run("laptop", &["sync", "--share", "docs"])?;
    fixture.assert_contents("server-files/docs/notes.txt", "survives host deletion")?;
    fs::remove_file(fixture.path("laptop-files/docs/notes.txt"))?;
    fixture.run("laptop", &["sync", "--share", "docs"])?;
    assert!(!fixture.path("server-files/docs/notes.txt").exists());
    let history = fixture.run("laptop", &["history", "server:/docs/notes.txt"])?;
    let history: Vec<Version> = serde_json::from_str(&history)?;
    assert!(history.last().context("no revisions")?.blob.is_none());
    Ok(())
}

#[test]
fn delegated_invitations_only_grant_the_template_permissions() -> Result<()> {
    let fixture = Fixture::new(SAMPLE)?;
    fixture.write(
        "server-files/docs/readme.txt",
        "readable with an invitation",
    )?;
    fixture.write("server-files/docs/.venv/cache.txt", "only by explicit path")?;
    let _host = fixture.host()?;
    let catalog = fixture.path("state-server/shares/docs/catalog.json");
    assert!(
        !catalog.exists(),
        "host should listen before scanning publications"
    );
    fixture.join("phone")?;
    let code = fixture.run(
        "phone",
        &[
            "invite",
            "--device",
            "laptop",
            "--template",
            "docs-laptop",
            "--qr",
        ],
    )?;
    let mut lines = code.lines();
    fixture.run(
        "laptop",
        &["join", lines.next().context("no invitation code")?],
    )?;
    assert!(lines.count() > 1, "QR code was not printed");
    assert!(!fixture.path("state-laptop/applied.json").exists());
    assert_eq!(fixture.run("laptop", &["ls"])?.trim(), "docs");
    assert!(
        !catalog.exists(),
        "listing publications does not scan their files"
    );
    assert_eq!(
        fixture.run("laptop", &["ls", "server:/docs"])?.trim(),
        "readme.txt"
    );
    assert!(!fs::read_to_string(&catalog)?.contains(".venv"));
    fixture.run(
        "laptop",
        &["get", "server:/docs/.venv/cache.txt", "cache.txt"],
    )?;
    fixture.assert_contents("cache.txt", "only by explicit path")?;
    fixture.run("laptop", &["get", "server:/docs/readme.txt", "joined.txt"])?;
    fixture.assert_contents("joined.txt", "readable with an invitation")?;
    fixture.failure("laptop", &["ls", "server:/projects"], "permission denied")?;
    Ok(())
}

#[cfg(unix)]
#[test]
fn published_symlinks_are_never_followed() -> Result<()> {
    let fixture = Fixture::new(SAMPLE)?;
    fixture.write("external/private", "outside the published root")?;
    fs::create_dir_all(fixture.path("server-files/photos"))?;
    std::os::unix::fs::symlink(
        fixture.path("external"),
        fixture.path("server-files/photos/link"),
    )?;
    let _host = fixture.host()?;
    fixture.join("phone")?;
    assert!(fixture.run("phone", &["ls", "server:/image"])?.is_empty());
    fixture.failure(
        "phone",
        &["get", "server:/image/link/private", "escaped"],
        "refusing link",
    )?;
    assert!(!fixture.path("escaped").exists());
    Ok(())
}
