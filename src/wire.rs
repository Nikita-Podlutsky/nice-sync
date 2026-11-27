use crate::transfer::PACK_BYTES;
use anyhow::{Context, Result, ensure};
use iroh::{
    Endpoint, EndpointAddr, RelayMode, SecretKey,
    endpoint::{Connection, RecvStream, SendStream, presets},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::{net::SocketAddr, time::Duration};
use tokio::io::AsyncReadExt;

const ALPN: &[u8] = b"nice-sync/1";
const REQUEST_LIMIT: usize = 4 * 1024 * 1024;
const RESPONSE_LIMIT: usize = 8 * 1024 * 1024;
pub const TIMEOUT: Duration = Duration::from_secs(120);

pub async fn bind(key: SecretKey, direct: Option<SocketAddr>) -> Result<Endpoint> {
    let builder = match direct {
        Some(address) => Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Disabled)
            .clear_ip_transports()
            .bind_addr(address)?,
        None => Endpoint::builder(presets::N0),
    };
    Ok(builder
        .secret_key(key)
        .alpns(vec![ALPN.to_vec()])
        .bind()
        .await?)
}

pub async fn connect(endpoint: &Endpoint, address: EndpointAddr) -> Result<Connection> {
    Ok(
        tokio::time::timeout(Duration::from_secs(25), endpoint.connect(address, ALPN))
            .await
            .context("timed out connecting to the Nice Sync host (25 seconds)")??,
    )
}

pub async fn read_request<T: DeserializeOwned>(receive: &mut RecvStream) -> Result<(T, Vec<u8>)> {
    read_packet(receive, REQUEST_LIMIT).await
}

pub async fn reply<T: Serialize>(
    send: &mut SendStream,
    result: Result<(T, Vec<u8>)>,
) -> Result<()> {
    match result {
        Ok((value, data)) => {
            let response = serde_json::to_vec(&Reply::Success(value))?;
            if response.len() <= RESPONSE_LIMIT {
                return send_packet(send, response, &data, RESPONSE_LIMIT).await;
            }
            let response: Reply<()> = Reply::Error("response metadata exceeds limit".into());
            send_packet(send, serde_json::to_vec(&response)?, &[], RESPONSE_LIMIT).await
        }
        Err(error) => {
            let response: Reply<()> = Reply::Error(format!("{error:#}"));
            send_packet(send, serde_json::to_vec(&response)?, &[], RESPONSE_LIMIT).await
        }
    }
}

#[derive(Serialize, Deserialize)]
enum Reply<T> {
    Success(T),
    Error(String),
}

pub async fn call<Q: Serialize, A: DeserializeOwned>(
    connection: &Connection,
    request: &Q,
    data: &[u8],
) -> Result<(A, Vec<u8>)> {
    let (mut send, mut receive) = tokio::time::timeout(TIMEOUT, connection.open_bi())
        .await
        .context("timed out opening a Nice Sync stream")??;
    send_packet(&mut send, serde_json::to_vec(request)?, data, REQUEST_LIMIT).await?;
    let (reply, data) = read_packet(&mut receive, RESPONSE_LIMIT).await?;
    match reply {
        Reply::Success(value) => Ok((value, data)),
        Reply::Error(error) => Err(anyhow::Error::msg(error)),
    }
}

async fn send_packet(
    send: &mut SendStream,
    header: Vec<u8>,
    data: &[u8],
    limit: usize,
) -> Result<()> {
    ensure!(
        header.len() <= limit && data.len() <= PACK_BYTES,
        "packet exceeds limit"
    );
    let mut prefix = [0; 8];
    prefix[..4].copy_from_slice(&(header.len() as u32).to_be_bytes());
    prefix[4..].copy_from_slice(&(data.len() as u32).to_be_bytes());
    for mut bytes in [&prefix[..], &header, data] {
        while !bytes.is_empty() {
            let written = tokio::time::timeout(TIMEOUT, send.write(bytes))
                .await
                .context("no write progress for 120 seconds")??;
            ensure!(written != 0, "stream stopped writing");
            bytes = &bytes[written..];
        }
    }
    send.finish()?;
    Ok(())
}

async fn read_packet<T: DeserializeOwned>(
    receive: &mut RecvStream,
    limit: usize,
) -> Result<(T, Vec<u8>)> {
    let (header_len, data_len) = tokio::time::timeout(TIMEOUT, async {
        Ok::<_, std::io::Error>((
            receive.read_u32().await? as usize,
            receive.read_u32().await? as usize,
        ))
    })
    .await
    .context("timed out waiting for the Nice Sync packet header")??;
    ensure!(
        header_len <= limit && data_len <= PACK_BYTES,
        "packet exceeds limit"
    );
    let mut header = vec![0; header_len];
    let mut data = vec![0; data_len];
    for mut bytes in [&mut header[..], &mut data[..]] {
        while !bytes.is_empty() {
            let length = tokio::time::timeout(TIMEOUT, receive.read(bytes))
                .await
                .context("no read progress for 120 seconds")??
                .context("truncated packet")?;
            ensure!(length != 0, "stream stopped reading");
            bytes = &mut bytes[length..];
        }
    }
    ensure!(
        tokio::time::timeout(TIMEOUT, receive.read(&mut [0]))
            .await
            .context("timed out waiting for the end of the packet")??
            .is_none(),
        "unexpected packet data"
    );
    Ok((serde_json::from_slice(&header)?, data))
}
