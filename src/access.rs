use crate::{
    language::{Grants, Network},
    store::{lock_directory, read_json, write_json},
};
use anyhow::{Context, Result, ensure};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use iroh::{EndpointAddr, EndpointId, SecretKey};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Clone, Deserialize, Serialize)]
pub struct Destination {
    pub endpoint: EndpointAddr,
    pub direct: bool,
    pub network: String,
    pub host: String,
}

#[derive(Deserialize, Serialize)]
pub struct Membership {
    pub destination: Destination,
    pub device: String,
}

impl Membership {
    pub fn load(state: &Path) -> Result<Self> {
        read_json::<Option<Self>>(&state.join("joined.json"))?
            .context("join the network using an invitation first")
    }

    pub fn check(&self, network: &Network, local: &str) -> Result<()> {
        ensure!(
            self.device == local
                && self.destination.network == network.name
                && self.destination.host == network.host,
            "membership does not match the network and device in the policy"
        );
        Ok(())
    }
}

#[derive(Deserialize, Serialize)]
pub struct Code {
    pub destination: Destination,
    pub device: String,
    pub token: [u8; 32],
}

impl Code {
    pub fn read(code: &str) -> Result<Self> {
        let payload = code
            .trim()
            .strip_prefix("NS1-")
            .context("expected a Nice Sync invitation code")?;
        ensure!(payload.len() <= 8192, "invitation code is too long");
        Ok(serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload)?)?)
    }

    pub fn encode(&self) -> Result<String> {
        Ok(format!(
            "NS1-{}",
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(self)?)
        ))
    }
}

#[derive(Clone, Deserialize, Serialize)]
pub struct Device {
    pub name: String,
    pub grants: Grants,
}

#[derive(Deserialize, Serialize)]
struct Ticket {
    device: Device,
    expires: u64,
    used_by: Option<EndpointId>,
}

#[derive(Default, Deserialize, Serialize)]
struct Registry {
    tickets: BTreeMap<String, Ticket>,
    devices: BTreeMap<EndpointId, Device>,
}

pub fn key(state: &Path) -> Result<SecretKey> {
    let _lock = lock_directory(&state.join("identity"))?;
    let file = state.join("identity").join("key.json");
    if let Some(bytes) = read_json::<Option<[u8; 32]>>(&file)? {
        return Ok(SecretKey::from_bytes(&bytes));
    }
    let key = SecretKey::generate();
    write_json(&file, &key.to_bytes())?;
    Ok(key)
}

pub fn issue(
    state: &Path,
    network: &Network,
    destination: Destination,
    issuer: Option<EndpointId>,
    template: Option<&str>,
    device: &str,
) -> Result<Code> {
    ensure!(
        destination.network == network.name && destination.host == network.host,
        "listener does not match the applied policy; restart serve"
    );
    ensure!(
        network.devices.contains(device) && device != network.host,
        "unknown or reserved device '{device}'"
    );
    let directory = state.join("access");
    let _lock = lock_directory(&directory)?;
    let file = directory.join("registry.json");
    let mut registry: Registry = read_json(&file)?;
    let issuer = match issuer {
        Some(id) => {
            &registry
                .devices
                .get(&id)
                .context("device is not registered")?
                .name
        }
        None => &network.host,
    };
    let (grants, lifetime) = match template {
        Some(template) => {
            let invitation = network
                .invitations
                .get(template)
                .context("unknown invitation template")?;
            ensure!(
                invitation.issuers.contains(issuer),
                "device cannot issue this invitation"
            );
            (invitation.grants.clone(), invitation.expires)
        }
        None => {
            ensure!(
                issuer == &network.host,
                "only the host can issue configured permissions"
            );
            (
                network
                    .permits
                    .get(device)
                    .context("device has no configured permissions")?
                    .clone(),
                900,
            )
        }
    };
    let now = epoch_seconds()?;
    registry.tickets.retain(|_, ticket| ticket.expires > now);
    ensure!(
        !registry.devices.values().any(|entry| entry.name == device),
        "device name is already registered"
    );
    let token = SecretKey::generate().to_bytes();
    registry.tickets.insert(
        token_hash(&token),
        Ticket {
            device: Device {
                name: device.to_owned(),
                grants,
            },
            expires: now + lifetime,
            used_by: None,
        },
    );
    write_json(&file, &registry)?;
    Ok(Code {
        destination,
        device: device.to_owned(),
        token,
    })
}

pub fn enroll(
    state: &Path,
    peer: EndpointId,
    token: &[u8; 32],
    target: &str,
    now: u64,
) -> Result<Device> {
    let directory = state.join("access");
    let _lock = lock_directory(&directory)?;
    let file = directory.join("registry.json");
    let mut registry: Registry = read_json(&file)?;
    let ticket = registry
        .tickets
        .get_mut(&token_hash(token))
        .context("invalid invitation")?;
    ensure!(ticket.expires > now, "invitation has expired");
    ensure!(
        ticket.device.name == target,
        "invitation is for another device"
    );
    if let Some(existing) = ticket.used_by {
        ensure!(existing == peer, "invitation was already used");
        return registry
            .devices
            .get(&peer)
            .cloned()
            .context("device registration was revoked");
    }
    ensure!(
        !registry.devices.contains_key(&peer),
        "key is already registered"
    );
    ensure!(
        !registry
            .devices
            .values()
            .any(|device| device.name == target),
        "device name is already registered"
    );
    ticket.used_by = Some(peer);
    let device = ticket.device.clone();
    registry.devices.insert(peer, device.clone());
    write_json(&file, &registry)?;
    Ok(device)
}

pub fn registered(state: &Path, peer: EndpointId) -> Result<Device> {
    let directory = state.join("access");
    let file = directory.join("registry.json");
    let registry: Registry = read_json(&file)?;
    registry
        .devices
        .get(&peer)
        .cloned()
        .context("device is not registered")
}

pub fn registered_id(state: &Path, name: &str) -> Result<EndpointId> {
    let registry: Registry = read_json(&state.join("access/registry.json"))?;
    registry
        .devices
        .into_iter()
        .find_map(|(id, device)| (device.name == name).then_some(id))
        .with_context(|| format!("configuration editor '{name}' is not registered"))
}

pub fn revoke(state: &Path, device: &str) -> Result<()> {
    let directory = state.join("access");
    let _lock = lock_directory(&directory)?;
    let file = directory.join("registry.json");
    let mut registry: Registry = read_json(&file)?;
    let old_counts = (registry.devices.len(), registry.tickets.len());
    registry.devices.retain(|_, entry| entry.name != device);
    registry
        .tickets
        .retain(|_, ticket| ticket.device.name != device);
    ensure!(
        old_counts != (registry.devices.len(), registry.tickets.len()),
        "no registration or invitation for '{device}'"
    );
    write_json(&file, &registry)
}

pub fn epoch_seconds() -> Result<u64> {
    Ok(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs())
}

fn token_hash(token: &[u8; 32]) -> String {
    blake3::hash(token).to_hex().to_string()
}
