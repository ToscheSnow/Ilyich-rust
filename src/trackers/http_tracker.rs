use std::{
  net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
  time::Duration,
};

use tokio::{sync::mpsc::Sender, sync::watch, task::JoinHandle, time::sleep};

use crate::{peers::peer_manager::PeerManagerMessage, stats::StatsSnapshot};

#[derive(Clone)]
pub struct TrackerAnnounceArgs {
  pub info_hash: [u8; 20],
  pub peer_id: [u8; 20],
  pub port: u16,
}

#[derive(Clone)]
struct AnnounceCtx {
  info_hash: [u8; 20],
  peer_id: [u8; 20],
  port: u16,
}

pub struct HttpTracker {
  urls: Vec<String>,
  args: TrackerAnnounceArgs,
  stats_board: watch::Receiver<StatsSnapshot>,
  peer_manager_sender: Sender<PeerManagerMessage>,
  total_length: u64,
}

impl HttpTracker {
  pub fn new(
    urls: Vec<String>,
    args: TrackerAnnounceArgs,
    stats_board: watch::Receiver<StatsSnapshot>,
    peer_manager_sender: Sender<PeerManagerMessage>,
    total_length: u64,
  ) -> Self {
    Self {
      urls,
      args,
      stats_board,
      peer_manager_sender,
      total_length,
    }
  }

  // one task per tracker, so a slow or dead tracker never blocks the others
  pub fn run(self) -> Vec<JoinHandle<()>> {
    let client = reqwest::Client::builder()
      .timeout(Duration::from_secs(15))
      .build()
      .expect("http client");
    let ctx = AnnounceCtx {
      info_hash: self.args.info_hash,
      peer_id: self.args.peer_id,
      port: self.args.port,
    };

    self
      .urls
      .into_iter()
      .map(|url| {
        tokio::spawn(track(
          url,
          ctx.clone(),
          client.clone(),
          self.stats_board.clone(),
          self.peer_manager_sender.clone(),
          self.total_length,
        ))
      })
      .collect()
  }
}
async fn track(
  url: String,
  ctx: AnnounceCtx,
  client: reqwest::Client,
  stats: watch::Receiver<StatsSnapshot>,
  peer_tx: Sender<PeerManagerMessage>,
  total_length: u64,
) {
  let mut first = true;

  loop {
    let snap = stats.borrow().clone();
    let left = total_length.saturating_sub(snap.verified_bytes);

    let mut full = format!(
      "{}{}info_hash={}&peer_id={}&port={}&uploaded={}&downloaded={}&left={}&compact=1&numwant=200",
      url,
      if url.contains('?') { '&' } else { '?' },
      url_encode(&ctx.info_hash),
      url_encode(&ctx.peer_id),
      ctx.port,
      snap.uploaded_bytes,
      snap.bytes_downloaded_this_session,
      left,
    );
    if first {
      full.push_str("&event=started");
    }

    match announce(&client, &full).await {
      Ok((peers, _interval)) => {
        first = false;
        if !peers.is_empty()
          && peer_tx
            .send(PeerManagerMessage::AttemptConnection(peers))
            .await
            .is_err()
        {
          return; // peer manager is gone, we are shutting down
        }
      }
      // failed once = this tracker is done for good, same as the TS failedTrackers set 🗑️
      Err(_) => return,
    }

    sleep(Duration::from_secs(120)).await;
  }
}

async fn announce(client: &reqwest::Client, url: &str) -> Result<(Vec<SocketAddr>, u64), String> {
  let resp = client.get(url).send().await.map_err(|e| e.to_string())?;
  if !resp.status().is_success() {
    return Err(format!("status {}", resp.status()));
  }
  let body = resp.bytes().await.map_err(|e| e.to_string())?;
  parse_response(&body)
}

fn parse_response(body: &[u8]) -> Result<(Vec<SocketAddr>, u64), String> {
  let mut pos = 0;
  let Some(B::Dict(dict)) = parse(body, &mut pos) else {
    return Err("bad bencode".into());
  };
  let get = |k: &[u8]| dict.iter().find(|(key, _)| key == k).map(|(_, v)| v);

  if let Some(B::Bytes(reason)) = get(b"failure reason") {
    return Err(String::from_utf8_lossy(reason).into_owned());
  }

  let interval = match get(b"interval") {
    Some(B::Int(i)) if *i > 0 => *i as u64,
    _ => 120,
  };

  let mut peers = Vec::new();

  match get(b"peers") {
    // compact: 6 bytes per peer
    Some(B::Bytes(b)) => {
      for c in b.chunks_exact(6) {
        let ip = Ipv4Addr::new(c[0], c[1], c[2], c[3]);
        peers.push(SocketAddr::new(
          IpAddr::V4(ip),
          u16::from_be_bytes([c[4], c[5]]),
        ));
      }
    }
    // non-compact: list of dicts, empty swarms are often an empty list
    Some(B::List(list)) => {
      for entry in list {
        let B::Dict(d) = entry else { continue };
        let ip = d.iter().find(|(k, _)| k == b"ip").map(|(_, v)| v);
        let port = d.iter().find(|(k, _)| k == b"port").map(|(_, v)| v);
        if let (Some(B::Bytes(ip)), Some(B::Int(port))) = (ip, port) {
          if let Ok(ip) = String::from_utf8_lossy(ip).parse::<IpAddr>() {
            peers.push(SocketAddr::new(ip, *port as u16));
          }
        }
      }
    }
    _ => {}
  }

  // ipv6 compact: 18 bytes per peer
  if let Some(B::Bytes(b)) = get(b"peers6") {
    for c in b.chunks_exact(18) {
      let mut octets = [0u8; 16];
      octets.copy_from_slice(&c[..16]);
      peers.push(SocketAddr::new(
        IpAddr::V6(Ipv6Addr::from(octets)),
        u16::from_be_bytes([c[16], c[17]]),
      ));
    }
  }

  Ok((peers, interval))
}

// info_hash and peer_id are raw bytes, so they must be percent-encoded byte by byte
fn url_encode(bytes: &[u8]) -> String {
  let mut out = String::with_capacity(bytes.len() * 3);
  for &b in bytes {
    if b.is_ascii_alphanumeric() || b"-_.~".contains(&b) {
      out.push(b as char);
    } else {
      out.push_str(&format!("%{:02X}", b));
    }
  }
  out
}

// tiny private bencode reader, so this file depends on none of your parser types
enum B {
  Int(i64),
  Bytes(Vec<u8>),
  List(Vec<B>),
  Dict(Vec<(Vec<u8>, B)>),
}

fn parse(buf: &[u8], pos: &mut usize) -> Option<B> {
  match *buf.get(*pos)? {
    b'i' => {
      *pos += 1;
      let end = *pos + buf[*pos..].iter().position(|&c| c == b'e')?;
      let n = std::str::from_utf8(&buf[*pos..end]).ok()?.parse().ok()?;
      *pos = end + 1;
      Some(B::Int(n))
    }
    b'l' => {
      *pos += 1;
      let mut items = Vec::new();
      while *buf.get(*pos)? != b'e' {
        items.push(parse(buf, pos)?);
      }
      *pos += 1;
      Some(B::List(items))
    }
    b'd' => {
      *pos += 1;
      let mut items = Vec::new();
      while *buf.get(*pos)? != b'e' {
        let B::Bytes(k) = parse(buf, pos)? else {
          return None;
        };
        items.push((k, parse(buf, pos)?));
      }
      *pos += 1;
      Some(B::Dict(items))
    }
    b'0'..=b'9' => {
      let colon = *pos + buf[*pos..].iter().position(|&c| c == b':')?;
      let len: usize = std::str::from_utf8(&buf[*pos..colon]).ok()?.parse().ok()?;
      let start = colon + 1;
      let end = start.checked_add(len)?;
      let data = buf.get(start..end)?.to_vec();
      *pos = end;
      Some(B::Bytes(data))
    }
    _ => None,
  }
}
