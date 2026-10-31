use std::{
  collections::HashSet,
  sync::{Arc, RwLock},
  time::Duration,
};

use bytes::{Bytes, BytesMut};
use tokio::{io::AsyncWriteExt, net::TcpStream, task::JoinSet, time::timeout};

use tokio::io::AsyncReadExt;

use tokio::sync::mpsc;

use crate::{
  event::TorrentEvent,
  peers::peer::{Peer, PeerHandle},
  recon::Recon,
};

enum PeerMessage {}

enum PeerStatus {
  Connected(String),
  Disconnected(String),
}

pub struct PeerManager {
  connecting_peers: HashSet<String>,
  active_peers: Arc<RwLock<HashSet<PeerHandle>>>,
  info_hash: Bytes,

  recon_listener: mpsc::UnboundedReceiver<TorrentEvent>,
  recon: Recon<TorrentEvent>,

  peer_message_sender: mpsc::Sender<PeerMessage>,
  peer_message_receiver: mpsc::Receiver<PeerMessage>,
}

impl PeerManager {
  fn add_addresses(&mut self, new_peer_addresses: Vec<String>) {
    for address in &new_peer_addresses {
      //trigger connect to new peers;
      if self.connecting_peers.contains(address) {
        continue;
      }

      self.connecting_peers.insert(address.clone());
      let peer_address = address.clone();

      let peer_status_transmitter = self.peer_status_sender.clone();

      let info_hash = self.info_hash.clone();

      let peer_recon = self.recon.clone();

      let _ = self.connections(async move {
        connect_peer(
          peer_recon,
          peer_address,
          peer_status_transmitter,
          info_hash,
          CLIENT_ID_BYTES,
        )
        .await;
      });
    }
  }

  pub fn new(
    active_peers: Arc<RwLock<HashSet<PeerHandle>>>,
    info_hash: Bytes,
    recon: Recon<TorrentEvent>,
    recon_listener: mpsc::UnboundedReceiver<TorrentEvent>,
  ) -> Self {
    let (peer_message_sender, peer_message_receiver) = mpsc::channel(1000);

    let manager = PeerManager {
      active_peers,
      connecting_peers: HashSet::new(),
      info_hash,

      recon_listener,
      recon,

      peer_message_sender,
      peer_message_receiver,

      connections: JoinSet::new(),
    };

    return manager;
  }

  pub async fn start(&mut self) {
    loop {
      tokio::select! {
          Some(torrent_event) = self.recon_listener.recv() => {
            // process torrent event
          }

          Some(peer_event) = self.peer_message_receiver.recv() => {
            // process peer event
          }

          Some(result) = self.connections.join_next() => {

          }
      }
    }
  }
}

async fn connect_peer(
  recon: Recon<TorrentEvent>,
  peer_address: String,
  transmitter: mpsc::UnboundedSender<PeerStatus>,
  info_hash: Bytes,
  CLIENT_ID: &'static [u8],
) {
  let torrent_handshake = handshake_buf(info_hash, Bytes::from(CLIENT_ID));

  let attempt = timeout(
    Duration::from_secs(10),
    dial(&peer_address, torrent_handshake.clone()),
  )
  .await;

  let Ok(Some((stream, buf, received_handshake))) = attempt else {
    // println!("");
    return;
  };

  let HandshakeRes {
    is_successful,
    remote_peer_id,
  } = verify_handshake(torrent_handshake, received_handshake);

  if !is_successful {
    return;
  }

  if transmitter
    .send(PeerStatus::Connected(peer_address))
    .is_err()
  {
    return;
  }

  let peer = Peer::new(recon, stream, piece_manager_sender, buf);
}

async fn dial(
  peer_address: &str,
  torrent_handshake: Bytes,
) -> Option<(TcpStream, BytesMut, Bytes)> {
  let mut stream = TcpStream::connect(peer_address).await.ok()?;

  println!("Connected to {peer_address}");

  stream.write_all(&torrent_handshake).await.ok()?;

  let mut buf = BytesMut::new();

  while buf.len() < 68 {
    let mut temp = [0u8; 4096];
    let n = match stream.read(&mut temp).await {
      Ok(0) | Err(_) => return None, // closed or failed
      Ok(n) => n,
    };
    buf.extend_from_slice(&temp[..n]);
  }

  let received_handshake = buf.split_to(68).freeze();

  Some((stream, buf, received_handshake))
}
