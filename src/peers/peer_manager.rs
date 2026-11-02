use std::{
  collections::HashSet,
  net::SocketAddr,
  sync::{Arc, RwLock},
  time::Duration,
};

use bytes::{Bytes, BytesMut};
use tokio::{
  io::AsyncWriteExt,
  net::TcpStream,
  select,
  sync::mpsc::{Receiver, UnboundedReceiver},
  task::{JoinHandle, JoinSet},
  time::timeout,
};

use tokio::io::AsyncReadExt;

use tokio::sync::mpsc;

use crate::{
  event::event::TorrentEvent,
  peers::{
    client::CLIENT_ID_BYTES,
    handshake_buf::{self, handshake_buf},
    peer::{self, Peer, PeerHandle},
    peer_manager::{self, PeerManagerMessage::AttemptConnection},
  },
  recon::Recon,
};

pub enum PeerManagerMessage {
  AttemptConnection(Vec<SocketAddr>),
  ConnectionSuccessful(SocketAddr, PeerHandle),
  ConnectionFailed(SocketAddr),
}

pub struct PeerManager {
  connecting_peers: HashSet<SocketAddr>,
  active_peers: Arc<RwLock<HashSet<PeerHandle>>>,
  info_hash: Bytes,

  message_receiver: mpsc::Receiver<PeerManagerMessage>,
  message_sender: mpsc::Sender<PeerManagerMessage>,

  recon_listener: mpsc::UnboundedReceiver<TorrentEvent>,
  recon: Recon<TorrentEvent>,
}

impl PeerManager {
  fn connect_peers(&mut self, peers: Vec<SocketAddr>) {
    for peer_address in peers {
      if !self.connecting_peers.contains(&peer_address) {
        Self::connect_peer(
          peer_address,
          self.info_hash.clone(),
          self.message_sender.clone(),
        );
      }
    }
  }

  fn connect_peer(
    peer_address: SocketAddr,
    info_hash: Bytes,
    reply_sender: tokio::sync::mpsc::Sender<PeerManagerMessage>,
  ) -> JoinHandle<()> {
    tokio::spawn(async move {
      let handshake_buf = handshake_buf(info_hash.into(), CLIENT_ID_BYTES);
    })
  }

  fn start_listening(&mut self) {}

  fn run(mut self) {
    self.start_listening();
    Self::spawn(self);
  }

  fn new(
    active_peers: Arc<RwLock<HashSet<PeerHandle>>>,
    info_hash: Bytes,
    recon: Recon<TorrentEvent>,
    recon_listener: UnboundedReceiver<TorrentEvent>,
  ) -> (Self, tokio::sync::mpsc::Sender<PeerManagerMessage>) {
    let (message_sender, message_receiver) = tokio::sync::mpsc::channel::<PeerManagerMessage>(200);

    let manager: PeerManager = PeerManager {
      connecting_peers: HashSet::new(),
      active_peers,
      info_hash,
      message_receiver,
      message_sender: message_sender.clone(),
      recon_listener,
      recon,
    };

    return (manager, message_sender);
  }

  fn spawn(mut self) -> JoinHandle<()> {
    tokio::spawn(async move {
      loop {
        select! {
          res = self.recon_listener.recv() => {
            match res {
              Some(msg) => match msg {
                TorrentEvent::Ban(peer) => {}
                TorrentEvent::DownloadComplete => return,
                TorrentEvent::Disconnect(peer) => {
                  let mut lock = self.active_peers.write().unwrap();
                  lock.remove(&peer);
                }
                _ => {}
              },
              None => break,
            }
          },

          res= self.message_receiver.recv()=> {
            match res {
              Some(res) => match res {
                PeerManagerMessage::AttemptConnection(peers) => {
                  self.connect_peers(peers);
                }
                PeerManagerMessage::ConnectionFailed(addr) => {
                  self.connecting_peers.remove(&addr);
                }
                PeerManagerMessage::ConnectionSuccessful(addr, peer) => {
                  self.connecting_peers.remove(&addr);
                  {
                    let mut lock = self.active_peers.write().unwrap();
                    lock.insert(peer);
                  }
                }
              },
              None => break,
            }
          }
        }
      }
    })
  }
}
