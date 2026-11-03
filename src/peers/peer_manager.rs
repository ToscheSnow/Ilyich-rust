use std::{
  collections::HashSet,
  net::SocketAddr,
  sync::{Arc, RwLock},
  time::Duration,
};

use bytes::Bytes;
use tokio::{
  io::AsyncWriteExt,
  net::{
    TcpStream,
    tcp::{OwnedReadHalf, OwnedWriteHalf},
  },
  select,
  sync::mpsc::{Sender, UnboundedReceiver},
  task::JoinHandle,
  time::timeout,
};

use tokio::io::AsyncReadExt;

use tokio::sync::mpsc;

use crate::{
  event::event::TorrentEvent,
  peers::{
    client::CLIENT_ID_BYTES,
    handshake_buf::handshake_buf,
    peer::{Peer, PeerDeps, PeerHandle},
    verify_handshake::{HandshakeRes, verify_handshake},
  },
  pieces::piece_manager::PieceManagerMessage,
  recon::Recon,
  torrent::Torrent,
};

#[derive(Clone)]
pub struct PeerRequirements {
  piece_manager_sender: Sender<PieceManagerMessage>,
  recon: Recon<TorrentEvent>,
  total_pieces: u32,
}

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

  piece_manager_sender: Sender<PieceManagerMessage>,
  total_pieces: u32,
}

impl PeerManager {
  fn connect_peers(&mut self, peers: Vec<SocketAddr>) {
    let peer_reqs = PeerRequirements {
      piece_manager_sender: self.piece_manager_sender.clone(),
      recon: self.recon.clone(),
      total_pieces: self.total_pieces,
    };

    for peer_address in peers {
      if !self.connecting_peers.contains(&peer_address) {
        self.connecting_peers.insert(peer_address);
        Self::connect_peer(
          peer_address,
          self.info_hash.clone(),
          self.message_sender.clone(),
          peer_reqs.clone(),
        );
      }
    }
  }

  fn connect_peer(
    peer_address: SocketAddr,
    info_hash: Bytes,
    reply_sender: tokio::sync::mpsc::Sender<PeerManagerMessage>,
    PeerRequirements {
      piece_manager_sender,
      recon,
      total_pieces,
    }: PeerRequirements,
  ) -> JoinHandle<()> {
    tokio::spawn(async move {
      let handshake_buf = handshake_buf(info_hash.into(), CLIENT_ID_BYTES);

      let Ok(stream) = Self::connect_addr(peer_address).await else {
        return;
      };

      let (mut reader, mut writer) = stream.into_split();

      let Ok(handshake_res) = Self::do_handshake(&mut writer, &mut reader, handshake_buf).await
      else {
        return;
      };

      if !handshake_res.is_successful {
        reply_sender
          .send(PeerManagerMessage::ConnectionFailed(peer_address))
          .await;
        return;
      }

      let deps: PeerDeps = PeerDeps {
        addr: peer_address,
        socket_reader: reader,
        socket_writer: writer,
        piece_manager_sender,
        recon,
        total_pieces,
      };
      //make a new peer here

      let peer: Peer = Peer::new(deps);
      // send a message to peer manager to add it
      reply_sender
        .send(PeerManagerMessage::ConnectionSuccessful(
          peer_address,
          peer.get_peer_handle(),
        ))
        .await;

      peer.run();

      //start running the peer loop
    })
  }

  async fn do_handshake(
    writer: &mut OwnedWriteHalf,
    reader: &mut OwnedReadHalf,
    handshake_buf: Bytes,
  ) -> Result<HandshakeRes, String> {
    writer.write_all(&handshake_buf).await;

    match timeout(
      Duration::from_secs(5),
      Self::read_handshake(reader, handshake_buf),
    )
    .await
    {
      Ok(res) => {
        return Ok(res);
      }
      Err(_) => {
        return Err(format!("Peer handshake timed_out"));
      }
    }
  }

  async fn read_handshake(reader: &mut OwnedReadHalf, sent_bytes: Bytes) -> HandshakeRes {
    let mut received_handshake = [0u8; 68];

    match reader.read_exact(&mut received_handshake).await {
      Ok(_) => verify_handshake(sent_bytes, &received_handshake),
      Err(_) => HandshakeRes::default(),
    }
  }

  async fn connect_addr(addr: SocketAddr) -> Result<TcpStream, String> {
    match timeout(Duration::from_secs(10), TcpStream::connect(addr)).await {
      Ok(Ok(stream)) => {
        return Ok(stream);
      }
      Ok(Err(_)) => {
        return Err(format!("Couldnt connect to peer ❌"));
      }
      Err(_) => {
        return Err(format!("Connection timed out ⏰"));
      }
    }
  }

  fn start_listening(&mut self) {}

  pub fn run(mut self) {
    self.start_listening();
    Self::spawn(self);
  }

  pub fn new(
    active_peers: Arc<RwLock<HashSet<PeerHandle>>>,
    info_hash: Bytes,
    recon: Recon<TorrentEvent>,
    recon_listener: UnboundedReceiver<TorrentEvent>,
    piece_manager_sender: Sender<PieceManagerMessage>,
    total_pieces: u32,
  ) -> (Self, tokio::sync::mpsc::Sender<PeerManagerMessage>) {
    let (message_sender, message_receiver) = tokio::sync::mpsc::channel::<PeerManagerMessage>(6000);

    let manager: PeerManager = PeerManager {
      connecting_peers: HashSet::new(),
      active_peers,
      info_hash,
      message_receiver,
      message_sender: message_sender.clone(),
      recon_listener,
      recon,
      piece_manager_sender,
      total_pieces,
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
                TorrentEvent::Ban(_) => {}
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
                  let announced_peer = peer.clone();
                  self.connecting_peers.remove(&addr);
                  {
                    let mut lock = self.active_peers.write().unwrap();
                    lock.insert(peer);
                  }
                  self.recon.announce(TorrentEvent::Connect(announced_peer));
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
