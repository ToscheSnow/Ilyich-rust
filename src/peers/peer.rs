use std::{
  collections::HashMap,
  net::SocketAddr,
  sync::{Arc, RwLock},
  time::Instant,
};

use tokio::{
  net::tcp::{OwnedReadHalf, OwnedWriteHalf},
  select,
  sync::mpsc::{self, Sender},
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{
  event::event::TorrentEvent::{self, Unchoke},
  peer_protocol::{BlockRequest, PeerEvent, PendingReq},
  peers::parse_peer_event::parse_peer_event,
  pieces::{
    bitfield::{add_piece, count_pieces, has_piece},
    piece_manager::{IncomingRequest, Piece, PieceManagerMessage},
  },
  recon::Recon,
};

pub enum PeerCommand {
  Request(BlockRequest),
  Have(u32),
  SendBlock(BlockRequest, Bytes),
  SendBitfield(Bytes),
  Shutdown,
}

pub struct PeerState {
  // i can not send
  am_choking: bool,
  // i can not receive
  peer_choking: bool,
  // they want something
  peer_interested: bool,
}

impl Default for PeerState {
  fn default() -> Self {
    // based on BEP 3 specification
    PeerState {
      am_choking: true,
      peer_choking: true,
      peer_interested: false,
    }
  }
}

pub struct Peer {
  total_pieces: u32,

  recon: Recon<TorrentEvent>,

  state: PeerState,
  pending_requests: HashMap<(u32, u32), PendingReq>,

  available_pieces: Arc<RwLock<Vec<u8>>>,
  available_piece_count: u32,

  from_socket: mpsc::Receiver<Bytes>,
  to_socket: mpsc::Sender<Bytes>,

  cmd_receiver: mpsc::Receiver<PeerCommand>,
  cmd_sender: mpsc::Sender<PeerCommand>,

  peer_handle: PeerHandle,
  piece_manager_sender: tokio::sync::mpsc::Sender<PieceManagerMessage>,
}

use std::hash::{Hash, Hasher};

#[derive(Clone)]
pub struct PeerHandle {
  pub cmd_sender: mpsc::Sender<PeerCommand>,
  pub addr: SocketAddr,
  pub available_pieces: Arc<RwLock<Vec<u8>>>,
}

impl PartialEq for PeerHandle {
  fn eq(&self, other: &Self) -> bool {
    self.addr == other.addr
  }
}
impl Eq for PeerHandle {}

impl Hash for PeerHandle {
  fn hash<H: Hasher>(&self, state: &mut H) {
    self.addr.hash(state);
  }
}

pub struct PeerDeps {
  pub total_pieces: u32,
  pub addr: SocketAddr,

  pub recon: Recon<TorrentEvent>,
  pub socket_reader: OwnedReadHalf,
  pub socket_writer: OwnedWriteHalf,

  pub piece_manager_sender: Sender<PieceManagerMessage>,
}

impl Peer {
  // pub fn start() {}

  pub fn new(
    PeerDeps {
      total_pieces,
      recon,
      socket_reader,
      socket_writer,
      piece_manager_sender,
      addr,
    }: PeerDeps,
  ) -> Self {
    let (from_socket_tx, from_socket) = mpsc::channel::<Bytes>(1000); // A: reader -> peer
    let (to_socket, to_socket_rx) = mpsc::channel::<Bytes>(1000); // B: peer -> writer

    let (cmd_sender, cmd_receiver) = mpsc::channel::<PeerCommand>(1000);

    let available_pieces = Arc::new(RwLock::new(Vec::new()));

    let peer_handle = PeerHandle {
      addr,
      available_pieces: Arc::clone(&available_pieces),
      cmd_sender: cmd_sender.clone(),
    };

    let peer: Peer = Peer {
      total_pieces,
      recon,
      state: PeerState::default(),
      pending_requests: HashMap::new(),
      available_pieces: Arc::clone(&available_pieces),
      available_piece_count: 0,
      from_socket,
      to_socket,
      cmd_receiver,
      cmd_sender,
      peer_handle,
      piece_manager_sender,
    };

    Self::start_incoming_reader(socket_reader, from_socket_tx);
    Self::start_outgoing_writer(socket_writer, to_socket_rx);

    peer
  }

  pub fn run(mut self) {
    tokio::spawn(async move {
      loop {
        select! {
          cmd = self.cmd_receiver.recv() => match cmd {
            Some(PeerCommand::Shutdown) | None => break,
            Some(cmd) => self.process_cmd(cmd).await,
          },

          msg = self.from_socket.recv() => match msg {
            Some(raw) => match parse_peer_event(raw) {
              Ok(event) => self.process_event(event).await,
              Err(err) => {},
            },
            None => break, // socket reader died 💀
          },
        }
      }
      self.disconnect();
    });
  }

  fn start_outgoing_writer(mut writer: OwnedWriteHalf, mut to_socket_rx: mpsc::Receiver<Bytes>) {
    tokio::spawn(async move {
      loop {
        match to_socket_rx.recv().await {
          Some(bytes) => {
            if writer.write_all(&bytes).await.is_err() {
              break;
            }
          }
          None => break,
        }
      }
    });
  }

  fn start_incoming_reader(mut reader: OwnedReadHalf, from_socket_tx: mpsc::Sender<Bytes>) {
    tokio::spawn(async move {
      let mut buf = BytesMut::with_capacity(128 * 1024);
      loop {
        match reader.read_buf(&mut buf).await {
          Ok(0) | Err(_) => {
            break;
          }

          Ok(_) => {
            while let Some(message) = Self::read_message(&mut buf) {
              if from_socket_tx.send(message).await.is_err() {
                return;
              };
            }
          }
        }
      }
    });
  }

  async fn process_cmd(&mut self, cmd: PeerCommand) {
    match cmd {
      PeerCommand::Request(block_req) => {
        self.request(block_req).await;
      }

      PeerCommand::Have(piece_idx) => {
        self.send_have_req(piece_idx).await;
      }

      PeerCommand::SendBlock(requested_block, bytes) => {
        self.send_piece(requested_block, &bytes).await;
      }

      PeerCommand::SendBitfield(bitfield) => {
        self.send_bitfield_buf(&bitfield).await;
      }

      PeerCommand::Shutdown => {
        self.disconnect();
      }
    }
  }

  fn disconnect(&mut self) {
    self.pending_requests.clear();

    self
      .recon
      .announce(TorrentEvent::Disconnect(self.peer_handle.clone()));
  }

  fn read_message(buf: &mut BytesMut) -> Option<Bytes> {
    if buf.len() < 4 {
      return None;
    }

    let length = u32::from_be_bytes(buf[..4].try_into().unwrap()) as usize;

    if buf.len() < 4 + length {
      return None;
    }
    buf.advance(4);
    let message = buf.split_to(length).freeze();
    Some(message)
  }

  async fn process_event(&mut self, message: PeerEvent) {
    match message {
      PeerEvent::CHOKE => {
        self.state.peer_choking = true;
        self.pending_requests.clear();

        // tell recon
        self
          .recon
          .announce(TorrentEvent::Choke(self.peer_handle.clone()));
      }

      //done
      PeerEvent::UNCHOKE => {
        self.state.peer_choking = false;
        self.recon.announce(Unchoke(self.peer_handle.clone()));
        //tell scheduler , stats, peerManager
      }

      // done
      PeerEvent::INTERESTED => {
        println!("🧩 Someone is interested");

        self.state.peer_interested = true;
        self.state.am_choking = false;

        self.send_unchoke_req().await;
      }

      // done
      PeerEvent::NotInterested => {
        self.state.peer_interested = false;
        // do nothing
      }

      // done
      PeerEvent::HAVE { piece_idx } => {
        {
          let mut bitfield = self.available_pieces.write().unwrap();
          if !has_piece(&bitfield, piece_idx as usize) {
            self.available_piece_count += 1;
            add_piece(&mut bitfield, piece_idx as usize);
          };
        }

        if self.available_piece_count as u32 == self.total_pieces {
          // tell seeder is there
          self
            .recon
            .announce(TorrentEvent::SeederConnect(self.peer_handle.clone()));
        }
      }

      //done
      PeerEvent::REQUEST(BlockRequest {
        piece_idx,
        offset,
        length,
      }) => {
        if self.state.am_choking {
          return;
        }
        // tell using upwards command instead of event bus
        let _ = self
          .piece_manager_sender
          .send(PieceManagerMessage::IncomingRequest(IncomingRequest {
            incoming_request: BlockRequest {
              piece_idx,
              offset,
              length,
            },
            outgoing_sender: self.cmd_sender.clone(),
          }))
          .await;
      }

      //done
      PeerEvent::PIECE {
        piece_idx,
        offset,
        block,
      } => {
        let piece_key = &(piece_idx, offset);

        if let Some(pending_req) = self.pending_requests.get(piece_key) {
          // eprintln!("Received unexpected piece");
          // return;
          if pending_req.length != block.len() as u32 {
            println!("Received block with incorrect length");
            return;
          }
        };
        self.pending_requests.remove(piece_key);

        self
          .piece_manager_sender
          .send(PieceManagerMessage::IncomingBlock(Piece {
            piece_idx,
            offset,
            block,
          }))
          .await;
      }

      //done
      PeerEvent::BITFIELD { bitfield } => {
        {
          let mut curr_bitfield = self.available_pieces.write().unwrap();
          *curr_bitfield = bitfield.into();
          self.available_piece_count = count_pieces(&curr_bitfield) as u32;
        }

        self.recon.announce(TorrentEvent::Bitfield);
        //Send interested Req;
        self.send_interested_req().await;

        // announce to the subs
        if self.available_piece_count as u32 == self.total_pieces {
          self
            .recon
            .announce(TorrentEvent::SeederConnect(self.peer_handle.clone()));
        }
      }

      //will not be used
      PeerEvent::CANCEL(_) => {
        // not implemented and not important
        return;
      }

      //done for now
      PeerEvent::PORT(dht_port) => {
        // println!("Found a dht port :{dht_port}");
        return;
      }

      //done for now
      PeerEvent::EXTENDED(extended_message_id) => {
        // println!("Received a extendedMessageId: {extended_message_id}");
        return;
      }
    }
  }

  async fn request(&mut self, req: BlockRequest) {
    let BlockRequest {
      piece_idx,
      offset,
      length,
    } = req;

    let mut buf = BytesMut::with_capacity(17);
    buf.put_u32(13);
    buf.put_u8(6);
    buf.put_u32(piece_idx);
    buf.put_u32(offset);
    buf.put_u32(length);

    self.pending_requests.insert(
      (piece_idx, offset),
      PendingReq {
        piece_idx,
        offset,
        length,
        sent_at: Instant::now(),
      },
    );

    self.to_socket.send(buf.freeze()).await.is_err();
    // mpsc channel push
  }

  async fn send_interested_req(&mut self) {
    // let buf: [u8; 5] = [0x00, 0x00, 0x00, 0x01, 0x02];
    let mut buf = BytesMut::with_capacity(5);

    buf.put_u32(1);
    buf.put_u8(2);

    // mpsc outgoing push
    self.to_socket.send(buf.freeze()).await.unwrap();
  }

  async fn send_bitfield_buf(&mut self, bitfield: &[u8]) {
    let mut buf = BytesMut::with_capacity(bitfield.len() + 1 + 4);

    buf.put_u32((bitfield.len() + 1) as u32);
    buf.put_u8(5);
    buf.extend_from_slice(bitfield);

    self.to_socket.send(buf.freeze()).await.unwrap();
    println!("We sent our bitfield");
  }

  async fn send_have_req(&mut self, piece_idx: u32) {
    let mut have_buf = BytesMut::with_capacity(9);

    have_buf.put_u32(5);
    have_buf.put_u8(4);
    have_buf.put_u32(piece_idx);

    //req queue channel push
    self.to_socket.send(have_buf.freeze()).await.unwrap();
  }

  async fn send_piece(&mut self, req: BlockRequest, block_buf: &[u8]) {
    let BlockRequest {
      piece_idx, offset, ..
    } = req;
    let mut buf = BytesMut::with_capacity(4 + 1 + 4 + 4 + block_buf.len());

    buf.put_u32((1 + 4 + 4 + block_buf.len()) as u32);

    buf.put_u8(7);

    buf.put_u32(piece_idx);
    buf.put_u32(offset);

    buf.extend_from_slice(block_buf);

    //req_queue_tx /channel push/send
    self.to_socket.send(buf.freeze()).await.unwrap();
  }

  pub fn in_flight() {
    // pending_requests.size
  }

  async fn send_unchoke_req(&mut self) {
    println!("🏖️ UNCHOKE PEER");
    let mut unchoke_buf = BytesMut::with_capacity(5);

    // length afterward is 1
    unchoke_buf.put_u32(1);

    //Unchoke message id is 1
    unchoke_buf.put_u8(1);

    // push into req_queue_tx for the socket
    self.to_socket.send(unchoke_buf.freeze()).await.unwrap();
  }

  pub fn get_peer_handle(&self) -> PeerHandle {
    self.peer_handle.clone()
  }
}
