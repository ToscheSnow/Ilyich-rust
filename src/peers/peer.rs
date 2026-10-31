use std::{
  collections::{HashMap, HashSet},
  os::unix::net::SocketAddr,
  time::Instant,
};

use tokio::{
  net::tcp::{OwnedReadHalf, OwnedWriteHalf},
  select,
  sync::mpsc,
};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::{
  event::TorrentEvent::{self},
  peer_protocol::{BlockRequest, ManagerMessage, PeerEvent, PendingReq},
  peers::{bitfield::add_pieces_from_bitfield, parse_peer_event::parse_peer_event},
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
  peer_choking: bool,
  peer_interested: bool,
  am_choking: bool,
  am_interested: bool,
}

impl Default for PeerState {
  fn default() -> Self {
    PeerState {
      peer_choking: true,
      peer_interested: false,
      am_choking: true,
      am_interested: false,
    }
  }
}

pub struct Peer {
  total_pieces: u32,

  recon: Recon<TorrentEvent>,

  state: PeerState,
  piece_manager_sender: mpsc::Sender<ManagerMessage>,
  pending_requests: HashMap<(u32, u32), PendingReq>,

  available_pieces: HashSet<u32>,

  incoming_req: mpsc::Receiver<Bytes>,
  outgoing_req: mpsc::Sender<Bytes>,

  incoming_command: mpsc::Receiver<PeerCommand>,

  peer_handle: PeerHandle,
}

#[derive(Clone)]
pub struct PeerHandle {
  pub cmd_sender: mpsc::Sender<PeerCommand>,
  pub addr: SocketAddr,
  pub remote_peer_id: [u8; 20],
}

impl Peer {
  pub fn start() {}

  // pub fn new(
  //   recon: Recon<TorrentEvent>,
  //   stream: TcpStream,
  //   piece_manager_sender: mpsc::Sender<ManagerMessage>,
  //   leftover: BytesMut,
  // ) -> Peer {
  //   let (socket_reader, socket_writer) = stream.into_split();

  //   let (req_queue_tx, req_queue_rx) = mpsc::channel(256);
  //   let (incoming_tx, incoming_rx) = mpsc::channel(256);

  //   let peer = Peer {
  //     recon,
  //     state: PeerState::default(),
  //     piece_manager_sender,
  //     pending_requests: HashMap::new(),
  //     available_pieces: HashSet::new(),

  //     socket_reader,
  //     socket_writer,
  //     buf: leftover,
  //   };

  //   peer
  // }

  async fn run(mut self) {
    loop {
      select! {
        Some(cmd) = self.incoming_command.recv() => {
          self.process_cmd(cmd).await;
        },

       Some(msg) = self.incoming_req.recv() => {
          match parse_peer_event(msg) {
            Ok(event) => {
              self.process_event(event).await;
            },
            Err(err) => {
              println!("{err}");
            }
          }
        },
        else => break,
      }
    }
  }

  async fn start_incoming_reader(mut reader: OwnedReadHalf, to_incoming: mpsc::Sender<Bytes>) {
    let mut buf = BytesMut::with_capacity(64 * 1024);
    loop {
      match reader.read_buf(&mut buf).await {
        Ok(0) | Err(_) => {
          break;
        }

        Ok(_) => {
          while let Some(message) = Self::read_message(&mut buf) {
            if to_incoming.send(message).await.is_err() {
              return;
            };
          }
        }
      }
    }
  }

  fn disconnect(&mut self) {}

  async fn process_cmd(&mut self, cmd: PeerCommand) {
    match cmd {
      PeerCommand::Request(block_req) => {
        self.request(block_req).await;
      }

      PeerCommand::Have(piece_idx) => {
        self.have_req(piece_idx).await;
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

  async fn start_outgoing_writer(
    mut writer: OwnedWriteHalf,
    mut to_outgoing: mpsc::Receiver<Bytes>,
  ) {
    loop {
      match to_outgoing.recv().await {
        Some(bytes) => {
          if writer.write_all(&bytes).await.is_err() {
            break;
          }
        }
        None => break,
      }
    }
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

  pub async fn process_event(&mut self, message: PeerEvent) {
    match message {
      PeerEvent::CHOKE => {
        self.state.peer_choking = true;
        self.pending_requests.clear();

        // tell recon
        self
          .recon
          .announce(TorrentEvent::Choke(self.peer_handle.clone()));

        //tell scheduler , stats, peerManager
      }

      PeerEvent::UNCHOKE => {
        self.state.peer_choking = false;

        //tell scheduler , stats, peerManager
      }

      // done
      PeerEvent::INTERESTED => {
        println!("🧩 Someone is interested");

        self.state.peer_interested = true;
        self.state.am_choking = false;

        self.unchoke_req().await;
      }

      // done
      PeerEvent::NotInterested => {
        self.state.peer_interested = false;
        // do nothing
      }

      // done
      PeerEvent::HAVE { piece_id } => {
        self.available_pieces.insert(piece_id);

        if self.available_pieces.len() as u32 == self.total_pieces {
          // tell seeder is there
          self
            .recon
            .announce(TorrentEvent::SeederConnect(self.peer_handle.clone()));
        }
      }

      PeerEvent::REQUEST(BlockRequest {
        piece_id,
        offset,
        length,
      }) => {
        if self.state.am_choking {
          return;
        }
        // tell using upwards command instead of event bus

        //announce to the event bus😨
      }

      //done
      PeerEvent::PIECE {
        piece_id,
        offset,
        data,
      } => {
        let piece_key = &(piece_id, offset);

        let Some(pending_req) = self.pending_requests.get(piece_key) else {
          eprintln!("Received unexpected piece");
          return;
        };

        if pending_req.length != data.len() as u32 {
          println!("Received block with incorrect length");
          return;
        }

        self.pending_requests.remove(piece_key);

        self
          .piece_manager_sender
          .send(ManagerMessage::PieceReceived {
            piece_id,
            offset,
            data,
          })
          .await
          .unwrap();
      }

      PeerEvent::BITFIELD { bitfield } => {
        add_pieces_from_bitfield(bitfield, &mut self.available_pieces);

        self.recon.announce(TorrentEvent::Bitfield);
        //Send interested Req;
        self.interested_req().await;

        // announce to the subs
        if self.available_pieces.len() as u32 == self.total_pieces {
          self
            .recon
            .announce(TorrentEvent::SeederConnect(self.peer_handle.clone()));
        }
        return;
      }

      PeerEvent::CANCEL(_) => {
        // not implemented and not important
        return;
      }

      PeerEvent::PORT(dht_port) => {
        println!("Found a dht port :{dht_port}");
        return;
      }

      PeerEvent::EXTENDED(extended_message_id) => {
        println!("Received a extendedMessageId: {extended_message_id}");
        return;
      }
    }
  }

  pub async fn request(&mut self, req: BlockRequest) {
    let BlockRequest {
      piece_id,
      offset,
      length,
    } = req;

    let mut buf = BytesMut::with_capacity(17);
    buf.put_u32(13);
    buf.put_u8(6);
    buf.put_u32(piece_id);
    buf.put_u32(offset);
    buf.put_u32(length);

    self.pending_requests.insert(
      (piece_id, offset),
      PendingReq {
        piece_id,
        offset,
        length,
        sent_at: Instant::now(),
      },
    );

    self.outgoing_req.send(buf.freeze()).await.unwrap();
    // mpsc channel push
  }

  pub async fn interested_req(&mut self) {
    // let buf: [u8; 5] = [0x00, 0x00, 0x00, 0x01, 0x02];
    let mut buf = BytesMut::with_capacity(5);

    buf.put_u32(1);
    buf.put_u8(2);

    self.state.am_interested = true;
    // mpsc outgoing push

    self.outgoing_req.send(buf.freeze()).await.unwrap();
  }

  async fn send_bitfield_buf(&mut self, bitfield: &[u8]) {
    let mut buf = BytesMut::with_capacity(bitfield.len() + 1 + 4);

    buf.put_u32((bitfield.len() + 1) as u32);
    buf.put_u8(5);
    buf.extend_from_slice(bitfield);

    self.outgoing_req.send(buf.freeze()).await.unwrap();
  }

  pub async fn have_req(&mut self, piece_id: u32) {
    let mut have_buf = BytesMut::with_capacity(9);

    have_buf.put_u32(5);
    have_buf.put_u8(4);
    have_buf.put_u32(piece_id);

    //req queue channel push
    self.outgoing_req.send(have_buf.freeze()).await.unwrap();
  }

  pub async fn send_piece(&mut self, req: BlockRequest, block_buf: &[u8]) {
    let BlockRequest {
      piece_id, offset, ..
    } = req;
    let mut buf = BytesMut::with_capacity(4 + 1 + 4 + 4 + block_buf.len());

    buf.put_u32((1 + 4 + 4 + block_buf.len()) as u32);

    buf.put_u8(7);

    buf.put_u32(piece_id);
    buf.put_u32(offset);

    buf.extend_from_slice(block_buf);

    //req_queue_tx /channel push/send
    self.outgoing_req.send(buf.freeze()).await.unwrap();
  }

  pub fn in_flight() {
    // pending_requests.size
  }

  async fn unchoke_req(&mut self) {
    println!("🏖️ UNCHOKE PEER");
    let mut unchoke_buf = BytesMut::with_capacity(5);

    // length afterward is 1
    unchoke_buf.put_u32(1);

    //Unchoke message id is 1
    unchoke_buf.put_u8(1);

    // push into req_queue_tx for the socket
    self.outgoing_req.send(unchoke_buf.freeze()).await.unwrap();
  }
}
