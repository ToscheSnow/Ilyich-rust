use std::{
  collections::{HashMap, HashSet},
  time::Instant,
};

use tokio::{
  net::tcp::{OwnedReadHalf, OwnedWriteHalf},
  select,
  sync::{mpsc, oneshot},
};

use crate::protocol::PeerEvent;
use crate::{
  protocol::{BlockRequest, ManagerMessage, PendingReq},
  recon::Recon,
  torrent::TorrentEvent,
};
use bytes::{Buf, BufMut, Bytes, BytesMut, buf};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::peers::parse_peer_event::parse_peer_event;

pub enum PeerCommand {
  Request(BlockRequest),
  Have(u32),
  SendBlock(BlockRequest, Bytes),
  SendBitfield(Bytes),
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
  recon: Recon<TorrentEvent>,

  state: PeerState,
  piece_manager_sender: mpsc::Sender<ManagerMessage>,
  pending_requests: HashMap<(u32, u32), PendingReq>,

  available_pieces: HashSet<u32>,

  incoming_req: mpsc::Receiver<Bytes>,
  outgoing_req: mpsc::Sender<Bytes>,

  incoming_command: mpsc::Receiver<PeerCommand>,
}

pub struct PeerHandle {
  cmd_sender: mpsc::Sender<PeerCommand>,
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

        //tell scheduler , stats, peerManager
      }

      PeerEvent::UNCHOKE => {
        self.state.peer_choking = false;

        //tell scheduler , stats, peerManager
      }

      PeerEvent::INTERESTED => {
        self.state.peer_interested = true;
        self.state.am_choking = false;

        // tell scheduler
      }
      PeerEvent::NOT_INTERESTED => {
        self.state.peer_interested = false;

        // do nothing
      }

      PeerEvent::HAVE { piece_id } => {
        self.available_pieces.insert(piece_id);

        //send a UNCHOKE_Req
      }

      PeerEvent::REQUEST(BlockRequest {
        piece_id,
        offset,
        length,
      }) => {
        if self.state.am_choking {
          return;
        }

        //announce to the event bus😨
      }

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
        // add_pieces_from_bitfield(bitfield,self.available_pieces);

        //Send interested Req;

        // announce to the subs
        return;
      }

      PeerEvent::CANCEL(block_request) => {
        // not implemented and not important
        return;
      }

      PeerEvent::PORT(dhtPort) => {
        println!("Found a dht port :{dhtPort}");
        return;
      }

      PeerEvent::EXTENDED(extendedMessageId) => {
        println!("Received a extendedMessageId: {extendedMessageId}");
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
