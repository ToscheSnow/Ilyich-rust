#[allow(non_snake_case)]
use std::{
  collections::{HashMap, HashSet},
  time::Instant,
};

use tokio::sync::mpsc;

use crate::protocol::{
  BlockRequest, ManagerMessage,
  PeerMessage::{self},
  PendingReq,
};
use bytes::{BufMut, Bytes, BytesMut};

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
  host: String,
  port: i16,

  state: PeerState,
  piece_manager_channel: mpsc::Sender<ManagerMessage>,
  pending_requests: HashMap<(u32, u32), PendingReq>,

  available_pieces: HashSet<u32>,

  req_queue: mpsc::Sender<Bytes>,
  incoming_quque: mpsc::Receiver<Bytes>,
}

impl Peer {
  pub async fn process_event(&mut self, message: PeerMessage) {
    match message {
      PeerMessage::CHOKE => {
        self.state.peer_choking = true;

        //tell scheduler , stats, peerManager
      }

      PeerMessage::UNCHOKE => {
        self.state.peer_choking = false;

        //tell scheduler , stats, peerManager
      }

      PeerMessage::INTERESTED => {
        self.state.peer_interested = true;
        self.state.am_choking = false;

        // tell scheduler
      }
      PeerMessage::NOT_INTERESTED => {
        self.state.peer_interested = false;

        // do nothing
      }

      PeerMessage::HAVE { piece_id } => {
        self.available_pieces.insert(piece_id);

        //send a UNCHOKE_Req
      }

      PeerMessage::REQUEST(BlockRequest {
        piece_id,
        offset,
        length,
      }) => {
        if self.state.am_choking {
          return;
        }

        //announce to the event bus😨
      }

      PeerMessage::PIECE {
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
          .piece_manager_channel
          .send(ManagerMessage::PieceReceived {
            piece_id,
            offset,
            data,
          })
          .await
          .unwrap();
      }

      PeerMessage::BITFIELD { bitfield } => {
        // addPiecesFromBitfield(bitfield,self.available_pieces);

        //Send interested Req;

        // announce to the subs
        return;
      }

      PeerMessage::CANCEL(BlockRequest) => {
        // not implemented and not important
        return;
      }

      PeerMessage::PORT => {
        return;
      }

      PeerMessage::EXTENDED => {
        println!("Ignore EXTENDED request 😂");

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

    self.req_queue.send(buf.freeze()).await.unwrap();
    // mpsc channel push
  }

  pub async fn cancel_req(&mut self, req: BlockRequest) {
    let BlockRequest {
      piece_id,
      offset,
      length,
    } = req;

    // if not valid request throw
    let mut buf = BytesMut::with_capacity(17);

    // 13 bytes always in hex for cancelling a request
    buf.put_u32(13);

    // message id 8 for cancelling a request
    buf.put_u8(8);
    buf.put_u32(piece_id);
    buf.put_u32(offset);
    buf.put_u32(length);

    self.pending_requests.remove(&(piece_id, offset)).unwrap();

    self.req_queue.send(buf.freeze()).await.unwrap();
    // mspc socket channel push
  }

  pub async fn interested_req(&mut self) {
    // let buf: [u8; 5] = [0x00, 0x00, 0x00, 0x01, 0x02];
    let mut buf = BytesMut::with_capacity(5);

    buf.put_u32(1);
    buf.put_u8(2);

    self.state.am_interested = true;
    // mpsc outoging push

    self.req_queue.send(buf.freeze()).await.unwrap();
  }

  fn send_bitfield_req(bitfield: &[u8]) -> BytesMut {
    let mut buf = BytesMut::with_capacity(bitfield.len() + 1 + 4);

    buf.put_u32((bitfield.len() + 1) as u32);
    buf.put_u8(5);
    buf.extend_from_slice(bitfield);

    return buf;
  }

  pub async fn HAVE_Req(&mut self, piece_id: u32) {
    let mut have_buf = BytesMut::with_capacity(9);

    have_buf.put_u32(5);
    have_buf.put_u8(4);
    have_buf.put_u32(piece_id);

    //req queue channel push
    self.req_queue.send(have_buf.freeze()).await.unwrap();
  }

  pub async fn SEND_PIECE(&mut self, req: BlockRequest, blockBuf: &[u8]) {
    let BlockRequest {
      piece_id, offset, ..
    } = req;
    let mut buf = BytesMut::with_capacity(4 + 1 + 4 + 4 + blockBuf.len());

    buf.put_u32((1 + 4 + 4 + blockBuf.len()) as u32);

    buf.put_u8(7);

    buf.put_u32(piece_id);
    buf.put_u32(offset);

    buf.extend_from_slice(blockBuf);

    //req_queue /channel push/send
    self.req_queue.send(buf.freeze()).await.unwrap();
  }

  pub fn inFlight() {
    // pending_requests.size
  }

  async fn UNCHOKE_Req(&mut self) {
    println!("🏖️ UNCHOKE PEER");

    let mut unchoke_buf = BytesMut::with_capacity(5);

    // length aftwerward is 1
    unchoke_buf.put_u32(1);

    //Unchoke message id is 1
    unchoke_buf.put_u8(1);

    // push into req_queue for the socket
    self.req_queue.send(unchoke_buf.freeze()).await.unwrap();
  }
}
