use std::collections::{HashMap, HashSet};

use crate::protocol::{BlockRequest, ManagerMessage, PeerMessage, PendingReq};

pub struct PeerState {
  peerChoking: bool,
  peerInterested: bool,
  amChoking: bool,
  amInterested: bool,
}

impl Default for PeerState {
  fn default() -> Self {
    PeerState {
      peerChoking: true,
      peerInterested: false,
      amChoking: true,
      amInterested: false,
    }
  }
}

pub struct Peer {
  host: String,
  port: i16,

  state: PeerState,
  pieceManagerChannel: tokio::sync::mpsc::Sender<ManagerMessage>,
  pendingRequests: HashMap<(usize, usize), PendingReq>,

  availablePieces: HashSet<usize>,
}

impl Peer {
  pub async fn processEvent(&mut self, message: PeerMessage) {
    match message {
      PeerMessage::CHOKE => {
        self.state.peerChoking = true;

        //tell scheduler , stats, peerManager
      }

      PeerMessage::UNCHOKE => {
        self.state.peerChoking = false;

        //tell scheduler , stats, peerManager
      }

      PeerMessage::INTERESTED => {
        self.state.peerInterested = true;
        self.state.amChoking = false;

        // tell scheduler
      }
      PeerMessage::NOT_INTERESTED => {
        self.state.peerInterested = false;

        // do nothing
      }

      PeerMessage::HAVE { pieceId } => {
        self.availablePieces.insert(pieceId);

        //send a UNCHOKE_Req
      }

      PeerMessage::REQUEST(BlockRequest {
        pieceIdx,
        offset,
        length,
      }) => {
        if self.state.amChoking {
          return;
        }

        //announce to the event bus😨
      }

      PeerMessage::PIECE {
        pieceIdx,
        offset,
        data,
      } => {
        let pieceKey = &(pieceIdx, offset);

        let Some(pendingReq) = self.pendingRequests.get(pieceKey) else {
          eprintln!("Received unexpected piece");
          return;
        };

        if pendingReq.length != data.len() {
          println!("Received block with incorrect length");
          return;
        }

        self.pendingRequests.remove(pieceKey);

        self
          .pieceManagerChannel
          .send(ManagerMessage::PieceReceived {
            pieceIdx,
            offset,
            data,
          })
          .await
          .unwrap();
      }

      PeerMessage::BITFIELD { bitfield } => {
        // addPiecesFromBitfield(bitfield,self.availablePieces);

        //Send interested Req;

        // announce to the subs
        return;
      }

      PeerMessage::CANCEL(BlockRequest {
        pieceIdx,
        offset,
        length,
      }) => {
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
}
