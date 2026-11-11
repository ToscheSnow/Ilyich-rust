use crate::{peer_protocol::PeerEvent, storage_manager::BlockMeta};
use bytes::Bytes;

pub fn parse_peer_event(message: Bytes) -> Result<PeerEvent, String> {
  if message.is_empty() {
    return Err("Empty message received".to_string());
  }

  let message_id = message[0];
  let event = match message_id {
    0 => PeerEvent::CHOKE,
    1 => PeerEvent::UNCHOKE,
    2 => PeerEvent::INTERESTED,
    3 => PeerEvent::NotInterested,

    4 => {
      if message.len() != 5 {
        return Err(format!("Invalid HAVE message length: {}", message.len()));
      }
      let piece_idx = u32::from_be_bytes(message[1..5].try_into().unwrap());
      PeerEvent::HAVE(piece_idx)
    }

    5 => PeerEvent::BITFIELD(Bytes::copy_from_slice(&message[1..])),

    6 => {
      if message.len() != 13 {
        return Err(format!("Invalid REQUEST message length: {}", message.len()));
      }
      PeerEvent::REQUEST(BlockMeta {
        piece_idx: u32::from_be_bytes(message[1..5].try_into().unwrap()),
        offset: u32::from_be_bytes(message[5..9].try_into().unwrap()),
        length: u32::from_be_bytes(message[9..13].try_into().unwrap()),
      })
    }

    7 => {
      if message.len() < 9 {
        return Err(format!("PIECE message length too short: {}", message.len()));
      }
      PeerEvent::PIECE {
        piece_idx: u32::from_be_bytes(message[1..5].try_into().unwrap()),
        offset: u32::from_be_bytes(message[5..9].try_into().unwrap()),
        block: Bytes::copy_from_slice(&message[9..]),
      }
    }

    8 => {
      if message.len() != 13 {
        return Err(format!("Invalid CANCEL message length: {}", message.len()));
      }
      PeerEvent::CANCEL(BlockMeta {
        piece_idx: u32::from_be_bytes(message[1..5].try_into().unwrap()),
        offset: u32::from_be_bytes(message[5..9].try_into().unwrap()),
        length: u32::from_be_bytes(message[9..13].try_into().unwrap()),
      })
    }

    9 => {
      if message.len() != 3 {
        return Err(format!("Invalid PORT message length: {}", message.len()));
      }
      PeerEvent::PORT(u16::from_be_bytes(message[1..3].try_into().unwrap()))
    }

    20 => {
      if message.len() < 2 {
        return Err(format!(
          "Invalid EXTENDED message length: {}",
          message.len()
        ));
      }
      PeerEvent::EXTENDED(message[1])
    }

    _ => {
      return Err(format!("Invalid peer message ID: {}", message_id));
    }
  };

  Ok(event)
}
