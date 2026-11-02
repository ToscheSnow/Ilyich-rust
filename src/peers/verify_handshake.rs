use bytes::Bytes;

pub struct HandshakeRes {
  pub is_successful: bool,
  pub remote_peer_id: Option<Bytes>,
  pub supports_extensions: bool,
  pub supports_dht: bool,
}

impl Default for HandshakeRes {
  fn default() -> Self {
    HandshakeRes {
      is_successful: false,
      remote_peer_id: None,
      supports_extensions: false,
      supports_dht: false,
    }
  }
}

pub fn verify_handshake(sent: Bytes, received: &[u8]) -> HandshakeRes {
  if received.len() < 68 {
    return HandshakeRes {
      is_successful: false,
      remote_peer_id: None,
      supports_extensions: false,
      supports_dht: false,
    };
  };

  let protocol_ok = received[0..20] == sent[0..20];
  let info_hash_ok = received[28..48] == sent[28..48];

  let is_successful = protocol_ok && info_hash_ok;
  let remote_peer_id = Bytes::copy_from_slice(&received[48..68]);

  let supports_extensions = is_successful && (received[25] & 0x10) != 0;
  let supports_dht = is_successful && (received[27] & 0x01) != 0;

  HandshakeRes {
    is_successful,
    remote_peer_id: Some(remote_peer_id),
    supports_extensions,
    supports_dht,
  }
}
