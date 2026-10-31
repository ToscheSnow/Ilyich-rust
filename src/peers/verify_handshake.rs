use bytes::Bytes;

pub struct HandshakeRes {
  pub is_successful: bool,
  pub remote_peer_id: Option<Bytes>,
}

pub fn verify_handshake(sent: Bytes, received: Bytes) -> HandshakeRes {
  if received.len() < 68 {
    return HandshakeRes {
      is_successful: false,
      remote_peer_id: None,
    };
  };

  let protocol_ok = received[0..20] == sent[0..20];

  let info_hash_ok = received[28..48] == sent[28..48];

  let is_successful = protocol_ok && info_hash_ok;

  let remote_peer_id = Bytes::copy_from_slice(&received[48..68]);

  HandshakeRes {
    is_successful,
    remote_peer_id: Some(remote_peer_id),
  }
}
