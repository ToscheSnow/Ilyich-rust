use bytes::{Bytes, BytesMut};

pub fn handshake_buf(info_hash: Bytes, client_id: &[u8]) -> Bytes {
  let mut buf = BytesMut::zeroed(68);
  // write pstrlen ie 1 byte
  buf[0] = 0x13;

  let protocol_string = b"BitTorrent protocol";

  // copy protocol_string into the buffer
  buf[1..20].copy_from_slice(protocol_string);

  //20 to 27 are reserved
  //already reserved to zero
  //advertise extensions
  buf[25] = 0x10;
  //advertise dht
  buf[27] = 0x01;

  //20 bytes for the info_hash
  buf[28..48].copy_from_slice(&info_hash);

  //20 bytes for the CLIENT_ID
  buf[48..68].copy_from_slice(&client_id);

  return buf.freeze();
}
