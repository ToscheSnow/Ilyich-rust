use bytes::Bytes;

pub struct PeerAddress {
  host:String,
  port:u16,
}

pub fn parse_peer_address(peers:Bytes) -> Vec<PeerAddress> {
  let mut result:Vec<PeerAddress> = Vec::new();

  for peer in peers.chunks_exact(6) {
    let host = format!("{}.{}.{}.{}",peer[0],peer[1],peer[2],peer[3]);
    let port = ((peer[4] as u16) << 8) | peer[5] as u16;

    result.push(PeerAddress { host, port });
  }

  return result;
}