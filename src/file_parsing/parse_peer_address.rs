use bytes::Bytes;

pub fn parse_peer_address(peers: Bytes) -> Vec<String> {
  let mut result: Vec<String> = Vec::new();

  for peer in peers.chunks_exact(6) {
    let port = ((peer[4] as u16) << 8) | peer[5] as u16;
    let address = format!("{}.{}.{}.{}:{}", peer[0], peer[1], peer[2], peer[3], port);

    result.push(address);
  }

   result
}
