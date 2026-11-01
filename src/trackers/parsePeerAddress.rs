use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6};

use bytes::Bytes;

fn parse_ipv4_peer_address(peers: Bytes) -> Result<Vec<SocketAddr>, String> {
  if peers.len() % 6 != 0 {
    return Err("Invalid compact IPv4 peers".to_string());
  }
  let mut res = Vec::new();

  for chunk in peers.chunks_exact(6) {
    let ipv4_host = Ipv4Addr::new(chunk[0], chunk[1], chunk[2], chunk[3]);
    let port = u16::from_be_bytes([chunk[4], chunk[5]]);

    let socket_addr = SocketAddrV4::new(ipv4_host, port);
    res.push(socket_addr.into());
  }

  Ok(res)
}

pub fn parse_ipv6_peer_address(peers: Bytes) -> Result<Vec<SocketAddr>, String> {
  if peers.len() % 18 != 0 {
    return Err("Invalid compact IPv6 peers".to_string());
  }

  let mut res = Vec::new();

  for chunk in peers.chunks_exact(18) {
    let ipv6_host = Ipv6Addr::new(
      u16::from_be_bytes([chunk[0], chunk[1]]),
      u16::from_be_bytes([chunk[2], chunk[3]]),
      u16::from_be_bytes([chunk[4], chunk[5]]),
      u16::from_be_bytes([chunk[6], chunk[7]]),
      u16::from_be_bytes([chunk[8], chunk[9]]),
      u16::from_be_bytes([chunk[10], chunk[11]]),
      u16::from_be_bytes([chunk[12], chunk[13]]),
      u16::from_be_bytes([chunk[14], chunk[15]]),
    );

    let port = u16::from_be_bytes([chunk[16], chunk[17]]);
    let socket_addr = SocketAddrV6::new(ipv6_host, port, 0, 0);

    res.push(socket_addr.into());
  }
  Ok(res)
}
