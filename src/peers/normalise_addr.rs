use std::net::SocketAddr;

pub fn normalise_addr(client_addr: Option<SocketAddr>) -> String {
  let Some(addr) = client_addr else {
    return "unknown".to_string();
  };

  match addr {
    SocketAddr::V4(ip4) => ip4.to_string(),
    SocketAddr::V6(ipv6) => match ipv6.ip().to_ipv4_mapped() {
      Some(ipv4) => ipv4.to_string(),
      None => ipv6.ip().to_string(),
    },
  }
}
