pub struct TrackerAnnounceArgs {
  info_hash: [u8; 20],
  peer_id: [u8; 20],
  port: u16,
}

pub struct HttpTracker {}
