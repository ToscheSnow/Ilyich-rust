#[derive(Clone)]
pub struct TrackerAnnounceArgs {
  pub info_hash: [u8; 20],
  pub peer_id: [u8; 20],
  pub port: u16,
}

pub struct HttpTracker {}
