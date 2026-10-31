use bytes::Bytes;

use crate::peers::client::CLIENT_ID_BYTES;

pub struct TrackerUrlParams {
  pub url_path: String,
  pub info_hash: Bytes,
  pub client_port: u16,
  pub uploaded: u64,
  pub total_bytes: u64,
  pub downloaded: u64,
}

pub fn http_tracker_query(
  TrackerUrlParams {
    url_path,
    info_hash,
    client_port,
    downloaded,
    total_bytes,
    uploaded,
  }: TrackerUrlParams,
) -> String {
  let info_hash_encoded = encode_percentage_encoded(&info_hash);
  let peer_id_encoded = encode_percentage_encoded(&CLIENT_ID_BYTES);

  let query = format!(
    "port={}&uploaded={}&downloaded={}&left={}",
    client_port, uploaded, downloaded, total_bytes
  );

  format!(
    "{}?info_hash={}&peer_id={}&{}&compact=1",
    url_path, info_hash_encoded, peer_id_encoded, query
  )
}

fn encode_percentage_encoded(bytes: &[u8]) -> String {
  bytes.iter().map(|byte| format!("%{:02x}", byte)).collect()
}
