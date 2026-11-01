use crate::file_parsing::parser_types::{BencodeDict, BencodeVal};
use std::collections::HashSet;

pub fn extract_tracker_urls(decoded_torrent: &BencodeDict) -> (Vec<String>, Vec<String>) {
  let mut http_urls = HashSet::new();
  let mut udp_urls = HashSet::new();

  let mut http_count = 0;
  let mut udp_count = 0;

  if let Some(BencodeVal::List(announcers)) = decoded_torrent.get("announce-list") {
    for tier in announcers {
      if let BencodeVal::List(tier) = tier {
        for tracker_bytes in tier {
          if let BencodeVal::Bytes(tracker_bytes) = tracker_bytes {
            let url = String::from_utf8_lossy(tracker_bytes).into_owned();
            if is_http_tracker(&url) {
              if http_urls.insert(url) {
                http_count += 1;
              }
            } else if is_udp_tracker(&url) {
              if udp_urls.insert(url) {
                udp_count += 1;
              }
            }
          }
        }
      }
    }
  }

  if let Some(BencodeVal::Bytes(announce_bytes)) = decoded_torrent.get("announce") {
    let announce = String::from_utf8_lossy(announce_bytes).into_owned();

    if is_http_tracker(&announce) {
      http_urls.insert(announce);
      http_count += 1;
    } else if is_udp_tracker(&announce) {
      udp_urls.insert(announce);
      udp_count += 1;
    }
  }

  println!("\n HTTP trackers found {http_count}");
  println!("UDP trackers found {udp_count}");

  (
    http_urls.into_iter().collect(),
    udp_urls.into_iter().collect(),
  )
}

fn is_http_tracker(url: &str) -> bool {
  return url.starts_with("http://") || url.starts_with("https://");
}

fn is_udp_tracker(url: &str) -> bool {
  return url.starts_with("udp://");
}
