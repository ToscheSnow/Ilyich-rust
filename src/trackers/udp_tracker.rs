use bytes::{Buf, Bytes};
use rand::RngExt;
use std::{
  collections::{HashMap, HashSet},
  net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
  sync::Arc,
  time::{Duration, Instant},
};
use tokio::{
  net::UdpSocket,
  sync::{
    mpsc::{self},
    watch,
  },
  task::JoinHandle,
  time::{sleep, timeout},
};

use crate::{
  peers::peer_manager::PeerManagerMessage,
  stats::{StatsSnapshot, TransferStats},
  trackers::http_tracker::TrackerAnnounceArgs,
};

pub enum TrackerState {
  Disconnected,
  Connected {
    connection_id: u64,
    expires_at: Instant,
  },
  Connecting,
}

#[derive(Clone)]
pub struct Datagram {
  to: SocketAddr,
  message: Bytes,
}

pub enum TrackerEvent {
  ConnectResponse {
    to: SocketAddr,
    transaction_id: u32,
    connection_id: u64,
  },

  AnnounceResponse {
    to: SocketAddr,
    transaction_id: u32,
    interval: u32,
    seeders: u32,
    leechers: u32,
    peers: Vec<SocketAddr>,
  },
  Disconnect(SocketAddr),

  Error {
    transaction_id: u32,
    error_message: String,
  },

  RequestTimeout {
    to: SocketAddr,
    action: TrackerAction,
    transaction_id: u32,
    tries_done: u8,
    connection_id: Option<u64>,
  },

  Resolved(SocketAddr),
  ResolveFailed(String),
}

#[derive(Debug)]
pub enum TrackerAction {
  Connect,
  Announce,
}

pub struct UdpTracker {
  session_key: u32,

  pending_requests: HashMap<u32, (SocketAddr, JoinHandle<()>)>,

  event_tx: mpsc::Sender<TrackerEvent>,
  event_rx: mpsc::Receiver<TrackerEvent>,

  socket_rx: Option<mpsc::Receiver<Datagram>>,
  socket_tx: mpsc::Sender<Datagram>,

  tracker_states: HashMap<SocketAddr, TrackerState>,
  failed_trackers: HashSet<String>,

  urls: Vec<String>,
  args: TrackerAnnounceArgs,

  stats_board: watch::Receiver<StatsSnapshot>,
  peer_manager_sender: mpsc::Sender<PeerManagerMessage>,

  transfer_stats: TransferStats,
}

impl UdpTracker {
  pub fn new(
    urls: Vec<String>,
    args: TrackerAnnounceArgs,
    stats_board: watch::Receiver<StatsSnapshot>,
    peer_manager_sender: mpsc::Sender<PeerManagerMessage>,
    total_length: u64,
  ) -> Self {
    let session_key: u32 = rand::rng().random();

    let (event_tx, event_rx) = mpsc::channel::<TrackerEvent>(200);
    let (socket_tx, socket_rx) = mpsc::channel::<Datagram>(300);

    let tracker = Self {
      session_key,
      args,
      urls,
      peer_manager_sender,
      pending_requests: HashMap::new(),
      failed_trackers: HashSet::new(),
      stats_board,
      tracker_states: HashMap::new(),

      event_rx,
      event_tx,

      socket_rx: Some(socket_rx),
      socket_tx,

      transfer_stats: TransferStats {
        left: total_length,
        downloaded: 0,
        uploaded: 0,
      },
    };

    tracker
  }

  pub fn run(mut self) -> JoinHandle<()> {
    tokio::spawn(async move {
      let socket = Arc::new(
        UdpSocket::bind("0.0.0.0:0")
          .await
          .expect("couldn't make udp socket"),
      );
      let socket_rx = self.socket_rx.take().expect("run called twice");

      Self::reader(Arc::clone(&socket), self.event_tx.clone());
      Self::writer(socket, socket_rx);

      let mut ticker = tokio::time::interval(Duration::from_secs(60)); // first tick is immediate

      loop {
        tokio::select! {
          _ = ticker.tick() => self.announce_all(),
          Some(event) = self.event_rx.recv() => self.reducer(event).await,
        }
      }
    })
  }

  fn announce_all(&self) {
    for url in &self.urls {
      if self.failed_trackers.contains(url) {
        continue;
      }
      self.request_tracker(url.clone());
    }
  }

  fn request_tracker(&self, url: String) {
    let events = self.event_tx.clone(); // the task gets its own clone, never self
    tokio::spawn(async move {
      let hostport = url
        .strip_prefix("udp://")
        .and_then(|rest| rest.split('/').next())
        .unwrap_or("")
        .to_string();

      let addr = match timeout(Duration::from_secs(5), tokio::net::lookup_host(hostport)).await {
        Ok(Ok(mut addrs)) => addrs.find(|a| a.is_ipv4()), // v4 socket for now
        _ => None,
      };

      let event = match addr {
        Some(a) => TrackerEvent::Resolved(a),
        None => TrackerEvent::ResolveFailed(url),
      };
      let _ = events.send(event).await;
    });
  }

  fn spawn(mut self) -> JoinHandle<()> {
    tokio::spawn(async move {
      loop {
        match self.event_rx.recv().await {
          Some(event) => self.reducer(event).await,
          None => break,
        }
      }
    })
  }

  fn reader(socket_reader: Arc<UdpSocket>, event_tx: mpsc::Sender<TrackerEvent>) -> JoinHandle<()> {
    tokio::spawn(async move {
      let mut buf = [0u8; 10 * 1024];
      loop {
        match socket_reader.recv_from(&mut buf).await {
          Ok((num, from)) => {
            let bytes = Bytes::copy_from_slice(&buf[0..num]);
            match Self::parse_tracker_event(bytes, from) {
              Ok(event) => {
                event_tx.send(event).await;
              }
              Err(error_string) => {
                // println!("{}", error_string)
              }
            };
          }
          Err(_) => {}
        }
      }
    })
  }

  //mutates state
  async fn reducer(&mut self, event: TrackerEvent) {
    match event {
      TrackerEvent::AnnounceResponse {
        to,
        transaction_id,
        peers,
        ..
      } => {
        if !self.pending_requests.contains_key(&transaction_id) {
          return;
        }

        let (_, timer) = self.pending_requests.get(&transaction_id).unwrap();
        timer.abort();
        self.pending_requests.remove(&transaction_id);

        match self.tracker_states.get(&to) {
          Some(TrackerState::Connected { .. }) => {
            self
              .peer_manager_sender
              .send(PeerManagerMessage::AttemptConnection(peers))
              .await;
          }
          _ => {}
        }
      }
      TrackerEvent::ConnectResponse {
        to,
        transaction_id,
        connection_id,
      } => {
        if !self.pending_requests.contains_key(&transaction_id) {
          return;
        }

        let (_, timer) = self.pending_requests.get(&transaction_id).unwrap();
        timer.abort();
        self.pending_requests.remove(&transaction_id);

        match self.tracker_states.get(&to) {
          Some(TrackerState::Connecting) => {
            let new_state = TrackerState::Connected {
              connection_id,
              expires_at: Instant::now() + Duration::from_secs(60),
            };

            self.tracker_states.insert(to, new_state);
            self.send_announce_req(to, connection_id, 0);
          }
          _ => {
            return;
          }
        };
      }
      TrackerEvent::Disconnect(from) => {
        self.tracker_states.insert(from, TrackerState::Disconnected);
      }
      TrackerEvent::Error {
        transaction_id,
        error_message,
        ..
      } => {
        if !self.pending_requests.contains_key(&transaction_id) {
          return;
        }

        let (_, timer) = self.pending_requests.get(&transaction_id).unwrap();
        timer.abort();
        self.pending_requests.remove(&transaction_id);

        // println!("Received error from a tracker: {}", error_message);
        return;
      }
      TrackerEvent::RequestTimeout {
        to,
        action,
        transaction_id,
        tries_done,
        connection_id,
      } => {
        if !self.pending_requests.contains_key(&transaction_id) {
          return;
        }

        match action {
          TrackerAction::Connect => {
            self.pending_requests.remove(&transaction_id);

            self.send_connect_req(to, tries_done);
          }
          TrackerAction::Announce => {
            self.pending_requests.remove(&transaction_id);

            match self.tracker_states.get(&to) {
              Some(&TrackerState::Connected {
                connection_id,
                expires_at,
              }) => {
                if expires_at <= Instant::now() {
                  self.event_tx.send(TrackerEvent::Disconnect(to)).await;
                  self.send_connect_req(to, 0);
                } else {
                  self.send_announce_req(to, connection_id, tries_done);
                }
              }
              _ => {}
            }
          }
        }
      }
      TrackerEvent::ResolveFailed(url) => {
        self.failed_trackers.insert(url);
      }
      TrackerEvent::Resolved(addr) => {
        self.send_connect_req(addr, 0);
      }
    }
  }

  fn send_connect_req(&mut self, addr: SocketAddr, tries: u8) {
    if tries >= 4 {
      self.tracker_states.insert(addr, TrackerState::Disconnected);
      return;
    }

    self.tracker_states.insert(addr, TrackerState::Connecting);
    let (txid, buf) = Self::connect_request_buffer();
    let _ = self.socket_tx.try_send(Datagram {
      to: addr,
      message: Bytes::copy_from_slice(&buf),
    });

    let events = self.event_tx.clone();
    let delay = Duration::from_secs(2u64 << tries);
    let timer = tokio::spawn(async move {
      sleep(delay).await;
      let _ = events
        .send(TrackerEvent::RequestTimeout {
          to: addr,
          action: TrackerAction::Connect,
          transaction_id: txid,
          tries_done: tries + 1,
          connection_id: None,
        })
        .await;
    });
    self.pending_requests.insert(txid, (addr, timer));
  }
  fn send_announce_req(&mut self, addr: SocketAddr, connection_id: u64, tries: u8) {
    if tries >= 5 {
      self.tracker_states.insert(addr, TrackerState::Disconnected);
      return;
    }

    let transfer_stats = self.transfer_stats.clone();

    let Ok((txid, buf)) = Self::announce_request_buffer(
      self.session_key,
      connection_id,
      self.args.clone(),
      transfer_stats,
    ) else {
      return;
    };

    let _ = self.socket_tx.try_send(Datagram {
      to: addr,
      message: Bytes::copy_from_slice(&buf),
    });

    let events = self.event_tx.clone();
    let delay = Duration::from_secs(2u64 << tries);
    let timer = tokio::spawn(async move {
      sleep(delay).await;
      let _ = events
        .send(TrackerEvent::RequestTimeout {
          to: addr,
          action: TrackerAction::Announce,
          transaction_id: txid,
          tries_done: tries + 1,
          connection_id: Some(connection_id),
        })
        .await;
    });
    self.pending_requests.insert(txid, (addr, timer));
  }

  fn writer(
    socket_writer: Arc<UdpSocket>,
    mut socket_rx: mpsc::Receiver<Datagram>,
  ) -> JoinHandle<()> {
    tokio::spawn(async move {
      loop {
        match socket_rx.recv().await {
          Some(resp) => {
            //  Safe: Logs the error and allows the worker thread to keep running
            if let Err(e) = socket_writer.send_to(&resp.message, resp.to).await {
              // eprintln!("Failed to send UDP packet: {}", e);
              // Or return the error if inside a function that returns a Result:
              // return Err(e.into());
            }
          }
          None => break,
        }
      }
    })
  }

  fn parse_tracker_event(mut message: Bytes, from: SocketAddr) -> Result<TrackerEvent, String> {
    /*
    MESSAGE ID (4 Byte) : RESPONSE TYPE
    0                   : Connect response
    1                   : Announce response
    2                   : Scrape response (Not implemented and not useful)
    3                   : Error response
    */

    let message_id = message.get_u32();

    match message_id {
      0 => {
        if message.len() < 12 {
          return Err(format!(
            "Received connection response of insufficient length"
          ));
        }

        let transaction_id = message.get_u32();
        let connection_id = message.get_u64();
        return Ok(TrackerEvent::ConnectResponse {
          to: from,
          transaction_id,
          connection_id,
        });
      }
      1 => {
        if message.len() < 16 {
          return Err(format!("Received announce request of insufficient length"));
        }

        let transaction_id = message.get_u32();
        let interval = message.get_u32();
        let leechers = message.get_u32();
        let seeders = message.get_u32();

        let stride = if from.is_ipv4() { 6 } else { 18 };

        let peers: Vec<SocketAddr> = message
          .chunk() // remaining bytes as &[u8]
          .chunks_exact(stride) // drops any trailing partial record
          .map(|c| {
            let (ip, port) = c.split_at(stride - 2);
            let port = u16::from_be_bytes([port[0], port[1]]);
            let ip: IpAddr = match ip.len() {
              4 => Ipv4Addr::from(<[u8; 4]>::try_from(ip).unwrap()).into(),
              _ => Ipv6Addr::from(<[u8; 16]>::try_from(ip).unwrap()).into(),
            };
            SocketAddr::new(ip, port)
          })
          .collect();

        return Ok(TrackerEvent::AnnounceResponse {
          to: from,
          transaction_id,
          interval,
          seeders,
          leechers,
          peers,
        });
      }
      3 => {
        if message.len() < 4 {
          return Err(format!("Missing trasaction id in error message"));
        }
        let transaction_id = message.get_u32();
        let error_bytes = message.chunk();
        let error_message = String::from_utf8_lossy(&error_bytes).into_owned();

        return Ok(TrackerEvent::Error {
          transaction_id,
          error_message,
        });
      }
      _ => {
        return Err(format!("Invalid call to UdpTracker event parser"));
      }
    }
  }

  fn announce_request_buffer(
    session_key: u32,
    connection_id: u64,
    TrackerAnnounceArgs {
      info_hash,
      peer_id,
      port,
    }: TrackerAnnounceArgs,
    TransferStats {
      downloaded,
      uploaded,
      left,
    }: TransferStats,
  ) -> Result<(u32, [u8; 98]), String> {
    let mut buf = [0u8; 98];

    if info_hash.len() != 20 {
      return Err(format!("Info hash buffer is of the wrong size"));
    }

    if peer_id.len() != 20 {
      return Err(format!("Peer id buffer is of the wrong size"));
    }

    let transaction_id: u32 = rand::rng().random();
    //connection id
    buf[0..8].copy_from_slice(&connection_id.to_be_bytes());

    //32 bit action announce=1
    buf[8..12].copy_from_slice(&1_u32.to_be_bytes());
    //copy transaction id
    buf[12..16].copy_from_slice(&transaction_id.to_be_bytes());

    buf[16..36].copy_from_slice(&info_hash);

    buf[36..56].copy_from_slice(&peer_id);

    buf[56..64].copy_from_slice(&downloaded.to_be_bytes());

    buf[64..72].copy_from_slice(&left.to_be_bytes());
    buf[72..80].copy_from_slice(&uploaded.to_be_bytes());

    buf[80..84].copy_from_slice(&2_u32.to_be_bytes());
    buf[84..88].copy_from_slice(&0_u32.to_be_bytes());

    buf[88..92].copy_from_slice(&session_key.to_be_bytes());
    buf[92..96].copy_from_slice(&(-1_i32).to_be_bytes());
    buf[96..98].copy_from_slice(&port.to_be_bytes());

    Ok((transaction_id, buf))
  }

  fn connect_request_buffer() -> (u32, [u8; 16]) {
    let mut buf = [0u8; 16];
    let transaction_id: u32 = rand::rng().random();

    let magic_constant: u64 = 0x41727101980;
    buf[0..8].copy_from_slice(&magic_constant.to_be_bytes());
    buf[8..12].copy_from_slice(&0_u32.to_be_bytes());
    buf[12..16].copy_from_slice(&transaction_id.to_be_bytes());

    (transaction_id, buf)
  }
}
