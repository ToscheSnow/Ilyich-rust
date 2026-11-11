use std::time::Duration;

use axum::{
  Router,
  extract::{
    State, WebSocketUpgrade,
    ws::{self, Utf8Bytes, WebSocket},
  },
  response::{Html, Response},
  routing::get,
};
use tokio::{net::TcpListener, sync::watch, time::interval};

use crate::stats::StatsSnapshot;

const INDEX_HTML: &str = include_str!("../../assets/index.html");

pub fn run(stats_board: watch::Receiver<StatsSnapshot>, addr: String) {
  tokio::spawn(async move {
    let app = Router::new()
      .route("/", get(|| async { Html(INDEX_HTML) }))
      .route("/stats", get(upgrade_socket).with_state(stats_board));

    let listener = TcpListener::bind(&addr).await.unwrap();
    let url = format!("http://{}", addr);

    println!("🧭 Stats server running on http://{} ,go there to view live stats!", addr);

    let _ = open::that(&url);
    axum::serve(listener, app).await.unwrap();
  });
}

async fn upgrade_socket(
  ws: WebSocketUpgrade,
  State(stats_board): State<watch::Receiver<StatsSnapshot>>,
) -> Response {
  ws.max_frame_size(1024 * 1024)
    .max_message_size(1024 * 1024)
    .on_upgrade(move |socket| handle_socket(socket, stats_board))
}

async fn handle_socket(mut socket: WebSocket, stats_board: watch::Receiver<StatsSnapshot>) {
  let mut ticker = interval(Duration::from_secs(1));

  loop {
    //operations send stats to receiver
    ticker.tick().await;

    let snap = stats_board.borrow().clone();
    let Ok(json) = serde_json::to_string(&snap) else {
      continue;
    };

    if socket
      .send(ws::Message::Text(Utf8Bytes::from(json)))
      .await
      .is_err()
    {
      break;
    }

    if snap.total_bytes_have > 0 && snap.left == 0 {
      let _ = socket.send(ws::Message::Close(None)).await;
      break;
    }
  }
}
