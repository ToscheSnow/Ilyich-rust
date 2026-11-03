use std::time::Duration;

use ratatui::{
  Frame,
  crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
  style::{Color, Style},
  text::Line,
  widgets::{Block, BorderType, Padding, Paragraph},
};
use tokio::{sync::watch, task::JoinHandle};

use crate::stats::{StatsSnapshot, format_bytes};

pub fn start_stats_ui(rx: watch::Receiver<StatsSnapshot>) -> JoinHandle<()> {
  tokio::task::spawn_blocking(move || {
    let mut terminal = ratatui::init();
    loop {
      let snap = rx.borrow().clone();
      if terminal.draw(|f| draw(f, &snap)).is_err() {
        break;
      }

      // doubles as the refresh timer (like your 500ms setInterval)
      if event::poll(Duration::from_millis(500)).unwrap_or(false) {
        if let Ok(Event::Key(key)) = event::read() {
          let ctrl_c =
            key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL);
          if key.kind == KeyEventKind::Press && (key.code == KeyCode::Char('q') || ctrl_c) {
            break;
          }
        }
      }
    }
    ratatui::restore();
  })
}

fn draw(f: &mut Frame, s: &StatsSnapshot) {
  let ratio = if s.total_bytes == 0 {
    0.0
  } else {
    (s.verified_bytes as f64 / s.total_bytes as f64).clamp(0.0, 1.0)
  };

  let mem = match memory_stats::memory_stats() {
    Some(m) => format!("RSS: {} ", format_bytes(m.physical_mem as u64),),
    None => "memory stats unavailable".to_string(),
  };

  let bright_green = Style::new().fg(Color::LightGreen);
  let green = Style::new().fg(Color::Green);
  let white = Style::new().fg(Color::White);
  let yellow = Style::new().fg(Color::LightYellow);

  let lines = vec![
    Line::styled(
      format!(
        "Progress: {} pieces ({:.1}%) — {} total",
        s.pieces_completed,
        ratio * 100.0,
        format_bytes(s.verified_bytes)
      ),
      bright_green,
    ),
    Line::styled(
      format!("Uploaded {} total", format_bytes(s.uploaded_bytes)),
      green,
    ),
    Line::styled(format!("Speed: {}", format_mbps(s.ema_speed)), white),
    Line::styled(
      format!(
        "Downloaded (Not written to Disk) {}",
        format_bytes(s.bytes_in_progress)
      ),
      white,
    ),
    Line::styled(format!("ETA: {}", format_eta(s.left, s.ema_speed)), white),
    Line::raw(""),
    Line::styled(format!("Peers: {}", s.connected_peer_count), white),
    Line::styled(format!("Choked Peers: {}", s.choked_count), white),
    Line::styled(format!("Seeders: {}", s.seeder_count), white),
    Line::raw(""),
    Line::styled(mem, yellow),
  ];

  f.render_widget(
    Paragraph::new(lines).block(
      Block::bordered()
        .border_type(BorderType::Rounded) // borderStyle="round"
        .padding(Padding::uniform(1)), // padding={1}
    ),
    f.area(),
  );
}

fn format_mbps(bytes_per_sec: f64) -> String {
  format!("{:.2} Mbps", bytes_per_sec * 8.0 / 1e6)
}

fn format_eta(left: u64, bytes_per_sec: f64) -> String {
  if left == 0 {
    return "done 🎉".to_string();
  }
  if bytes_per_sec <= 0.0 {
    return "--".to_string();
  }
  let sec = (left as f64 / bytes_per_sec) as u64;
  let (h, m, s) = (sec / 3600, (sec % 3600) / 60, sec % 60);
  if h > 0 {
    format!("{h}h {m}m")
  } else if m > 0 {
    format!("{m}m {s}s")
  } else {
    format!("{s}s")
  }
}
