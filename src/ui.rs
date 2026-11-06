use ratatui::{
  Frame,
  crossterm::event::{self, Event, KeyCode, KeyEventKind, KeyModifiers},
  layout::{Constraint, Direction, Layout},
  style::Color,
  text::{Line, Span},
  widgets::{Block, Borders, Paragraph},
};
use std::time::Duration;
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

      if event::poll(Duration::from_millis(500)).unwrap_or(false) {
        match event::read() {
          Ok(Event::Key(key)) if key.kind == KeyEventKind::Press => {
            let ctrl_c =
              key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL);

            if key.code == KeyCode::Char('q') || ctrl_c {
              break;
            }
          }
          _ => {}
        }
      }
    }

    ratatui::restore();
  })
}

fn draw(f: &mut Frame, s: &StatsSnapshot) {
  let area = f.area();

  let block = Block::default()
    .borders(Borders::ALL)
    .border_type(ratatui::widgets::BorderType::Rounded)
    .padding(ratatui::widgets::Padding::uniform(1));

  let inner = block.inner(area);
  f.render_widget(block, area);

  let ratio = if s.total_bytes == 0 {
    0.0
  } else {
    (s.verified_bytes as f64 / s.total_bytes as f64).clamp(0.0, 1.0)
  };

  let pct = format!("{:.1}%", ratio * 100.0);
  let speed = format_mbps(s.ema_speed.max(0.0));
  let eta = format_eta(s.total_bytes.saturating_sub(s.verified_bytes), s.ema_speed);
  let memory = memory_stats::memory_stats();

  let chunks = Layout::default()
    .direction(Direction::Vertical)
    .constraints([
      Constraint::Length(5), // Progress section text (Progress, ETA, Speed, Downloaded, Uploaded)
      Constraint::Length(4), // Peers section text (Margin + Peers, Choked, Seeders)
      Constraint::Length(2), // Memory lines text (Margin + RSS/Heap info)
    ])
    .split(inner);

  // Progress Section (Combining elements to exactly follow TS formatting and colors)
  let progress = vec![
    Line::from(vec![
      Span::styled(
        format!(
          "Progress {} pieces ({}) — ",
          s.pieces_completed, pct
        ),
        Color::White,
      ),
      Span::styled(
        format!("{} total", format_bytes(s.verified_bytes)),
        Color::White,
      ),
    ]),
    Line::from(Span::styled(format!("ETA: {eta}"), Color::White)),
    Line::from(Span::styled(format!("Speed: {speed}"), Color::White)),
    Line::from(Span::styled(
      format!(
        "Downloaded (Not written to Disk) {}",
        format_bytes(s.bytes_in_progress)
      ),
      Color::Green,
    )),
    Line::from(Span::styled(
      format!("Uploaded {} total", format_bytes(s.uploaded_bytes)),
      Color::Green,
    )),
  ];
  f.render_widget(Paragraph::new(progress), chunks[0]);

  // Peers Section
  let peers = vec![
    Line::from(""), // Top margin
    Line::from(Span::styled(
      format!("Peers: {}", s.connected_peer_count),
      Color::White,
    )),
    Line::from(Span::styled(
      format!("Choked Peers: {}", s.choked_count),
      Color::LightGreen,
    )),
    Line::from(Span::styled(
      format!("Seeders: {}", s.seeder_count),
      Color::LightGreen,
    )),
  ];
  f.render_widget(Paragraph::new(peers), chunks[1]);

  // Memory Section
  let mut memory_lines = vec![Line::from("")]; // Top margin
  if let Some(mem) = memory {
    memory_lines.push(Line::from(Span::styled(
      format!("Memory: {} | Heap: N/A", format_bytes(mem.physical_mem as u64)),
      Color::Yellow,
    )));
  }
  f.render_widget(Paragraph::new(memory_lines), chunks[2]);
}

fn format_mbps(bytes_per_second: f64) -> String {
  format!("{:.2} Mbps", (bytes_per_second * 8.0) / 1e6)
}

fn format_eta(remaining_bytes: u64, speed: f64) -> String {
  if !speed.is_finite() || speed <= 0.0 {
    return "--".to_string();
  }

  let sec = (remaining_bytes as f64 / speed) as u64;

  if sec == 0 {
    return "done 🎉".to_string();
  }

  let h = sec / 3600;
  let m = (sec % 3600) / 60;
  let s = sec % 60;

  if h > 0 {
    format!("{h}h {m}m")
  } else if m > 0 {
    format!("{m}m {s}s")
  } else {
    format!("{s}s")
  }
}
