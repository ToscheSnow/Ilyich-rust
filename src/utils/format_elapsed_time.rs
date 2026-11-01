use std::time::Duration;

pub fn format_elapsed_time(duration: Duration) -> String {
  let total_seconds = duration.as_secs();

  let hours = total_seconds / 3600;
  let minutes = (total_seconds / 60) % 60;
  let seconds = total_seconds % 60;

  let mut result = Vec::new();

  if hours > 0 {
    result.push(format!("{hours} hr"));
  }

  if minutes > 0 {
    result.push(format!("{minutes} minutes"));
  }

  if seconds > 0 || result.is_empty() {
    result.push(format!("{seconds} seconds"));
  }

  result.join(" ")
}
