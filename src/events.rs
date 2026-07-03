use std::time::{Duration, Instant};
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct Event {
    id: Uuid,
    kind: EventKind,
}

impl Event {
    pub fn new(id: Uuid, kind: EventKind) -> Self {
        Self { id, kind }
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn kind(&self) -> &EventKind {
        &self.kind
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EventKind {
    Queued,
    Started,
    RetryScheduled { attempt: u32, next_delay_ms: u64 },
    PauseStarted,
    Paused,
    CancellationStarted,
    Cancelled,
    Completed,
    Failed { error: String },
}

impl std::fmt::Display for EventKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventKind::Queued => write!(f, "Queued"),
            EventKind::Started => write!(f, "Started"),
            EventKind::RetryScheduled {
                attempt,
                next_delay_ms,
            } => {
                write!(f, "Retrying: attempt {} in {} ms", attempt, next_delay_ms)
            }
            EventKind::PauseStarted => write!(f, "Pause started"),
            EventKind::Paused => write!(f, "Paused"),
            EventKind::CancellationStarted => write!(f, "Cancellation started"),
            EventKind::Cancelled => write!(f, "Cancelled"),
            EventKind::Completed => write!(f, "Completed"),
            EventKind::Failed { error } => write!(f, "Failed: {}", error),
        }
    }
}

impl std::fmt::Display for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}]: {}", self.id, self.kind)
    }
}

/// Latest observed transfer progress for one download.
#[derive(Debug, Clone, Copy)]
pub struct Progress {
    bytes: u64,
    total: Option<u64>,

    started_at: Instant,
    last_sample_at: Instant, // last time we recomputed instantaneous_bps
    last_sample_bytes: u64,  // bytes_downloaded at last sample

    instantaneous_bps: f64, // most recent sample
    ema_bps: f64,           // exponential moving average
}

impl Progress {
    const EMA_ALPHA: f64 = 0.2;
    const MIN_SAMPLE_BYTES: u64 = 64 * 1024; // 64 KiB
    const MIN_SAMPLE_INTERVAL: Duration = Duration::from_millis(200);

    pub(crate) fn new(starting_bytes: u64, total: Option<u64>) -> Self {
        let now = Instant::now();
        Progress {
            bytes: starting_bytes,
            total,
            instantaneous_bps: 0.0,
            ema_bps: 0.0,
            started_at: now,
            last_sample_at: now,
            last_sample_bytes: starting_bytes,
        }
    }

    pub(crate) fn add(&mut self, n: u64) -> bool {
        self.bytes += n;
        let now = Instant::now();
        let dt = now.duration_since(self.last_sample_at);
        let byte_delta = self.bytes - self.last_sample_bytes;

        if dt >= Self::MIN_SAMPLE_INTERVAL || byte_delta >= Self::MIN_SAMPLE_BYTES {
            self.recompute(now, dt, byte_delta);
            self.last_sample_at = now;
            self.last_sample_bytes = self.bytes;
            return true;
        }
        false
    }

    fn recompute(&mut self, _now: Instant, dt: Duration, byte_delta: u64) {
        let secs = dt.as_secs_f64();
        if secs > 0.0 && byte_delta > 0 {
            let inst = byte_delta as f64 / secs;
            self.instantaneous_bps = inst;
            self.ema_bps = if self.ema_bps <= 0.0 {
                inst
            } else {
                Self::EMA_ALPHA * inst + (1.0 - Self::EMA_ALPHA) * self.ema_bps
            };
        }
    }

    /// Number of bytes downloaded, including a resumed prefix.
    pub fn bytes_downloaded(&self) -> u64 {
        self.bytes
    }

    /// Expected total size, when provided by the server.
    pub fn total_bytes(&self) -> Option<u64> {
        self.total
    }

    /// Smoothed transfer rate in bytes per second.
    pub fn bytes_per_second(&self) -> f64 {
        self.ema_bps
    }

    /// Time elapsed since the current transfer attempt started.
    pub fn elapsed(&self) -> Duration {
        self.started_at.elapsed()
    }

    /// Estimated time remaining, when size and rate are known.
    pub fn eta(&self) -> Option<Duration> {
        let total = self.total?;
        let remaining = total.saturating_sub(self.bytes);
        if self.ema_bps > 0.0 {
            Some(Duration::from_secs_f64(remaining as f64 / self.ema_bps))
        } else {
            None
        }
    }

    /// Completion percentage, when the total size is known and non-zero.
    pub fn percent(&self) -> Option<f64> {
        self.total
            .filter(|&total| total > 0)
            .map(|total| (self.bytes as f64 / total as f64) * 100.0)
    }

    pub(crate) fn force_update(&mut self) {
        let now = Instant::now();
        let dt = now.duration_since(self.last_sample_at);
        let byte_delta = self.bytes - self.last_sample_bytes;

        self.recompute(now, dt, byte_delta);
        self.last_sample_at = now;
        self.last_sample_bytes = self.bytes;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn samples_after_byte_threshold() {
        let mut progress = Progress::new(0, None);

        assert!(!progress.add(1024));
        assert!(progress.add(Progress::MIN_SAMPLE_BYTES - 1024));
        assert_eq!(progress.bytes_downloaded(), Progress::MIN_SAMPLE_BYTES);
        assert_eq!(progress.total_bytes(), None);
        assert_eq!(progress.percent(), None);
        assert_eq!(progress.eta(), None);
    }

    #[test]
    fn resumed_progress_updates_rate_percent_and_eta() {
        let mut progress = Progress::new(25, Some(100));
        progress.last_sample_at = Instant::now() - Duration::from_secs(1);

        assert!(progress.add(25));
        assert_eq!(progress.bytes_downloaded(), 50);
        assert_eq!(progress.percent(), Some(50.0));
        assert!(progress.bytes_per_second() > 0.0);
        assert!(progress.eta().is_some());
    }
}
