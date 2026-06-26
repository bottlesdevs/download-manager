use crate::download::RemoteInfo;
use reqwest::Url;
use std::path::PathBuf;
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

#[derive(Debug, Clone)]
pub enum EventKind {
    Queued {
        url: Url,
        destination: PathBuf,
    },
    Probed {
        info: RemoteInfo,
    },
    Progress {
        bytes_downloaded: u64,
        total_bytes: Option<u64>,
    },
    Started {
        url: Url,
        destination: PathBuf,
        total_bytes: Option<u64>,
    },
    Retrying {
        attempt: u32,
        next_delay_ms: u64,
    },
    Completed {
        path: PathBuf,
        bytes_downloaded: u64,
    },
    Failed {
        error: String,
    },
    Cancelled,
}

impl std::fmt::Display for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}]: {}", self.id, self.kind)
    }
}

impl std::fmt::Display for EventKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventKind::Queued { .. } => write!(f, "Queued"),
            EventKind::Probed { info } => write!(f, "Probed: {:?}", info),
            EventKind::Progress {
                bytes_downloaded,
                total_bytes,
            } => write!(f, "Progress: {:?}:{:?}", bytes_downloaded, total_bytes),
            EventKind::Failed { error } => write!(f, "Failed: {}", error),
            EventKind::Cancelled => write!(f, "Cancelled"),
            EventKind::Started { total_bytes, .. } => {
                if let Some(total) = total_bytes {
                    write!(f, "Started ({} bytes)", total)
                } else {
                    write!(f, "Started")
                }
            }
            EventKind::Retrying {
                attempt,
                next_delay_ms,
            } => {
                write!(f, "Retrying: attempt {} in {} ms", attempt, next_delay_ms)
            }
            EventKind::Completed {
                path,
                bytes_downloaded,
            } => {
                write!(f, "Completed: {:?} ({} bytes)", path, bytes_downloaded)
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ProgressTracker {
    bytes: u64,
    total: Option<u64>,

    started_at: Instant,
    last_sample_at: Instant, // last time we recomputed instantaneous_bps
    last_sample_bytes: u64,  // bytes_downloaded at last sample

    instantaneous_bps: f64, // most recent sample
    ema_bps: f64,           // exponential moving average
}

impl ProgressTracker {
    const EMA_ALPHA: f64 = 0.2;
    const MIN_SAMPLE_BYTES: u64 = 64 * 1024; // 64 KiB
    const MIN_SAMPLE_INTERVAL: Duration = Duration::from_millis(200);

    pub(crate) fn new(starting_bytes: u64, total: Option<u64>) -> Self {
        let now = Instant::now();
        ProgressTracker {
            bytes: starting_bytes,
            total,
            instantaneous_bps: 0.0,
            ema_bps: 0.0,
            started_at: now,
            last_sample_at: now,
            last_sample_bytes: 0,
        }
    }

    pub fn add(&mut self, n: u64) -> bool {
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

    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    pub fn total(&self) -> Option<u64> {
        self.total
    }

    pub fn rate_bps(&self) -> f64 {
        self.ema_bps
    }

    pub fn elapsed(&self) -> Duration {
        self.started_at.elapsed()
    }

    pub fn eta(&self) -> Option<Duration> {
        let total = self.total?;
        let remaining = total.saturating_sub(self.bytes);
        if self.ema_bps > 0.0 {
            Some(Duration::from_secs_f64(remaining as f64 / self.ema_bps))
        } else {
            None
        }
    }

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
