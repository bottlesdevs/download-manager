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
pub struct Progress {
    pub bytes_downloaded: u64,
    pub total_bytes: Option<u64>,

    pub instantaneous_bps: f64, // most recent sample
    pub ema_bps: f64,           // exponential moving average

    // Internal timing / sampling
    started_at: Instant,
    updated_at: Instant,     // last time we saw any bytes
    last_sample_at: Instant, // last time we recomputed instantaneous_bps
    last_sample_bytes: u64,  // bytes_downloaded at last sample

    // Sampling thresholds
    pub min_sample_interval: Duration,
    pub min_sample_bytes: u64,

    ema_alpha: f64, // smoothing factor for EMA
}

impl Progress {
    pub(crate) fn new(total_bytes: Option<u64>) -> Self {
        let now = Instant::now();
        Progress {
            bytes_downloaded: 0,
            total_bytes,
            instantaneous_bps: 0.0,
            ema_bps: 0.0,
            started_at: now,
            updated_at: now,
            last_sample_at: now,
            last_sample_bytes: 0,
            ema_alpha: 0.2,
            min_sample_bytes: 64 * 1024, // 64 KiB
            min_sample_interval: Duration::from_millis(200),
        }
    }

    pub fn bytes_downloaded(&self) -> u64 {
        self.bytes_downloaded
    }

    pub fn elapsed(&self) -> Duration {
        self.started_at.elapsed()
    }

    pub fn percent(&self) -> Option<f64> {
        self.total_bytes
            .filter(|&total| total > 0)
            .map(|total| (self.bytes_downloaded as f64 / total as f64) * 100.0)
    }

    pub fn remaining_bytes(&self) -> Option<u64> {
        self.total_bytes
            .map(|t| t.saturating_sub(self.bytes_downloaded))
    }

    pub fn eta(&self) -> Option<Duration> {
        let remaining = self.remaining_bytes()?;
        if self.ema_bps > 0.0 {
            Some(Duration::from_secs_f64(remaining as f64 / self.ema_bps))
        } else {
            None
        }
    }

    pub(crate) fn update(&mut self, chunk_len: u64) -> bool {
        let now = Instant::now();
        self.bytes_downloaded += chunk_len;
        self.updated_at = now;

        let dt = now.duration_since(self.last_sample_at);
        let byte_delta = self.bytes_downloaded - self.last_sample_bytes;

        if dt >= self.min_sample_interval || byte_delta >= self.min_sample_bytes {
            let secs = dt.as_secs_f64();
            if secs > 0.0 && byte_delta > 0 {
                // instantaneous over sample window
                let inst = (byte_delta as f64) / secs;
                self.instantaneous_bps = inst;

                // EMA (if first sample, seed with inst)
                if self.ema_bps <= 0.0 {
                    self.ema_bps = inst;
                } else {
                    self.ema_bps = self.ema_alpha * inst + (1.0 - self.ema_alpha) * self.ema_bps;
                }
            }

            self.last_sample_at = now;
            self.last_sample_bytes = self.bytes_downloaded;
            return true;
        }

        false
    }

    pub(crate) fn force_update(&mut self) {
        let now = Instant::now();
        let dt = now.duration_since(self.last_sample_at);
        let byte_delta = self.bytes_downloaded - self.last_sample_bytes;

        if dt.as_secs_f64() > 0.0 && byte_delta > 0 {
            let inst = (byte_delta as f64) / dt.as_secs_f64();
            self.instantaneous_bps = inst;
            if self.ema_bps <= 0.0 {
                self.ema_bps = inst;
            } else {
                self.ema_bps = self.ema_alpha * inst + (1.0 - self.ema_alpha) * self.ema_bps;
            }
            self.last_sample_at = now;
            self.last_sample_bytes = self.bytes_downloaded;
        }
        self.updated_at = now;
    }
}
