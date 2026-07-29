mod context;
pub mod download;
pub mod error;
pub mod events;
pub mod manager;
pub mod request;
mod scheduler;
mod storage;
mod worker;

pub mod prelude {
    pub use super::download::{Download, DownloadResult};
    pub use super::error::Error;
    pub use super::events::{Event, Progress};
    pub use super::manager::{DownloadManager, DownloadManagerConfig, SchedulerFuture};
    pub use super::request::{Request, RequestBuilder};
}
