mod context;
mod download;
mod error;
mod events;
mod manager;
mod request;
mod scheduler;
mod worker;

pub use download::{Download, DownloadResult};
pub use error::{Error, Result};
pub use events::Event;
pub use manager::DownloadManager;
pub use request::Request;

pub mod prelude {
    pub use crate::{
        download::{Download, DownloadResult},
        error::{Error, Result},
        events::{Event, ProgressTracker},
        request::Request,
    };
}
