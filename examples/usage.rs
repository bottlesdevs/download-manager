use std::{path::PathBuf, sync::Arc};

use download_manager::prelude::*;
use http::{Response, header};
use http_client::{MockClient, body};
use tracing::info;
use url::Url;

// Production callers construct the backend under an entered Tokio context:
//
// let _guard = handle.enter();
// let http = Arc::new(http_client::ReqwestClient::new()?);
// let manager = DownloadManager::new(http, config)?;

fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().compact().init();

    futures_lite::future::block_on(async {
        let http = Arc::new(MockClient::new(|_| {
            Ok(Response::builder()
                .header(header::CONTENT_LENGTH, 13)
                .body(body("hello, world!"))?)
        }));
        let manager = DownloadManager::new(http, DownloadManagerConfig::default())?;

        let destination = PathBuf::from("example-download.bin");
        let download = manager.download(Url::parse("https://example.com/file")?, &destination)?;

        let result = download.await?;
        info!(path = %result.path.display(), bytes = result.bytes_downloaded);
        manager.shutdown().await;

        Ok(())
    })
}
