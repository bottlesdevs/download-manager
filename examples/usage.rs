use std::{path::PathBuf, sync::Arc};

use download_manager::prelude::*;
use futures_util::StreamExt;
use http::{Response, header};
use http_client::{MockClient, body};
use tracing::info;
use url::Url;

// Production callers construct the backend under an entered Tokio context:
//
// let _guard = handle.enter();
// let http = Arc::new(http_client::ReqwestClient::new()?);
// let (manager, scheduler) = DownloadManager::new(http, config);
// tokio::spawn(scheduler);

fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().compact().init();
    let executor = async_executor::Executor::new();

    futures_lite::future::block_on(executor.run(async {
        let http = Arc::new(MockClient::new(|_| {
            Ok(Response::builder()
                .header(header::CONTENT_LENGTH, 13)
                .body(body("hello, world!"))?)
        }));
        let (manager, scheduler) = DownloadManager::new(http, DownloadManagerConfig::default());
        let scheduler = executor.spawn(scheduler);

        let destination = PathBuf::from("example-download.bin");
        let download = manager.download(Url::parse("https://example.com/file")?, &destination)?;
        let mut events = download.events();
        let mut progress = download.progress();
        let event_task = executor.spawn(async move {
            while let Some(event) = events.next().await {
                info!(%event);
            }
        });
        let progress_task = executor.spawn(async move {
            while let Some(progress) = progress.next().await {
                info!(
                    bytes = progress.bytes_downloaded(),
                    total = ?progress.total_bytes(),
                );
            }
        });

        let result = download.await?;
        info!(path = %result.path.display(), bytes = result.bytes_downloaded);
        manager.shutdown().await;
        scheduler.await;
        event_task.await;
        progress_task.await;

        Ok(())
    }))
}
