use std::{sync::Arc, time::Duration};

use reqwest::{Client, Method};
use tokio::{fs::File, io::AsyncWriteExt, sync::mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, trace, warn};
use uuid::Uuid;

use crate::{
    download::RemoteInfo, error::DownloadError, events::Progress, prelude::DownloadResult,
    request::Request,
};

pub(crate) enum WorkerMsg {
    Metadata {
        id: Uuid,
        info: RemoteInfo,
    },
    Progress {
        id: Uuid,
        bytes_downloaded: u64,
        total_bytes: Option<u64>,
        rate_bps: f64,
        eta: Option<Duration>,
    },
    Finish {
        id: Uuid,
        result: Result<DownloadResult, DownloadError>,
    },
}

#[instrument(level = "info", skip(request, client, worker_tx, cancel_token), fields(id = %request.id(), url = %request.url()))]
pub(crate) async fn run(
    request: Arc<Request>,
    client: Client,
    worker_tx: mpsc::Sender<WorkerMsg>,
    cancel_token: CancellationToken,
) {
    let result = attempt_download(request.as_ref(), client, cancel_token, worker_tx.clone()).await;
    if result.is_ok() {
        info!(id = %request.id(), "Download attempt finished successfully");
    } else {
        warn!(id = %request.id(), "Download attempt finished with error");
    }

    let _ = worker_tx
        .send(WorkerMsg::Finish {
            id: request.id(),
            result,
        })
        .await;
}

#[instrument(level = "debug", skip(request, client, cancel_token), fields(id = %request.id(), url = %request.url()))]
pub(crate) async fn probe_head(
    request: &Request,
    client: &Client,
    cancel_token: CancellationToken,
) -> Option<RemoteInfo> {
    use reqwest::header;
    debug!("Probing remote with HTTP HEAD");
    let req = client
        .request(Method::HEAD, request.url().as_ref())
        .headers(request.config().headers().clone())
        .send();

    let resp = tokio::select! {
        resp = req => resp.ok()?.error_for_status().ok()?,
        _ = cancel_token.cancelled() => return None,
    };

    let headers = resp.headers();
    let content_length = resp.content_length();
    trace!(content_length = ?content_length, "Got HEAD response");
    let accept_ranges = headers
        .get(header::ACCEPT_RANGES)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let etag = headers
        .get(header::ETAG)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let last_modified = headers
        .get(header::LAST_MODIFIED)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    Some(RemoteInfo {
        content_length,
        accept_ranges,
        etag,
        last_modified,
        content_type,
    })
}

#[instrument(level = "info", skip(request, client, cancel_token, worker_tx), fields(id = %request.id(), url = %request.url(), destination = ?request.destination()))]
pub(crate) async fn attempt_download(
    request: &Request,
    client: Client,
    cancel_token: CancellationToken,
    worker_tx: mpsc::Sender<WorkerMsg>,
) -> Result<DownloadResult, DownloadError> {
    if let Some(info) = probe_head(request, &client, cancel_token.clone()).await {
        let _ = worker_tx
            .send(WorkerMsg::Metadata {
                id: request.id(),
                info,
            })
            .await;
    }

    if let Some(parent) = request.destination().parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    if request.destination().exists() && !request.config().overwrite() {
        warn!(destination = ?request.destination(), "Destination exists and overwrite=false; failing");
        return Err(DownloadError::FileExists {
            path: request.destination().to_path_buf(),
        });
    }

    let req = client
        .request(Method::GET, request.url().as_ref())
        .headers(request.config().headers().clone())
        .send();

    let mut response = tokio::select! {
      resp = req => Ok(resp?.error_for_status()?),
        _ = cancel_token.cancelled() =>  Err(DownloadError::Cancelled),
    }?;
    let total_bytes = response.content_length();
    debug!(total_bytes = ?total_bytes, "Server accepted download");

    let mut file = File::create(request.destination()).await?;
    let mut progress = Progress::new(total_bytes);
    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => {
                warn!(destination = ?request.destination(), "Cancellation received; cleaning up partial file");
                drop(file);
                tokio::fs::remove_file(request.destination()).await?;
                return Err(DownloadError::Cancelled);
            }
            chunk = response.chunk() => {
                match chunk {
                    Ok(Some(chunk)) => {
                        file.write_all(&chunk).await?;
                        if progress.update(chunk.len() as u64) {
                            let _ = worker_tx.send(WorkerMsg::Progress {
                                id: request.id(),
                                bytes_downloaded: progress.bytes_downloaded(),
                                total_bytes,
                                rate_bps: progress.ema_bps,
                                eta: progress.eta()
                            })
                            .await;
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        error!(error = %e, destination = ?request.destination(), "Error while reading response chunk; removing partial file");
                        drop(file);
                        tokio::fs::remove_file(request.destination()).await?;
                        return Err(e.into());
                    }
                }
            }
        }
    }

    progress.force_update();
    let _ = worker_tx
        .send(WorkerMsg::Progress {
            id: request.id(),
            bytes_downloaded: progress.bytes_downloaded(),
            total_bytes,
            rate_bps: progress.ema_bps,
            eta: progress.eta(),
        })
        .await;
    file.sync_all().await?;
    info!(destination = ?request.destination(), bytes = progress.bytes_downloaded(), "Download completed successfully");

    Ok(DownloadResult {
        path: request.destination().to_path_buf(),
        bytes_downloaded: progress.bytes_downloaded(),
    })
}
