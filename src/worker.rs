use std::{sync::Arc, time::Duration};

use reqwest::{Client, Method, Response, StatusCode, header};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

use crate::{
    download::RemoteInfo,
    error::{Error, Result, ResultExt},
    events::ProgressTracker,
    prelude::DownloadResult,
    request::Request,
    storage::{self, Manifest, PartFile},
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
}

#[instrument(level = "info", skip(request, client, cancel_token, worker_tx), fields(id = %request.id(), url = %request.url(), destination = ?request.destination()))]
pub(crate) async fn run(
    request: Arc<Request>,
    client: Client,
    worker_tx: mpsc::Sender<WorkerMsg>,
    cancel_token: CancellationToken,
) -> Result<DownloadResult> {
    let dest = request.destination();

    // `overwrite` guards the *final* path; partial data lives in `<dest>.part`.
    if tokio::fs::try_exists(dest).await? && !request.config.overwrite() {
        warn!(?dest, "Destination exists and overwrite=false; failing");
        return Err(Error::FileExists {
            path: dest.to_path_buf(),
        });
    }

    // Resume only from a durable, validator-bearing prefix that is actually on
    // disk (clamp against the real `.part` length, never trust the manifest alone).
    let prior = Manifest::load(dest)
        .await
        .filter(|manifest| manifest.is_resumable_for(request.url().as_str()));
    let offset = match &prior {
        Some(m) => m.resume_offset().min(storage::part_len(dest).await?),
        None => 0,
    };

    // The GET is the source of truth: ask for the range we want and let the
    // response status decide. 416 means our offset is stale/complete -> restart.
    let mut response = send_get(&request, &client, offset, prior.as_ref(), &cancel_token).await?;
    if response.status() == StatusCode::RANGE_NOT_SATISFIABLE {
        debug!(offset, "Range not satisfiable; restarting from 0");
        response = send_get(&request, &client, 0, None, &cancel_token).await?;
    }

    // 206 -> server honored the range (resume); anything else (200) -> full body.
    let resumed = response.status() == StatusCode::PARTIAL_CONTENT;
    let mut response = response.error_for_status()?;
    let start = if resumed { offset } else { 0 };
    let total = response.content_length().map(|remaining| start + remaining);

    let info = remote_info(&response, total);
    let _ = worker_tx
        .send(WorkerMsg::Metadata {
            id: request.id(),
            info: info.clone(),
        })
        .await
        .log_debug();

    let mut manifest = Manifest {
        url: request.url().to_string(),
        etag: info.etag,
        last_modified: info.last_modified,
        total_length: total,
        completed_ranges: Vec::new(),
    };
    manifest.set_contiguous(start);
    let mut part = PartFile::open(dest, start, manifest).await?;
    let mut progress = ProgressTracker::new(start, total);
    debug!(start, ?total, resumed, "Transfer started");

    loop {
        tokio::select! {
            _ = cancel_token.cancelled() => {
                warn!(?dest, "Cancellation received; discarding partial download");
                part.discard().await?;
                return Err(Error::Cancelled);
            }
            chunk = response.chunk() => {
                match chunk {
                    Ok(Some(chunk)) => {
                        part.write(&chunk).await?;
                        if progress.add(chunk.len() as u64) {
                            send_progress(&worker_tx, request.id(), &progress, total).await;
                        }
                    }
                    Ok(None) => break,
                    Err(e) => {
                        // Keep `.part` + manifest so a retry / next run can resume.
                        let _ = part.checkpoint().await.log_warn();
                        error!(error = %e, ?dest, "Transfer error; keeping partial for resume");
                        return Err(e.into());
                    }
                }
            }
        }
    }

    progress.force_update();
    send_progress(&worker_tx, request.id(), &progress, total).await;
    let path = part.finalize().await?;
    info!(
        ?path,
        bytes = progress.bytes(),
        "Download completed successfully"
    );

    Ok(DownloadResult {
        path,
        bytes_downloaded: progress.bytes(),
    })
}

/// Issue the GET, attaching `Range`/`If-Range` when resuming from `offset > 0`.
/// Does not call `error_for_status`: the caller inspects the raw status first so
/// it can handle `416` (range not satisfiable) before treating it as an error.
async fn send_get(
    request: &Request,
    client: &Client,
    offset: u64,
    prior: Option<&Manifest>,
    cancel_token: &CancellationToken,
) -> Result<Response> {
    let mut builder = client
        .request(Method::GET, request.url().as_ref())
        .headers(request.config.headers().clone());

    if offset > 0 {
        builder = builder.header(header::RANGE, format!("bytes={offset}-"));
        if let Some(validator) = prior.and_then(Manifest::validator) {
            builder = builder.header(header::IF_RANGE, validator);
        }
    }

    tokio::select! {
        resp = builder.send() => Ok(resp?),
        _ = cancel_token.cancelled() => {
            // Cancelled before any PartFile is open; clear any leftover `.part`
            // from a prior run so "cleanup succeeded" holds on this path too.
            storage::discard_partial(request.destination()).await?;
            Err(Error::Cancelled)
        }
    }
}

fn remote_info(response: &Response, total: Option<u64>) -> RemoteInfo {
    let headers = response.headers();
    let get = |name: header::HeaderName| {
        headers
            .get(name)
            .and_then(|value| value.to_str().log_debug())
            .map(str::to_string)
    };
    RemoteInfo {
        content_length: total,
        accept_ranges: get(header::ACCEPT_RANGES),
        etag: get(header::ETAG),
        last_modified: get(header::LAST_MODIFIED),
        content_type: get(header::CONTENT_TYPE),
    }
}

async fn send_progress(
    tx: &mpsc::Sender<WorkerMsg>,
    id: Uuid,
    progress: &ProgressTracker,
    total: Option<u64>,
) {
    let _ = tx
        .send(WorkerMsg::Progress {
            id,
            bytes_downloaded: progress.bytes(),
            total_bytes: total,
            rate_bps: progress.rate_bps(),
            eta: progress.eta(),
        })
        .await
        .log_debug();
}
