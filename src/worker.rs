use std::sync::Arc;

use reqwest::{Client, Method, Response, StatusCode, header};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};

use crate::{
    error::{Error, Result, ResultExt},
    events::Progress,
    prelude::DownloadResult,
    request::Request,
    storage::{self, Manifest, PartFile},
};

pub(crate) enum WorkerResult {
    Finished(DownloadResult),
    Stopped,
}

#[instrument(level = "info", skip(request, client, progress_tx, stop_requested_token), fields(url = %request.url(), destination = ?request.destination()))]
pub(crate) async fn run(
    request: Arc<Request>,
    client: Client,
    progress_tx: watch::Sender<Progress>,
    stop_requested_token: CancellationToken,
) -> Result<WorkerResult> {
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
    let mut response = tokio::select! {
        biased;
        _ = stop_requested_token.cancelled() => {
            return Ok(WorkerResult::Stopped);
        }
        response = send_get(&request, &client, offset, prior.as_ref()) => response?,
    };
    if response.status() == StatusCode::RANGE_NOT_SATISFIABLE {
        debug!(offset, "Range not satisfiable; restarting from 0");
        response = tokio::select! {
            biased;
            _ = stop_requested_token.cancelled() => {
                return Ok(WorkerResult::Stopped);
            }
            response = send_get(&request, &client, 0, None) => response?,
        };
    }

    // 206 -> server honored the range (resume); anything else (200) -> full body.
    let resumed = response.status() == StatusCode::PARTIAL_CONTENT;
    let mut response = response.error_for_status()?;
    let start = if resumed { offset } else { 0 };
    let total = response.content_length().map(|remaining| start + remaining);

    let mut manifest = Manifest {
        url: request.url().to_string(),
        etag: response_header(&response, header::ETAG),
        last_modified: response_header(&response, header::LAST_MODIFIED),
        total_length: total,
        completed_ranges: Vec::new(),
    };
    manifest.set_contiguous(start);
    let mut part = PartFile::open(dest, start, manifest).await?;
    let mut progress = Progress::new(start, total);
    progress_tx.send_replace(progress);
    debug!(start, ?total, resumed, "Transfer started");

    loop {
        tokio::select! {
            biased;
            _ = stop_requested_token.cancelled() => {
                warn!(?dest, "Stop requested; checkpointing partial download");
                part.checkpoint().await?;
                return Ok(WorkerResult::Stopped);
            }
            chunk = response.chunk() => {
                match chunk {
                    Ok(Some(chunk)) => {
                        part.write(&chunk).await?;
                        if progress.add(chunk.len() as u64) {
                            progress_tx.send_replace(progress);
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
    progress_tx.send_replace(progress);
    let path = part.finalize().await?;
    info!(
        ?path,
        bytes = progress.bytes_downloaded(),
        "Download completed successfully"
    );

    Ok(WorkerResult::Finished(DownloadResult {
        path,
        bytes_downloaded: progress.bytes_downloaded(),
    }))
}

/// Issue the GET, attaching `Range`/`If-Range` when resuming from `offset > 0`.
/// Does not call `error_for_status`: the caller inspects the raw status first so
/// it can handle `416` (range not satisfiable) before treating it as an error.
async fn send_get(
    request: &Request,
    client: &Client,
    offset: u64,
    prior: Option<&Manifest>,
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

    builder.send().await.map_err(Into::into)
}

fn response_header(response: &Response, name: header::HeaderName) -> Option<String> {
    response
        .headers()
        .get(name)
        .and_then(|value| value.to_str().log_debug())
        .map(str::to_string)
}
