use std::sync::Arc;

use async_broadcast::Sender;
use futures_lite::io::AsyncReadExt;
use futures_util::{FutureExt, select_biased};
use http::{Request as HttpRequest, Response, StatusCode, header};
use http_client::HttpClient;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};
use url::Url;

use crate::{
    error::{Error, Result, ResultExt},
    events::Progress,
    prelude::DownloadResult,
    request::{ChunkSource, Request, Source},
    storage::{self, Manifest, PartFile},
};

pub(crate) enum WorkerResult {
    Finished(DownloadResult),
    Stopped,
}

#[instrument(level = "info", skip(request, client, progress_tx, stop), fields(source = %request.describe(), destination = ?request.destination()))]
pub(crate) async fn run(
    request: Arc<Request>,
    client: Arc<dyn HttpClient>,
    progress_tx: Sender<Progress>,
    stop: CancellationToken,
) -> Result<WorkerResult> {
    let dest = request.destination();

    match async_fs::metadata(dest).await {
        Ok(_) if !request.config.overwrite => {
            warn!(?dest, "Destination exists and overwrite=false; failing");
            return Err(Error::FileExists {
                path: dest.to_path_buf(),
            });
        }
        Err(error) if error.kind() != std::io::ErrorKind::NotFound => return Err(error.into()),
        _ => {}
    }

    match request.source() {
        Source::Simple(url) => run_simple(&request, url, client.as_ref(), progress_tx, stop).await,
        Source::Chunked(chunks) => {
            run_chunked(&request, chunks, client.as_ref(), progress_tx, stop).await
        }
    }
}

async fn run_simple(
    request: &Request,
    url: &Url,
    client: &dyn HttpClient,
    progress_tx: Sender<Progress>,
    stop: CancellationToken,
) -> Result<WorkerResult> {
    let dest = request.destination();

    let prior = Manifest::load(dest)
        .await
        .filter(|manifest| manifest.is_resumable_for(url.as_str()));
    let offset = match &prior {
        Some(manifest) => manifest.resume_offset().min(storage::part_len(dest).await?),
        None => 0,
    };

    let mut response = {
        let stopped = stop.cancelled().fuse();
        let response = send_get(request, url, client, offset, prior.as_ref()).fuse();
        futures_util::pin_mut!(stopped, response);
        select_biased! {
            _ = stopped => return Ok(WorkerResult::Stopped),
            response = response => response?,
        }
    };
    if response.status() == StatusCode::RANGE_NOT_SATISFIABLE {
        debug!(offset, "Range not satisfiable; restarting from 0");
        let stopped = stop.cancelled().fuse();
        let restarted = send_get(request, url, client, 0, None).fuse();
        futures_util::pin_mut!(stopped, restarted);
        response = select_biased! {
            _ = stopped => return Ok(WorkerResult::Stopped),
            response = restarted => response?,
        };
    }

    let resumed = response.status() == StatusCode::PARTIAL_CONTENT;
    if !response.status().is_success() {
        return Err(Error::HttpStatus(response.status()));
    }
    let start = if resumed { offset } else { 0 };
    let total = content_length(&response).map(|remaining| start + remaining);

    let mut manifest = Manifest {
        url: url.to_string(),
        etag: response_header(&response, header::ETAG),
        last_modified: response_header(&response, header::LAST_MODIFIED),
        total_length: total,
        completed_ranges: Vec::new(),
    };
    manifest.set_contiguous(start);
    let mut part = PartFile::open(dest, start, manifest).await?;
    let mut progress = Progress::new(start, total);
    let _ = progress_tx.try_broadcast(progress);
    debug!(start, ?total, resumed, "Transfer started");

    let mut buffer = vec![0; 64 * 1024];
    loop {
        let stopped = stop.cancelled().fuse();
        let read = response.body_mut().read(&mut buffer).fuse();
        futures_util::pin_mut!(stopped, read);
        let read = select_biased! {
            _ = stopped => {
                warn!(?dest, "Stop requested; checkpointing partial download");
                part.checkpoint().await?;
                return Ok(WorkerResult::Stopped);
            }
            read = read => read,
        };

        match read {
            Ok(0) => break,
            Ok(read) => {
                part.write(&buffer[..read]).await?;
                if progress.add(read as u64) {
                    let _ = progress_tx.try_broadcast(progress);
                }
            }
            Err(error) => {
                let _ = part.checkpoint().await.log_warn();
                error!(%error, ?dest, "Transfer error; keeping partial for resume");
                return Err(Error::Network(error.to_string().into()));
            }
        }
    }

    progress.force_update();
    let _ = progress_tx.try_broadcast(progress);
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

/// Fetches each chunk in order and appends it (decompressing first, when
/// the chunk says it needs it) to the destination's `.part` file. No
/// resume support — a chunked download always restarts from scratch;
/// only the sequential-append/atomic-finalize machinery of [`PartFile`]
/// is reused, not the manifest-based resume path `run_simple` uses.
async fn run_chunked(
    request: &Request,
    chunks: &[ChunkSource],
    client: &dyn HttpClient,
    progress_tx: Sender<Progress>,
    stop: CancellationToken,
) -> Result<WorkerResult> {
    let dest = request.destination();

    let manifest = Manifest {
        url: format!("chunked:{}:{}", chunks.len(), dest.display()),
        etag: None,
        last_modified: None,
        total_length: None,
        completed_ranges: Vec::new(),
    };
    let mut part = PartFile::open(dest, 0, manifest).await?;
    let mut progress = Progress::new(0, None);
    let _ = progress_tx.try_broadcast(progress);
    debug!(chunks = chunks.len(), "Chunked transfer started");

    for chunk in chunks {
        let mut response = {
            let stopped = stop.cancelled().fuse();
            let response = send_get(request, &chunk.url, client, 0, None).fuse();
            futures_util::pin_mut!(stopped, response);
            select_biased! {
                _ = stopped => {
                    part.checkpoint().await?;
                    return Ok(WorkerResult::Stopped);
                }
                response = response => response?,
            }
        };
        if !response.status().is_success() {
            return Err(Error::HttpStatus(response.status()));
        }

        let mut raw = Vec::new();
        let mut buffer = vec![0; 64 * 1024];
        loop {
            let stopped = stop.cancelled().fuse();
            let read = response.body_mut().read(&mut buffer).fuse();
            futures_util::pin_mut!(stopped, read);
            let read = select_biased! {
                _ = stopped => {
                    warn!(?dest, "Stop requested; checkpointing partial download");
                    part.checkpoint().await?;
                    return Ok(WorkerResult::Stopped);
                }
                read = read => read,
            };

            match read {
                Ok(0) => break,
                Ok(read) => {
                    raw.extend_from_slice(&buffer[..read]);
                    if progress.add(read as u64) {
                        let _ = progress_tx.try_broadcast(progress);
                    }
                }
                Err(error) => {
                    let _ = part.checkpoint().await.log_warn();
                    error!(%error, ?dest, "Transfer error on chunk");
                    return Err(Error::Network(error.to_string().into()));
                }
            }
        }

        let bytes = if chunk.compressed {
            blocking::unblock(move || -> std::io::Result<Vec<u8>> {
                use std::io::Read;
                let mut decoder = flate2::read::ZlibDecoder::new(&raw[..]);
                let mut decompressed = Vec::new();
                decoder.read_to_end(&mut decompressed)?;
                Ok(decompressed)
            })
            .await?
        } else {
            raw
        };

        part.write(&bytes).await?;
    }

    progress.force_update();
    let _ = progress_tx.try_broadcast(progress);
    let path = part.finalize().await?;
    info!(
        ?path,
        bytes = progress.bytes_downloaded(),
        "Chunked download completed successfully"
    );

    Ok(WorkerResult::Finished(DownloadResult {
        path,
        bytes_downloaded: progress.bytes_downloaded(),
    }))
}

async fn send_get(
    request: &Request,
    url: &Url,
    client: &dyn HttpClient,
    offset: u64,
    prior: Option<&Manifest>,
) -> Result<Response<http_client::Body>> {
    let mut http_request = HttpRequest::get(url.as_str())
        .body(Vec::new())
        .map_err(|error| Error::InvalidRequest(error.to_string()))?;
    *http_request.headers_mut() = request.config.headers.clone();

    if offset > 0 {
        let value = format!("bytes={offset}-");
        http_request.headers_mut().insert(
            header::RANGE,
            value.parse().map_err(|source| Error::InvalidHeaderValue {
                value,
                source: Arc::new(source),
            })?,
        );
        if let Some(validator) = prior.and_then(Manifest::validator) {
            let value = validator.to_string();
            http_request.headers_mut().insert(
                header::IF_RANGE,
                value.parse().map_err(|source| Error::InvalidHeaderValue {
                    value,
                    source: Arc::new(source),
                })?,
            );
        }
    }

    client.send(http_request).await.map_err(Into::into)
}

fn content_length(response: &Response<http_client::Body>) -> Option<u64> {
    response
        .headers()
        .get(header::CONTENT_LENGTH)?
        .to_str()
        .ok()?
        .parse()
        .ok()
}

fn response_header(
    response: &Response<http_client::Body>,
    name: header::HeaderName,
) -> Option<String> {
    response
        .headers()
        .get(name)
        .and_then(|value| value.to_str().log_debug())
        .map(str::to_string)
}
