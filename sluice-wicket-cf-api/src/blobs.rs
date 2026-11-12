//! The packages and the droplets: blobs kept under one key each (`packages/<guid>.zip`,
//! `droplets/<sha256>.wasm`, a site's files and a build's droplet `droplets/<sha256>.tar`), in
//! the data directory (one instance's) or, with `--blobs`, in an S3 bucket several instances
//! share (`crate::s3`).
//!
//! A blob is made on the instance's own disk first (a [`Spool`]) and kept whole or not at
//! all: a rename in the directory, one put to the bucket. It is read as a stream, or from a
//! file: the directory's own, a copy of the bucket's.

use std::path::{Path, PathBuf};

use crate::s3::S3;

/// A blob, by what names it: a package's guid, a droplet's or its files' digest.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Blob {
    Package(String),
    Droplet(String),
    /// A static site's files or what buildpacks built: a tar.
    Files(String),
}

impl Blob {
    pub fn key(&self) -> String {
        match self {
            Blob::Package(guid) => format!("packages/{guid}.zip"),
            Blob::Droplet(sha256) => format!("droplets/{sha256}.wasm"),
            Blob::Files(sha256) => format!("droplets/{sha256}.tar"),
        }
    }

    /// What names it.
    pub fn name(&self) -> &str {
        match self {
            Blob::Package(name) | Blob::Droplet(name) | Blob::Files(name) => name,
        }
    }

    /// The blob kept under `key`; None: no blob's (a file being written, another's object).
    fn of(key: &str) -> Option<Blob> {
        let (kind, name) = key.split_once('/')?;
        let (name, extension) = name.rsplit_once('.')?;
        if name.is_empty() || name.contains(['/', '.']) {
            return None;
        }
        match (kind, extension) {
            ("packages", "zip") => Some(Blob::Package(name.to_string())),
            ("droplets", "wasm") => Some(Blob::Droplet(name.to_string())),
            ("droplets", "tar") => Some(Blob::Files(name.to_string())),
            _ => None,
        }
    }
}

/// The bucket of `--blobs`.
#[derive(Clone)]
pub struct BlobSettings {
    pub bucket: String,
    /// Before every key: none, or ending in `/`.
    pub prefix: String,
    /// None: AWS, by the region.
    pub endpoint: Option<String>,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    /// The PEM CA verifying the endpoint; None: the webPKI roots.
    pub ca_pem: Option<Vec<u8>>,
}

impl std::fmt::Debug for BlobSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BlobSettings")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("endpoint", &self.endpoint)
            .field("region", &self.region)
            .finish_non_exhaustive()
    }
}

/// A file of the instance's own disk, removed with this: an upload received, a blob being
/// made, a copy of one.
pub struct Spool(pub(crate) PathBuf);

impl Spool {
    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for Spool {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// A blob being read: its size and its bytes as they come, larger than this process's memory
/// as they may be.
pub struct Reading {
    pub bytes: u64,
    pub body: reqwest::Body,
}

impl Reading {
    pub async fn of(file: tokio::fs::File) -> std::io::Result<Reading> {
        Ok(Reading {
            bytes: file.metadata().await?.len(),
            body: file.into(),
        })
    }
}

/// A blob in a file: the directory's, or a copy gone with this.
pub enum Local {
    Kept(PathBuf),
    Copy(Spool),
}

impl Local {
    pub fn path(&self) -> &Path {
        match self {
            Local::Kept(path) => path,
            Local::Copy(spool) => spool.path(),
        }
    }
}

pub enum Blobs {
    /// The data directory.
    Dir(PathBuf),
    S3(S3),
}

impl Blobs {
    /// Several instances'.
    pub fn shared(&self) -> bool {
        matches!(self, Blobs::S3(_))
    }

    /// Keeps the file spooled, of the digest `sha256`, as `blob`: whole once this returns,
    /// in place of one kept before (of a digest, the same bytes).
    pub async fn put(&self, blob: &Blob, spool: Spool, sha256: &str) -> Result<(), String> {
        match self {
            Blobs::Dir(dir) => {
                let path = dir.join(blob.key());
                // on the disk once it returns: in a VM, a kill loses what its guest's page
                // cache held
                let renamed = tokio::task::spawn_blocking(move || {
                    std::fs::File::open(spool.path())
                        .and_then(|file| file.sync_all())
                        .and_then(|()| std::fs::rename(spool.path(), &path))
                        .and_then(|()| match path.parent() {
                            Some(dir) => std::fs::File::open(dir)?.sync_all(),
                            None => Ok(()),
                        })
                        .map_err(|e| format!("{}: {e}", path.display()))
                });
                renamed.await.map_err(|e| e.to_string())?
            }
            Blobs::S3(s3) => {
                let key = blob.key();
                // of a digest: the same bytes, sent once
                if !matches!(blob, Blob::Package(_)) && s3.has(&key).await? {
                    return Ok(());
                }
                s3.put(&key, spool.path(), sha256).await
            }
        }
    }

    pub async fn has(&self, blob: &Blob) -> Result<bool, String> {
        match self {
            Blobs::Dir(dir) => {
                let path = dir.join(blob.key());
                (tokio::fs::try_exists(&path).await).map_err(|e| format!("{}: {e}", path.display()))
            }
            Blobs::S3(s3) => s3.has(&blob.key()).await,
        }
    }

    /// The blob as a stream; None: it is not kept.
    pub async fn read(&self, blob: &Blob) -> Result<Option<Reading>, String> {
        match self {
            Blobs::Dir(dir) => {
                let path = dir.join(blob.key());
                let read = async { Reading::of(tokio::fs::File::open(&path).await?).await };
                match read.await {
                    Ok(reading) => Ok(Some(reading)),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
                    Err(e) => Err(format!("{}: {e}", path.display())),
                }
            }
            Blobs::S3(s3) => Ok(s3.get(&blob.key()).await?.map(|object| Reading {
                bytes: object.bytes,
                body: reqwest::Body::wrap_stream(object.response.bytes_stream()),
            })),
        }
    }

    /// The blob in a file, the bucket's copied to what `spool` gives; None: it is not kept.
    pub async fn local(
        &self,
        blob: &Blob,
        spool: impl FnOnce() -> Result<(Spool, std::fs::File), String>,
    ) -> Result<Option<Local>, String> {
        use tokio::io::AsyncWriteExt as _;
        let s3 = match self {
            Blobs::Dir(dir) => {
                let path = dir.join(blob.key());
                return match tokio::fs::try_exists(&path).await {
                    Ok(kept) => Ok(kept.then_some(Local::Kept(path))),
                    Err(e) => Err(format!("{}: {e}", path.display())),
                };
            }
            Blobs::S3(s3) => s3,
        };
        let key = blob.key();
        let Some(mut object) = s3.get(&key).await? else {
            return Ok(None);
        };
        let (spool, file) = spool()?;
        let mut file = tokio::fs::File::from_std(file);
        let unwritten = |e: std::io::Error| format!("{}: {e}", spool.path().display());
        let mut bytes = 0;
        while let Some(chunk) = object.response.chunk().await.transpose() {
            let chunk = chunk.map_err(|e| format!("s3: get {key}: {}", e.without_url()))?;
            bytes += chunk.len() as u64;
            file.write_all(&chunk).await.map_err(unwritten)?;
        }
        file.flush().await.map_err(unwritten)?;
        if bytes != object.bytes {
            return Err(format!("s3: get {key}: {bytes} of {} bytes", object.bytes));
        }
        Ok(Some(Local::Copy(spool)))
    }

    /// The blobs kept.
    pub async fn list(&self) -> Result<Vec<Blob>, String> {
        match self {
            Blobs::Dir(dir) => {
                let dir = dir.clone();
                let listed = tokio::task::spawn_blocking(move || {
                    let mut blobs = Vec::new();
                    for sub in ["packages", "droplets"] {
                        let entries = std::fs::read_dir(dir.join(sub))
                            .map_err(|e| format!("{}: {e}", dir.join(sub).display()))?;
                        for entry in entries.flatten() {
                            let name = entry.file_name();
                            let key = format!("{sub}/{}", name.to_string_lossy());
                            blobs.extend(Blob::of(&key));
                        }
                    }
                    Ok(blobs)
                });
                listed.await.map_err(|e| e.to_string())?
            }
            Blobs::S3(s3) => {
                let keys = s3.list().await?;
                Ok(keys.iter().filter_map(|key| Blob::of(key)).collect())
            }
        }
    }

    /// Deletes the blob; one gone already is deleted.
    pub async fn delete(&self, blob: &Blob) -> Result<(), String> {
        match self {
            Blobs::Dir(dir) => {
                let path = dir.join(blob.key());
                match tokio::fs::remove_file(&path).await {
                    Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
                        Err(format!("{}: {e}", path.display()))
                    }
                    _ => Ok(()),
                }
            }
            Blobs::S3(s3) => s3.delete(&blob.key()).await,
        }
    }
}
