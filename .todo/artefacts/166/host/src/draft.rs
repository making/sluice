//! wasi:keyvalue@0.2.0-draft, the version `wasmtime serve -Skeyvalue` links, over the
//! same `Backend`: u64 cursor (an offset here), u64 increment, no cas.

use wasmtime::component::Resource;

use super::{Bucket, Ctx};

pub mod bindings {
    wasmtime::component::bindgen!({
        path: "../wit/keyvalue-draft",
        world: "wasi:keyvalue/imports",
        imports: { default: async | trappable },
        with: {
            "wasi:keyvalue/store.bucket": crate::Bucket,
        },
    });
}

use bindings::wasi::keyvalue::{atomics, batch, store};

fn other(e: String) -> store::Error {
    store::Error::Other(e)
}

impl store::Host for Ctx {
    async fn open(
        &mut self,
        identifier: String,
    ) -> wasmtime::Result<Result<Resource<Bucket>, store::Error>> {
        let Some((backend, namespace)) = self.buckets.get(&identifier).cloned() else {
            return Ok(Err(store::Error::NoSuchStore));
        };
        Ok(Ok(self.table.push(Bucket { backend, namespace })?))
    }
}

impl store::HostBucket for Ctx {
    async fn get(
        &mut self,
        bucket: Resource<Bucket>,
        key: String,
    ) -> wasmtime::Result<Result<Option<Vec<u8>>, store::Error>> {
        let (backend, ns) = self.bucket(&bucket)?;
        Ok(backend.get(&ns, &key).await.map_err(other))
    }
    async fn set(
        &mut self,
        bucket: Resource<Bucket>,
        key: String,
        value: Vec<u8>,
    ) -> wasmtime::Result<Result<(), store::Error>> {
        let (backend, ns) = self.bucket(&bucket)?;
        Ok(backend.set(&ns, &key, value).await.map_err(other))
    }
    async fn delete(
        &mut self,
        bucket: Resource<Bucket>,
        key: String,
    ) -> wasmtime::Result<Result<(), store::Error>> {
        let (backend, ns) = self.bucket(&bucket)?;
        Ok(backend.delete(&ns, &key).await.map_err(other))
    }
    async fn exists(
        &mut self,
        bucket: Resource<Bucket>,
        key: String,
    ) -> wasmtime::Result<Result<bool, store::Error>> {
        let (backend, ns) = self.bucket(&bucket)?;
        Ok(backend.exists(&ns, &key).await.map_err(other))
    }
    /// The u64 cursor is the count of keys already returned: the backend's
    /// pages are walked up to it (the spike's simplest mapping).
    async fn list_keys(
        &mut self,
        bucket: Resource<Bucket>,
        cursor: Option<u64>,
    ) -> wasmtime::Result<Result<store::KeyResponse, store::Error>> {
        let (backend, ns) = self.bucket(&bucket)?;
        let offset = cursor.unwrap_or(0) as usize;
        let (mut skipped, mut next) = (0usize, None);
        loop {
            let (keys, after) = match backend.list_keys(&ns, next).await {
                Ok(page) => page,
                Err(e) => return Ok(Err(other(e))),
            };
            if skipped + keys.len() > offset || after.is_none() {
                let page: Vec<String> = keys.into_iter().skip(offset - skipped.min(offset)).collect();
                let cursor = after.map(|_| (offset + page.len()) as u64);
                return Ok(Ok(store::KeyResponse { keys: page, cursor }));
            }
            skipped += keys.len();
            next = after;
        }
    }
    async fn drop(&mut self, bucket: Resource<Bucket>) -> wasmtime::Result<()> {
        self.table.delete(bucket)?;
        Ok(())
    }
}

impl atomics::Host for Ctx {
    async fn increment(
        &mut self,
        bucket: Resource<Bucket>,
        key: String,
        delta: u64,
    ) -> wasmtime::Result<Result<u64, store::Error>> {
        let (backend, ns) = self.bucket(&bucket)?;
        let Ok(delta) = i64::try_from(delta) else {
            return Ok(Err(other("delta exceeds i64".into())));
        };
        Ok(match backend.increment(&ns, &key, delta).await {
            Ok(value) => u64::try_from(value).map_err(|_| other("negative counter".into())),
            Err(e) => Err(other(e)),
        })
    }
}

impl batch::Host for Ctx {
    async fn get_many(
        &mut self,
        bucket: Resource<Bucket>,
        keys: Vec<String>,
    ) -> wasmtime::Result<Result<Vec<Option<(String, Vec<u8>)>>, store::Error>> {
        let (backend, ns) = self.bucket(&bucket)?;
        Ok(backend
            .get_many(&ns, keys)
            .await
            .map(|pairs| pairs.into_iter().map(|(k, v)| v.map(|v| (k, v))).collect())
            .map_err(other))
    }
    async fn set_many(
        &mut self,
        bucket: Resource<Bucket>,
        key_values: Vec<(String, Vec<u8>)>,
    ) -> wasmtime::Result<Result<(), store::Error>> {
        let (backend, ns) = self.bucket(&bucket)?;
        for (key, value) in key_values {
            if let Err(e) = backend.set(&ns, &key, value).await {
                return Ok(Err(other(e)));
            }
        }
        Ok(Ok(()))
    }
    async fn delete_many(
        &mut self,
        bucket: Resource<Bucket>,
        keys: Vec<String>,
    ) -> wasmtime::Result<Result<(), store::Error>> {
        let (backend, ns) = self.bucket(&bucket)?;
        for key in keys {
            if let Err(e) = backend.delete(&ns, &key).await {
                return Ok(Err(other(e)));
            }
        }
        Ok(Ok(()))
    }
}
