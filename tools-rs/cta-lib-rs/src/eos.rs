// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Client library for the EOS namespace gRPC API.
//!
//! [`EosGrpcClient`] covers the subset of the EOS RPC service that CTA tools
//! need: querying file and container metadata, creating containers and
//! inserting file entries into the namespace.
//!
//! An [`EosEndpointMap`] registry maps a *disk instance* name to an
//! [`EndpointConfig`] and exposes the most common operations directly, opening
//! the connection on demand.
//!
//! ```no_run
//! # use std::collections::HashMap;
//! # use cta_lib::{eos::EosEndpointMap, rpc::EndpointConfig};
//! # async fn example(configs: HashMap<String, EndpointConfig>) -> Result<(), Box<dyn std::error::Error>> {
//! let mut endpoints = EosEndpointMap::from(configs);
//! let exists = endpoints
//!     .check_file_exists_by_disk_id("eosctatape", "1234")
//!     .await?;
//! # Ok(())
//! # }
//! ```

use std::{collections::HashMap, num::ParseIntError, path::PathBuf, sync::LazyLock};

use eos_protobuf::eos::rpc::{
    ContainerInsertRequest, ContainerMdProto, FileInsertRequest, FileMdProto, InsertReply, MdId,
    MdRequest, MdResponse, NsStatRequest, Time, Type, eos_client::EosClient,
};
use tokio_stream::StreamExt;
use tonic::{service::interceptor::InterceptedService, transport::Channel};

use crate::rpc::{self, AuthorizationInterceptor, EndpointConfig};

use nix::sys::stat::Mode;

/// The EOS API requires us to send the token as a GRPC protobuf field, rather
/// than the more standard request header. This helper macro makes it less verbose.
macro_rules! with_auth_key_from {
    ($auth:expr, $req:ident { $($field:ident: $value:expr),*$(,)? }) => {
        $req {
            authkey: String::from_utf8_lossy(&($auth.token)).to_string(),
            $($field: $value,)*
        }
    };
}

/// Permission bits applied to namespace entries created by this crate:
/// `rwx` for the owner, `rw` for group and others
pub static DEFAULT_FILE_MODE: LazyLock<Mode> = LazyLock::new(|| {
    (Mode::S_IRWXU | Mode::S_IRGRP | Mode::S_IWGRP | Mode::S_IROTH | Mode::S_IWOTH) &
        // Filemode: filter out S_ISUID, S_ISGID and S_ISVTX because EOS does not follow POSIX semantics for these bits
        !(Mode::S_ISUID | Mode::S_ISGID | Mode::S_ISVTX)
});

/// An Error coming from the EOS API client
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// No endpoint is registered for the requested disk instance in the
    /// [`EosEndpointMap`].
    #[error("Disk instance '{0}' not found")]
    DiskInstanceNotFound(String),
    /// The connection to the EOS endpoint could not be established.
    #[error("gRPC Connection Error: {0}")]
    Rpc(rpc::Error),
    /// EOS returned an error status for the call.
    #[error("gRPC Status Error: {0}")]
    Tonic(tonic::Status),
    /// A numeric identifier could not be parsed from its string form.
    #[error("Error parsing '{0}': {1}")]
    ParseInt(String, ParseIntError),
    /// The disk file id is not usable (EOS reserves the value `0`).
    #[error("Invalid disk file id: {0}")]
    InvalidDiskFileId(u64),
    /// EOS answered successfully but with a payload that violates the
    /// expectations of the caller (missing metadata, too many results, …).
    #[error("Unexpected return value: {0}")]
    UnexpectedReturnValue(String),
    /// No namespace entry exists for the queried identifier.
    #[error("Not found: {0:?}")]
    NotFound(MdId),
    /// Walking up the parent chain reached the namespace root, so there is no
    /// parent container left to create.
    #[error("Root container reached")]
    RootContainerReached,
    /// Invalid file/container path
    #[error("Invalid path: {0}")]
    InvalidPath(String),
}

/// Auth function to get the system time as seconds since the epoch. Start of 1970 UTC
/// is the standard in every supported system.
///
/// # Panics
///
/// Panics if the system clock is set before the UNIX epoch.
pub fn system_time_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("Current time is lower than UNIX_EPOCH")
        .as_secs()
}

/// This struct encapsulates an EOS GRPC client, abstracting out details such as authentication
/// and streaming.
#[derive(Debug)]
pub struct EosGrpcClient {
    _inner: EosClient<InterceptedService<Channel, rpc::AuthorizationInterceptor>>,
    authentication: rpc::JwtAuth,
}

impl EosGrpcClient {
    /// Build a client from a config structure
    ///
    /// # Errors
    ///
    /// Propagates the connection errors of [`EndpointConfig::build_channel`]
    /// and the token errors of [`AuthorizationInterceptor::new`].
    pub async fn new(config: &EndpointConfig) -> Result<Self, rpc::Error> {
        let channel = config.build_channel().await?;
        let client = EosClient::new(InterceptedService::new(
            channel,
            AuthorizationInterceptor::new(config.authentication.clone())?,
        ));
        Ok(Self {
            _inner: client,
            authentication: config.authentication.clone(),
        })
    }

    /// Get item metadata
    ///
    /// Issues an `md` query and collapses the response stream into the single
    /// result the caller expects.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] when the stream is empty and
    /// [`Error::UnexpectedReturnValue`] when it holds more than one item.
    async fn get_metadata(&mut self, r#type: Type, id: MdId) -> Result<MdResponse, Error> {
        log::debug!("Retrieving EOS metadata for file {id:?}");

        let response_stream = self
            ._inner
            .md(with_auth_key_from!(
                self.authentication,
                MdRequest {
                    r#type: r#type.into(),
                    id: Some(id.clone()),
                    role: None,
                    selection: None,
                }
            ))
            .await
            .map_err(Error::Tonic)?
            .into_inner();

        let res = response_stream
            .collect::<Result<Vec<_>, _>>()
            .await
            .map_err(Error::Tonic)?;

        match res.len() {
            0 => Err(Error::NotFound(id)),
            1 => Ok(res[0].clone()),
            2.. => Err(Error::UnexpectedReturnValue(format!(
                "The operation should have returned at most 1 result but returned {}",
                res.len()
            ))),
        }
    }

    /// Get all metadata regarding a file
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the file does not exist
    pub async fn get_file_metadata(&mut self, id: MdId) -> Result<FileMdProto, Error> {
        let md_resp = self.get_metadata(Type::File, id.clone()).await?;
        match md_resp.fmd {
            Some(fmd) => {
                if fmd.id == 0 {
                    // Important: EOS responds with id 0 when the item doesn't exist
                    Err(Error::NotFound(id))
                } else {
                    Ok(fmd)
                }
            }
            None => Err(Error::UnexpectedReturnValue(
                "No file metadata was returned".into(),
            )),
        }
    }

    /// Get all metadata regarding a container
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the container does not exist.
    pub async fn get_container_metadata(&mut self, id: MdId) -> Result<ContainerMdProto, Error> {
        let md_resp = self.get_metadata(Type::Container, id.clone()).await?;
        match md_resp.cmd {
            Some(cmd) => {
                if cmd.id == 0 {
                    // Important: EOS reponds with id 0 when the item doesn't exist
                    Err(Error::NotFound(id))
                } else {
                    Ok(cmd)
                }
            }
            None => Err(Error::UnexpectedReturnValue(
                "No container metadata was returned".into(),
            )),
        }
    }

    /// Add a new container at a path, with a given storage class. Optionally create the parent
    /// container(s) if they don't exist
    ///
    /// The operation is idempotent: if a container already exists at `path`,
    /// its id is returned unchanged. `file_mode` defaults to 766 ([`DEFAULT_FILE_MODE`]).
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if the parent does not exist and
    /// `create_parents` is `false`, and [`Error::RootContainerReached`] if
    /// `path` has no parent at all.
    pub async fn add_container(
        &mut self,
        path: &str,
        storage_class: &str,
        create_parents: bool,
        file_mode: Option<Mode>,
    ) -> Result<u64, Error> {
        match self.get_container_disk_id_by_path(path).await {
            Ok(c_id) => {
                return {
                    log::info!(
                        "Container with path '{}' already exists (id: {})",
                        path,
                        c_id
                    );
                    Ok(c_id)
                };
            }
            Err(Error::NotFound(_)) => {
                // there isn't a container with that ID yet, proceed
            }
            Err(e) => return Err(e),
        };

        log::info!("Adding container with path '{path}'");

        let current_path = PathBuf::from(path.strip_suffix("/").unwrap_or(path));
        let parent = current_path.parent();

        let parent_path = match parent {
            None => return Err(Error::RootContainerReached),
            Some(path) => path,
        }
        .to_string_lossy()
        .to_string();

        // the parent container should exist. if not, we'll add it
        match self.get_container_disk_id_by_path(&parent_path).await {
            Ok(_) => {
                // there is already a parent container. do nothing.
            }
            Err(Error::NotFound(md_id)) => {
                // if we're supposed to create the parent(s) container(s), let's go up the chain
                if create_parents {
                    log::info!(
                        "Parent container '{}' doesn't exist yet. Creating it.",
                        parent_path
                    );
                    // we have to pin the future, because of recursion
                    Box::pin(self.add_container(
                        &parent_path,
                        storage_class,
                        create_parents,
                        file_mode,
                    ))
                    .await?;
                } else {
                    // otherwise, fail already
                    return Err(Error::NotFound(md_id));
                }
            }
            Err(e) => return Err(e),
        };

        let secs_since_epoch: u64 = system_time_now();

        // now let's create the actual target container
        let mut dir = ContainerMdProto {
            path: path.into(),
            name: current_path
                .file_name()
                .ok_or(Error::InvalidPath(path.into()))?
                .to_string_lossy()
                .to_string()
                .into(),
            mode: file_mode.unwrap_or(*DEFAULT_FILE_MODE).bits(),
            ctime: Some(Time {
                sec: secs_since_epoch,
                n_sec: 0,
            }),
            mtime: Some(Time {
                sec: secs_since_epoch,
                n_sec: 0,
            }),
            ..Default::default()
        };

        dir.xattrs
            .insert("sys.archive.storage_class".into(), storage_class.into());

        let res = self
            ._inner
            .container_insert(with_auth_key_from!(
                self.authentication,
                ContainerInsertRequest {
                    container: vec![dir],
                    inherit_md: false,
                }
            ))
            .await
            .map_err(Error::Tonic)?;

        let reply = res.into_inner();

        log::debug!("InsertReply: {reply:#?}");

        log::debug!("Confirming container's existence after creation ({path})");

        match self.get_container_disk_id_by_path(path).await {
            Ok(c_id) => {
                log::info!(
                    "Container with id '{}' and path '{}' confirmed to exist",
                    c_id,
                    path
                );
                Ok(c_id)
            }
            Err(e) => Err(e),
        }
    }

    /// Get a file's path from its `disk_file_id`
    ///
    /// # Errors
    ///
    /// Returns [`Error::ParseInt`] if `disk_file_id` is not a number,
    /// [`Error::InvalidDiskFileId`] if it is `0` and [`Error::NotFound`] if no
    /// such file exists.
    pub async fn get_file_path_by_disk_id(&mut self, disk_file_id: &str) -> Result<String, Error> {
        let int_id = disk_file_id
            .parse::<u64>()
            .map_err(|e| Error::ParseInt(disk_file_id.into(), e))?;

        if int_id == 0 {
            return Err(Error::InvalidDiskFileId(int_id));
        }

        let id = MdId {
            r#type: Type::File.into(),
            id: int_id,
            path: "".into(),
            ino: 0,
        };

        let md = self.get_file_metadata(id).await?;

        Ok(String::from_utf8_lossy(&md.path.clone()).to_string())
    }

    /// Get a file's disk id from its path
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if no file exists at `file_path`.
    pub async fn get_file_disk_id_by_path(&mut self, file_path: &str) -> Result<u64, Error> {
        let id = MdId {
            r#type: Type::File.into(),
            id: 0,
            path: file_path.into(),
            ino: 0,
        };

        let md = self.get_file_metadata(id).await?;
        Ok(md.id)
    }

    /// Get a container's disk id from its path
    ///
    /// # Errors
    ///
    /// Returns [`Error::NotFound`] if no container exists at `container_path`.
    pub async fn get_container_disk_id_by_path(
        &mut self,
        container_path: &str,
    ) -> Result<u64, Error> {
        let id = MdId {
            r#type: Type::Container.into(),
            id: 0,
            path: container_path.into(),
            ino: 0,
        };

        let md = self.get_container_metadata(id).await?;
        Ok(md.id)
    }

    /// Check whether a file exists, given its disk file id
    ///
    /// # Errors
    ///
    /// Fails for any error other than "not found", e.g. an unparsable
    /// `disk_file_id` or a failing RPC.
    pub async fn check_file_exists_by_disk_id(
        &mut self,
        disk_file_id: &str,
    ) -> Result<bool, Error> {
        match self.get_file_path_by_disk_id(disk_file_id).await {
            Ok(_) => Ok(true),
            Err(Error::NotFound(_)) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Get the current ID counters for containers and files, respectively
    ///
    /// # Errors
    ///
    /// Returns [`Error::Tonic`] if the `ns_stat` call fails.
    pub async fn get_current_ids(&mut self) -> Result<(u64, u64), Error> {
        let res = self
            ._inner
            .ns_stat(with_auth_key_from!(self.authentication, NsStatRequest {}))
            .await
            .map_err(Error::Tonic)?;

        let res = res.into_inner();

        Ok((res.current_cid, res.current_fid))
    }

    /// Insert new files into the EOS namespace
    ///
    /// The reply reports the per-file outcome; a successful return value only
    /// means the RPC itself succeeded.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Tonic`] if the `file_insert` call fails.
    pub async fn insert_files(&mut self, files: &[FileMdProto]) -> Result<InsertReply, Error> {
        let resp = self
            ._inner
            .file_insert(with_auth_key_from!(
                self.authentication,
                FileInsertRequest {
                    files: files.to_vec()
                }
            ))
            .await
            .map_err(Error::Tonic)?;
        Ok(resp.into_inner())
    }
}

/// A registry of EOS endpoints, keyed by CTA disk instance name.
/// Note that each call opens a fresh connection; connections are not pooled.
pub struct EosEndpointMap {
    pub(crate) endpoint_map: HashMap<String, EndpointConfig>,
}

/// A macro to generate proxy methods for the `EosEndpointMap` struct.
///
/// Each generated method resolves the disk instance to a client and delegates
/// to the [`EosGrpcClient`] method of the same name.
macro_rules! endpoint_method {
    ($name:ident, ($($param:ident: $type:ty),*) => $ret:ty) => {
        /// Resolves `disk_instance` to an [`EosGrpcClient`] and forwards the
        /// call to its equally named method.
        ///
        /// # Errors
        ///
        /// Returns [`Error::DiskInstanceNotFound`] if the instance is unknown or
        /// [`Error::Rpc`] if the connection cannot be established. Returns any
        /// error of the underlying client method.
        pub async fn $name(
            &mut self,
            disk_instance: &str,
            $($param: $type),*
        ) -> Result<$ret, Error> {
            let mut endpoint = self.get_client(disk_instance).await?;
            endpoint.$name($($param),*).await
        }
    };
}

impl EosEndpointMap {
    /// Opens a new connection to the endpoint registered for `index`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::DiskInstanceNotFound`] if `index` is not in the map.
    /// Returns [`Error::Rpc`] if the instance is registered but the connection
    /// cannot be established.
    pub async fn get_client(&mut self, index: impl AsRef<str>) -> Result<EosGrpcClient, Error> {
        let config = self
            .endpoint_map
            .get_mut(index.as_ref())
            .ok_or_else(|| Error::DiskInstanceNotFound(index.as_ref().to_string()))?;
        EosGrpcClient::new(config).await.map_err(Error::Rpc)
    }

    endpoint_method!(get_file_metadata, (id: MdId) => FileMdProto);
    endpoint_method!(get_file_path_by_disk_id, (disk_file_id: &str) => String);
    endpoint_method!(get_file_disk_id_by_path, (path: &str) => u64);
    endpoint_method!(get_current_ids, () => (u64, u64));
    endpoint_method!(check_file_exists_by_disk_id, (disk_file_id: &str) => bool);
    endpoint_method!(get_container_metadata, (id: MdId) => ContainerMdProto);
    endpoint_method!(get_container_disk_id_by_path, (path: &str) => u64);
    endpoint_method!(add_container, (path: &str, storage_class: &str, create_parents: bool, file_mode: Option<Mode>) => u64);
    endpoint_method!(insert_files, (files: &[FileMdProto]) => InsertReply);
}

impl From<HashMap<String, EndpointConfig>> for EosEndpointMap {
    fn from(endpoint_map: HashMap<String, EndpointConfig>) -> Self {
        EosEndpointMap { endpoint_map }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rpc::JwtAuth;
    use url::Url;

    fn endpoint_config() -> EndpointConfig {
        EndpointConfig::new(
            Url::parse("http://127.0.0.1:1").expect("test endpoint should parse"),
            JwtAuth::new(b"token".to_vec()),
            None,
            None,
        )
    }

    fn endpoint_map(instances: &[&str]) -> EosEndpointMap {
        EosEndpointMap::from(
            instances
                .iter()
                .map(|name| ((*name).to_string(), endpoint_config()))
                .collect::<HashMap<_, _>>(),
        )
    }

    fn file_id(id: u64) -> MdId {
        MdId {
            r#type: Type::File.into(),
            id,
            path: Vec::new(),
            ino: 0,
        }
    }

    #[test]
    fn default_file_mode_grants_the_expected_permissions() {
        let mode = *DEFAULT_FILE_MODE;

        for granted in [
            Mode::S_IRUSR,
            Mode::S_IWUSR,
            Mode::S_IXUSR,
            Mode::S_IRGRP,
            Mode::S_IWGRP,
            Mode::S_IROTH,
            Mode::S_IWOTH,
        ] {
            assert!(mode.contains(granted), "{granted:?} should be set");
        }

        // EOS does not follow POSIX semantics for these, so they must be clear.
        for cleared in [Mode::S_ISUID, Mode::S_ISGID, Mode::S_ISVTX] {
            assert!(!mode.intersects(cleared), "{cleared:?} should be cleared");
        }

        assert_eq!(mode.bits(), 0o766, "rwxrw-rw-");
    }

    #[test]
    fn system_time_now_returns_a_plausible_unix_timestamp() {
        let now = system_time_now();

        // 2026-01-01T00:00:00Z; the clock of a machine running CTA is past that.
        assert!(now > 1_767_225_600, "{now} should be a recent timestamp");
    }

    #[test]
    fn auth_key_macro_fills_in_the_token() {
        let auth = JwtAuth::new(b"a.token.value".to_vec());

        let request = with_auth_key_from!(
            auth,
            MdRequest {
                r#type: Type::File.into(),
                id: Some(file_id(42)),
                role: None,
                selection: None,
            }
        );

        assert_eq!(request.authkey, "a.token.value");
        assert_eq!(request.id.expect("id should be set").id, 42);
    }

    #[tokio::test]
    async fn get_client_returns_disk_instance_not_found_for_an_unknown_instance() {
        let mut map = endpoint_map(&["eosctatape"]);

        match map.get_client("does-not-exist").await {
            Err(Error::DiskInstanceNotFound(instance)) => assert_eq!(instance, "does-not-exist"),
            other => panic!("expected DiskInstanceNotFound, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn proxy_methods_report_unknown_disk_instance() {
        let mut map = endpoint_map(&["eosctatape"]);

        let error = map
            .get_current_ids("does-not-exist")
            .await
            .expect_err("unknown disk instance should be an error");

        assert!(matches!(error, Error::DiskInstanceNotFound(ref i) if i == "does-not-exist"));
    }

    #[tokio::test]
    async fn connection_failure_returns_rpc_error() {
        let mut map = endpoint_map(&["eosctatape"]);

        let error = map
            .get_current_ids("eosctatape")
            .await
            .expect_err("unreachable endpoint should error");

        assert!(
            matches!(error, Error::Rpc(_)),
            "expected Rpc error, got {error:?}"
        );
    }
}
