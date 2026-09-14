// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

use std::{collections::HashMap, num::ParseIntError, path::PathBuf, sync::LazyLock};

use eos_protobuf::eos::rpc::{
    ContainerInsertRequest, ContainerMdProto, FileInsertRequest, FileMdProto, InsertReply, MdId,
    MdRequest, MdResponse, NsStatRequest, Time, Type, eos_client::EosClient,
};
use tokio_stream::StreamExt;
use tonic::{service::interceptor::InterceptedService, transport::Channel};

use crate::{
    StreamResponseExt,
    rpc::{self, AuthorizationInterceptor, EndpointConfig},
};

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

pub static DEFAULT_FILE_MODE: LazyLock<Mode> = LazyLock::new(|| {
    (Mode::S_IRWXU | Mode::S_IRGRP | Mode::S_IWGRP | Mode::S_IROTH | Mode::S_IWOTH) &
        // Filemode: filter out S_ISUID, S_ISGID and S_ISVTX because EOS does not follow POSIX semantics for these bits
        !(Mode::S_ISUID | Mode::S_ISGID | Mode::S_ISVTX)
});

/// An Error coming from the EOS API client
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Disk instance '{0}' not found")]
    DiskInstanceNotFound(String),
    #[error("RPC Error: {0}")]
    Rpc(rpc::Error),
    #[error("RPC Error: {0}")]
    Tonic(tonic::Status),
    #[error("Error parsing '{0}': {1}")]
    ParseInt(String, ParseIntError),
    #[error("Invalid disk file id: {0}")]
    InvalidDiskFileId(u64),
    #[error("Unexpected return value: {0}")]
    UnexpectedReturnValue(String),
    #[error("Not found: {0:?}")]
    NotFound(MdId),
    #[error("Container not found")]
    ContainerNotFound(MdId),
    #[error("Root container reached")]
    RootContainerReached,
    #[error("Checksum missing in file '{0}'")]
    ChecksumMissing(String),
}

/// Auth function to get the system time as seconds since the epoch. Start of 1970 UTC
/// is the standard in every supported system.
pub fn system_time_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("Current time is lower than UNIX_EPOCH")
        .as_secs()
}

/// This struct encapsulates an EOS GRPC client, abstracting out details such as authentication
/// and streaming.
pub struct EosGrpcClient {
    _inner: EosClient<InterceptedService<Channel, rpc::AuthorizationInterceptor>>,
    authentication: rpc::JwtAuth,
}

impl EosGrpcClient {
    /// Build a client from a config structure
    pub async fn new(config: &EndpointConfig) -> Result<Self, rpc::Error> {
        let channel = config.build_channel().await?;
        let client = EosClient::new(InterceptedService::new(
            channel,
            AuthorizationInterceptor::new(config.authentication.clone()),
        ));
        Ok(Self {
            _inner: client,
            authentication: config.authentication.clone(),
        })
    }

    /// Get item metadata
    async fn get_metadata(&mut self, r#type: Type, id: MdId) -> Result<MdResponse, Error> {
        log::debug!("Retrieving EOS metadata for file {id:?}");

        let mut response_stream = self
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
            .map_err(Error::Tonic)?;

        let stream = response_stream.stream_response();

        let res = stream
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
    pub async fn get_container_metadata(&mut self, id: MdId) -> Result<ContainerMdProto, Error> {
        let md_resp = self.get_metadata(Type::Container, id.clone()).await?;
        match md_resp.cmd {
            Some(cmd) => {
                if cmd.id == 0 {
                    // Important: EOS reponds with id 0 when the item doesn't exist
                    Err(Error::ContainerNotFound(id))
                } else {
                    Ok(cmd)
                }
            }
            None => Err(Error::UnexpectedReturnValue(
                "No container metadata was returned".into(),
            )),
        }
    }

    /// Add a new container at a path, with a given storage class. Optionally create the parent container(s)
    /// if they don't exist
    pub async fn add_container(
        &mut self,
        path: &str,
        storage_class: &str,
        create_parents: bool,
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
            Err(Error::ContainerNotFound(_)) => {
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
            Err(Error::ContainerNotFound(md_id)) => {
                // if we're supposed to create the parent(s) container(s), let's go up the chain
                if create_parents {
                    log::info!(
                        "Parent container '{}' doesn't exist yet. Creating it.",
                        parent_path
                    );
                    // we have to pin the future, because of recursion
                    Box::pin(self.add_container(&parent_path, storage_class, create_parents))
                        .await?;
                } else {
                    // otherwise, fail already
                    return Err(Error::ContainerNotFound(md_id));
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
                // unwrap: there is always a name because we right-stripped '/'
                // and checked for a parent above
                .unwrap()
                .to_string_lossy()
                .to_string()
                .into(),
            mode: DEFAULT_FILE_MODE.bits(),
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

    /// Get a file's path from its disk_file_id
    pub async fn get_file_path_by_disk_id(
        &mut self,
        disk_file_id: &str,
    ) -> Result<Option<String>, Error> {
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

        Ok(String::from_utf8(md.path.clone())
            .unwrap_or_else(|_| panic!("Can't decode path: {:?}", md.path))
            .into())
    }

    /// Get a file's disk id from its path
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

    /// Check whether a file exists, given its path
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

pub struct EosEndpointMap {
    pub(crate) endpoint_map: HashMap<String, EndpointConfig>,
}

/// A macro to generate proxy methods for the EosEndpointMap struct.
macro_rules! endpoint_method {
    ($name:ident, ($($param:ident: $type:ty),*) => $ret:ty) => {
        pub async fn $name(
            &mut self,
            disk_instance: &str,
            $($param: $type),*
        ) -> Result<$ret, Error> {
            let mut endpoint = self
                .get_client(disk_instance)
                .await
                .ok_or(Error::DiskInstanceNotFound(disk_instance.into()))?;

            endpoint.$name($($param),*).await
        }
    };
}

impl EosEndpointMap {
    pub async fn get_client(&mut self, index: impl AsRef<str>) -> Option<EosGrpcClient> {
        let r = self.endpoint_map.get_mut(index.as_ref());
        match r {
            Some(c) => EosGrpcClient::new(c).await.ok(),
            None => todo!(),
        }
    }

    endpoint_method!(get_file_metadata, (id: MdId) => FileMdProto);
    endpoint_method!(get_file_path_by_disk_id, (disk_file_id: &str) => Option<String>);
    endpoint_method!(get_file_disk_id_by_path, (path: &str) => u64);
    endpoint_method!(get_current_ids, () => (u64, u64));
    endpoint_method!(check_file_exists_by_disk_id, (disk_file_id: &str) => bool);
}

impl From<HashMap<String, EndpointConfig>> for EosEndpointMap {
    fn from(endpoint_map: HashMap<String, EndpointConfig>) -> Self {
        EosEndpointMap { endpoint_map }
    }
}
