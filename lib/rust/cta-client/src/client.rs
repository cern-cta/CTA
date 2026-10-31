// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Client for the CTA frontend's admin gRPC interface.
//!
//! The frontend exposes admin commands through two services: a unary one
//! (`CtaRpc`, one aggregated [`Response`]) and a streaming one
//! (`CtaRpcStream`, a sequence of [`StreamResponse`] frames). Both are wrapped
//! by [`CtaGrpcClient`]
//!
//! ```no_run
//! # use cta_client::client::CtaGrpcClient;
//! # use cta_grpc_common::EndpointConfig;
//! # async fn example(config: EndpointConfig) -> Result<(), Box<dyn std::error::Error>> {
//! let unary = CtaGrpcClient::new_unary(&config).await?;
//! let streaming = CtaGrpcClient::new_streaming(&config).await?;
//! # Ok(())
//! # }
//! ```

use cta_grpc_common::{AuthorizationInterceptor, EndpointConfig, Error as RpcError};
use cta_protobuf::cta::{
    admin::AdminCmd,
    xrd::{
        Request, Response, StreamResponse, cta_rpc_client::CtaRpcClient,
        cta_rpc_stream_client::CtaRpcStreamClient, data::Data, request::Request as RequestType,
        response::ResponseType,
    },
};
use tokio_stream::{Stream, StreamExt};
use tonic::{service::interceptor::InterceptedService, transport::Channel};

use crate::{
    admin_cmd,
    errors::{Error, TapeFileLsItemConversionError},
    stream::StreamResponseExt,
    types::{File, FileSelector},
};

/// An authenticated client for the CTA frontend.
/// Construct with [`CtaGrpcClient::new_unary`] or [`CtaGrpcClient::new_streaming`].
pub struct CtaGrpcClient<C> {
    inner: C,
}

/// The generated unary client
pub type SyncClientType = CtaRpcClient<InterceptedService<Channel, AuthorizationInterceptor>>;
/// The generated streaming client
pub type StreamingClientType =
    CtaRpcStreamClient<InterceptedService<Channel, AuthorizationInterceptor>>;

impl CtaGrpcClient<SyncClientType> {
    /// Connects to the unary admin service described by `config`.
    ///
    /// # Errors
    ///
    /// Propagates the connection errors of [`EndpointConfig::build_channel`]
    /// and the token errors of [`AuthorizationInterceptor::new`].
    pub async fn new_unary(config: &EndpointConfig) -> Result<Self, Error> {
        let channel = config.build_channel().await?;
        let client = CtaRpcClient::new(InterceptedService::new(
            channel,
            AuthorizationInterceptor::new(config.authentication.clone())?,
        ));
        Ok(Self { inner: client })
    }

    /// Sends an admin command and returns the single aggregated response.
    ///
    /// Note that a successful return value only means the call itself
    /// succeeded: callers must still inspect `Response::type` to find out
    /// whether CTA accepted the command.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Rpc`] if the RPC fails.
    pub async fn raw_admin_cmd(
        &mut self,
        cmd: AdminCmd,
    ) -> Result<tonic::Response<Response>, Error> {
        self.inner
            .admin(Request {
                request: Some(RequestType::Admincmd(Box::new(cmd))),
            })
            .await
            .map_err(|e| Error::Rpc(RpcError::Status(e)))
    }

    /// Restores a single tape file copy in the CTA catalogue
    /// (`recycletapefile restore`).
    ///
    /// The entry is identified by the tape volume, disk instance, archive file
    /// id and copy number of `file`. `file.disk_file.id` is submitted as the
    /// disk file id to attach the restored copy to, so callers that recreated
    /// the EOS entry must update that field to the new id first.
    ///
    /// # Errors
    ///
    /// Fails if the RPC fails, or if the frontend answers with anything other
    /// than a success response, in which case the frontend's message is used
    /// as the error message.
    pub async fn restore_deleted_file_copy(&mut self, file: &File) -> Result<(), Error> {
        log::info!(
            "Restoring file copy in CTA catalogue: {}",
            file.disk_file.path
        );

        let cmd = admin_cmd!(
            Recycletapefile.SubcmdRestore {
                Vid: str => file.tape_file.vid,
                Instance: str => file.disk_file.instance,
                DiskFileId: str => file.disk_file.id,
                ArchiveFileId: u64 => file.archive_file.id,
                CopyNumber: u64 => file.tape_file.copy_nb,
            }
        );

        let res = self.raw_admin_cmd(cmd).await?;
        let res = res.into_inner();

        log::info!("Restored file copy in CTA catalogue: {}", res.message_txt);

        match res.r#type() {
            ResponseType::RspSuccess => Ok(()),
            ResponseType::RspInvalid
            | ResponseType::RspErrProtobuf
            | ResponseType::RspErrCta
            | ResponseType::RspErrUser => Err(Error::UnexpectedResponseType(res.r#type())),
        }
    }
}

impl CtaGrpcClient<StreamingClientType> {
    /// Connects to the streaming admin service described by `config`.
    ///
    /// # Errors
    ///
    /// Propagates the connection errors of [`EndpointConfig::build_channel`]
    /// and the token errors of [`AuthorizationInterceptor::new`].
    pub async fn new_streaming(config: &EndpointConfig) -> Result<Self, Error> {
        let channel = config.build_channel().await.map_err(Error::Rpc)?;
        let client = CtaRpcStreamClient::new(InterceptedService::new(
            channel,
            AuthorizationInterceptor::new(config.authentication.clone()).map_err(Error::Rpc)?,
        ));
        Ok(Self { inner: client })
    }

    /// Sends an admin command and returns the raw response stream.
    ///
    /// Prefer consuming the result through
    /// [`StreamResponseExt::stream_response`],
    /// which validates the stream header and yields the payload items directly.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Rpc`] if the RPC fails.
    pub async fn raw_admin_cmd(
        &mut self,
        cmd: AdminCmd,
    ) -> Result<tonic::Streaming<StreamResponse>, Error> {
        self.inner
            .generic_admin_stream(Request {
                request: Some(RequestType::Admincmd(Box::new(cmd))),
            })
            .await
            .map_err(|e| Error::Rpc(RpcError::Status(e)))
            .map(|r| r.into_inner())
    }

    /// Retrieves the file which matches the given `archive_file_id`
    ///
    /// # Errors
    ///
    /// Returns [`Error::UnexpectedStreamData`] if the returned data items do not match
    /// the expectations or are not expected at all. [`Error::Rpc`]
    /// can also be returned.
    pub async fn get_file(&mut self, archive_file_id: u64) -> Result<File, Error> {
        let cmd = admin_cmd!(Tapefile.SubcmdLs {
            ArchiveFileId: u64 => archive_file_id
        });

        let mut res = self.raw_admin_cmd(cmd).await?;
        let mut stream = res.stream_response();

        match stream.next().await {
            Some(Ok(Data::TflsItem(ls_item))) => {
                log::info!("Retrieved tape file information: {:?}", ls_item);

                // Sanity check: make sure we received exactly one result
                // and that the stream does not contain unexpected additional items
                // or errors after the first result.
                match stream.next().await {
                    Some(Ok(next)) => Err(Error::UnexpectedStreamData(Box::new(next))),
                    Some(Err(e)) => Err(e),
                    None => {
                        let file = File::try_from(ls_item).map_err(
                            |e: TapeFileLsItemConversionError| {
                                Error::InvalidResponse(e.to_string())
                            },
                        )?;
                        Ok(file)
                    }
                }
            }
            Some(Ok(data)) => Err(Error::UnexpectedStreamData(Box::new(data))),
            Some(Err(e)) => Err(e),
            None => Err(Error::NotFound("File not found".into())),
        }
    }

    /// Queries the tape file recycle bin (`recycletapefile ls`).
    ///
    /// All filter arguments are optional and are combined by the frontend;
    /// passing none of them lists the whole recycle bin. The matching records
    /// are collected and returned to the caller.
    ///
    /// # Errors
    ///
    /// Fails if the connection cannot be established, if the frontend reports
    /// an error for the command, if the stream contains an unexpected item
    /// type, or if the protobuf response data is malformed (e.g., invalid
    /// timestamps or checksums).
    pub async fn list_deleted_files(
        &mut self,
        file_selector: FileSelector,
    ) -> Result<impl Stream<Item = Result<File, Error>> + use<>, Error> {
        let cmd = admin_cmd! (
            Recycletapefile.SubcmdLs {
                Vid: str? => file_selector.vid,
                Instance: str? => file_selector.disk_instance,
                ArchiveFileId: u64? => file_selector.archive_file_id,
                CopyNumber: u64? => file_selector.copy_number,
                FileId: str_list? => file_selector.file_ids,
        });

        // Execute command and get stream
        let response_stream = self.raw_admin_cmd(cmd).await?;

        Ok(response_stream
            .into_stream_response()
            .map(|item| match item {
                Ok(Data::RtflsItem(ls_item)) => {
                    File::try_from(ls_item).map_err(|e| Error::InvalidResponse(e.to_string()))
                }
                Ok(other) => Err(Error::UnexpectedStreamData(Box::new(other))),
                Err(e) => Err(e),
            }))
    }
}

#[cfg(test)]
mod tests {
    use cta_grpc_common::JwtAuth;
    use url::Url;

    use super::*;

    /// An endpoint on the loopback interface where nothing is ever listening.
    fn unreachable_config() -> EndpointConfig {
        EndpointConfig::new(
            Url::parse("http://127.0.0.1:1").expect("test endpoint should parse"),
            JwtAuth::new(b"a.token.value".to_vec()).unwrap(),
            None,
            None,
        )
    }

    #[tokio::test]
    async fn new_unary_fails_on_an_unreachable_frontend() {
        let error = CtaGrpcClient::new_unary(&unreachable_config())
            .await
            .err()
            .expect("connecting to a closed port should fail");

        assert!(
            matches!(error, Error::Rpc(_)),
            "expected a Transport error, got {error:?}"
        );
    }

    #[tokio::test]
    async fn new_streaming_fails_on_an_unreachable_frontend() {
        let error = CtaGrpcClient::new_streaming(&unreachable_config())
            .await
            .err()
            .expect("connecting to a closed port should fail");

        assert!(
            matches!(error, Error::Rpc(_)),
            "expected a Transport error, got {error:?}"
        );
    }
}
