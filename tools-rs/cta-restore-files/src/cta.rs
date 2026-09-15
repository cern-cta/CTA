// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! The CTA frontend side of the restore workflow.
//!
//! Both operations of this tool map onto the `recycletapefile` admin command:
//! [`CtaEndpoint::list_deleted_files`] runs its `ls` subcommand (streaming) and
//! [`CtaEndpoint::restore_deleted_file_copy`] its `restore` subcommand (unary).

use crate::output::{OutputFormat, output_as_json, output_as_table};
use cta_lib::{StreamResponseExt, cta::CtaGrpcClient, rpc::EndpointConfig};
use tokio_stream::StreamExt;

use cta_protobuf::cta::{
    admin::{
        AdminCmd, OptionStrList, OptionString, OptionUInt64, RecycleTapeFileLsItem, admin_cmd,
        option_str_list, option_string, option_u_int64,
    },
    xrd::{data::Data, response::ResponseType},
};

/// A handle on the CTA frontend admin interface.
#[derive(Debug, Clone)]
pub struct CtaEndpoint {
    /// Endpoint, credentials and TLS settings of the CTA frontend.
    pub config: EndpointConfig,
}

impl CtaEndpoint {
    /// Queries the tape file recycle bin (`recycletapefile ls`).
    ///
    /// All filter arguments are optional and are combined by the frontend;
    /// passing none of them lists the whole recycle bin.
    ///
    /// The return value depends on `output_format`:
    ///
    /// * [`OutputFormat::None`] — nothing is printed; the collected items
    ///   are returned as `Some(items)` for processing by the `restore` subcommand.
    /// * [`OutputFormat::Table`] / [`OutputFormat::Json`] — the items are
    ///   rendered to stdout and `None` is returned.
    ///
    /// # Errors
    ///
    /// Fails if the connection cannot be established, if the frontend reports
    /// an error for the command, or if the stream contains an unexpected item
    /// type.
    pub async fn list_deleted_files(
        &self,
        output_format: OutputFormat,
        vid: Option<String>,
        disk_instance: Option<String>,
        archive_file_id: Option<u64>,
        copy_number: Option<u64>,
        file_ids: Option<Vec<String>>,
    ) -> Result<Option<Vec<RecycleTapeFileLsItem>>, anyhow::Error> {
        // Build command with filters
        let mut cmd = AdminCmd::default();
        cmd.set_cmd(admin_cmd::Cmd::Recycletapefile);
        cmd.set_subcmd(admin_cmd::SubCmd::SubcmdLs);

        if let Some(vid) = &vid {
            cmd.option_str.push(OptionString {
                key: option_string::Key::Vid.into(),
                value: vid.to_string(),
            });
        }

        if let Some(disk_instance) = &disk_instance {
            cmd.option_str.push(OptionString {
                key: option_string::Key::Instance.into(),
                value: disk_instance.to_string(),
            });
        }

        if let Some(archive_file_id) = archive_file_id {
            cmd.option_uint64.push(OptionUInt64 {
                key: option_u_int64::Key::ArchiveFileId.into(),
                value: archive_file_id,
            });
        }

        if let Some(copy_number) = copy_number {
            cmd.option_uint64.push(OptionUInt64 {
                key: option_u_int64::Key::CopyNumber.into(),
                value: copy_number,
            });
        }

        if let Some(file_ids) = &file_ids {
            cmd.option_str_list.push(OptionStrList {
                key: option_str_list::Key::FileId.into(),
                item: file_ids.clone(),
            });
        }

        // Print header for table output
        if let OutputFormat::Table = output_format {
            println!("Listing deleted files in CTA Catalogue:");
        }

        // Execute command and get stream
        let mut client = CtaGrpcClient::new_streaming(&self.config).await?;
        let mut response_stream = client.admin_cmd(cmd).await?;
        let mut iter = response_stream.stream_response();

        // Process stream based on output format (collection vs rendering)
        match output_format {
            OutputFormat::None => {
                // Collect all items and return for further processing
                let mut items = Vec::new();
                while let Some(res) = iter.next().await {
                    match res {
                        Ok(Data::RtflsItem(item)) => items.push(item),
                        Ok(other) => anyhow::bail!("Unexpected item in stream: {other:#?}"),
                        Err(e) => anyhow::bail!("Stream error: {e:#?}"),
                    }
                }
                Ok(Some(items))
            }
            OutputFormat::Json => {
                // Stream items directly to JSON output
                output_as_json(&mut iter).await?;
                Ok(None)
            }
            OutputFormat::Table => {
                // Stream items directly to table output
                output_as_table(&mut iter).await?;
                Ok(None)
            }
        }
    }

    /// Restores a single tape file copy in the CTA catalogue
    /// (`recycletapefile restore`).
    ///
    /// The entry is identified by the tape volume, disk instance, archive file
    /// id and copy number of `file`. `file.disk_file_id` is submitted as the
    /// disk file id to attach the restored copy to, so callers that recreated
    /// the EOS entry must update that field to the new id first.
    ///
    /// # Errors
    ///
    /// Fails if the RPC fails, or if the frontend answers with anything other
    /// than a success response, in which case the frontend's message is used
    /// as the error message.
    pub async fn restore_deleted_file_copy(
        &self,
        file: &RecycleTapeFileLsItem,
    ) -> Result<(), anyhow::Error> {
        log::info!(
            "Restoring file copy in CTA catalogue: {}",
            file.disk_file_path
        );

        let mut cmd = AdminCmd::default();
        cmd.set_cmd(admin_cmd::Cmd::Recycletapefile);
        cmd.set_subcmd(admin_cmd::SubCmd::SubcmdRestore);

        cmd.option_str.push(OptionString {
            key: option_string::Key::Vid.into(),
            value: file.vid.to_string(),
        });

        cmd.option_str.push(OptionString {
            key: option_string::Key::Instance.into(),
            value: file.disk_instance.to_string(),
        });

        cmd.option_uint64.push(OptionUInt64 {
            key: option_u_int64::Key::ArchiveFileId.into(),
            value: file.archive_file_id,
        });

        cmd.option_uint64.push(OptionUInt64 {
            key: option_u_int64::Key::CopyNumber.into(),
            value: file.copy_nb as u64,
        });

        cmd.option_str.push(OptionString {
            key: option_string::Key::DiskFileId.into(),
            value: file.disk_file_id.to_string(),
        });

        let mut client = CtaGrpcClient::new_unary(&self.config).await?;

        let res = client.admin_cmd(cmd).await?;

        let res = res.into_inner();

        log::info!("Restored file copy in CTA catalogue: {}", res.message_txt);

        match res.r#type() {
            ResponseType::RspSuccess => Ok(()),
            ResponseType::RspInvalid
            | ResponseType::RspErrProtobuf
            | ResponseType::RspErrCta
            | ResponseType::RspErrUser => {
                anyhow::bail!(res.message_txt)
            }
        }
    }
}
