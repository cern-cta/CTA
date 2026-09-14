// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

use crate::output::{OutputFormat, output_as_json, output_as_table};
use cta_lib::{StreamResponseExt, cta::CtaGrpcClient, rpc::EndpointConfig};
use tokio_stream::StreamExt;

use cta_protobuf::cta::{
    admin::{
        AdminCmd, OptionStrList, OptionString, OptionUInt64, RecycleTapeFileLsItem, admin_cmd,
        option_str_list, option_string, option_u_int64,
    },
    xrd::{data::Data, request::Request as RequestType, response::ResponseType},
};

#[derive(Debug, Clone)]
pub struct CtaEndpoint {
    pub config: EndpointConfig,
}

impl CtaEndpoint {
    pub async fn list_deleted_files(
        &self,
        output_format: OutputFormat,
        vid: Option<String>,
        disk_instance: Option<String>,
        archive_file_id: Option<u64>,
        copy_number: Option<u64>,
        file_ids: Option<Vec<String>>,
    ) -> Result<Option<Vec<RecycleTapeFileLsItem>>, anyhow::Error> {
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

        if let OutputFormat::Table = output_format {
            println!("Listing deleted files in CTA Catalogue:");
        }

        let mut client = CtaGrpcClient::new_streaming(&self.config).await?;
        let mut response_stream = client.admin_cmd(cmd).await?;
        let mut iter = response_stream.stream_response();

        match output_format {
            OutputFormat::None => {
                let iter: Result<Vec<_>, _> = iter.collect().await;
                Ok(Some(
                    iter?
                        .into_iter()
                        .filter_map(|e| match e {
                            Data::RtflsItem(item) => Some(item),
                            _ => None,
                        })
                        .collect(),
                ))
            }
            OutputFormat::Json => {
                output_as_json(&mut iter).await?;
                Ok(None)
            }
            OutputFormat::Table => {
                output_as_table(&mut iter).await?;
                Ok(None)
            }
        }
    }

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

        println!("A: {:#?}", RequestType::Admincmd(Box::new(cmd.clone())));

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
