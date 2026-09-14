// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

use cta_lib::ResponseError;
use cta_protobuf::cta::xrd::data::Data;
use serde_json::json;

use std::{borrow::Cow, io::IsTerminal};
use tokio_stream::{Stream, StreamExt};

#[derive(Debug)]
pub enum OutputFormat {
    None,
    Json,
    Table,
}

/// A simple line-buffered table printer for streaming rows to stdout
pub struct TablePrinter<const N: usize> {
    widths: [usize; N],
    color: bool,
}

impl<const N: usize> TablePrinter<N> {
    pub fn new(columns: [(&str, usize); N]) -> Self {
        let color = std::io::stdout().is_terminal();

        let widths: [usize; N] = std::array::from_fn(|i| {
            let (name, w) = columns[i];
            if w > 0 { w } else { name.chars().count() }
        });

        let header = columns
            .iter()
            .zip(widths.iter())
            .map(|((name, _), w)| paint(color, "1;36", &format!("{name:<w$}")))
            .collect::<Vec<_>>()
            .join(" | ");
        println!("{}", header);

        let rule = widths
            .iter()
            .map(|w| "─".repeat(*w))
            .collect::<Vec<_>>()
            .join("-+-");
        println!("{}", paint(color, "2", &rule));

        Self { widths, color }
    }

    pub fn print_row(&self, values: [Cow<'_, str>; N]) {
        let sep = paint(self.color, "2", "|");
        let row = values
            .iter()
            .zip(self.widths.iter())
            .map(|(v, w)| fit(v.as_ref(), *w))
            .collect::<Vec<_>>()
            .join(&format!(" {sep} "));
        println!("{row}");
    }

    pub fn close(self) {
        let rule = self
            .widths
            .iter()
            .map(|w| "-".repeat(*w))
            .collect::<Vec<_>>()
            .join("-+-");
        println!("{}", paint(self.color, "2", &rule));
    }
}

fn fit(s: &str, width: usize) -> String {
    let len = s.chars().count();
    if len > width {
        let truncated: String = s.chars().take(width.saturating_sub(1)).collect();
        format!("{truncated}…")
    } else {
        format!("{s:<width$}")
    }
}

fn paint(color: bool, code: &str, s: &str) -> String {
    if color {
        format!("\x1b[{code}m{s}\x1b[0m")
    } else {
        s.to_string()
    }
}

pub async fn output_as_table<I: Stream<Item = Result<Data, ResponseError>> + Unpin>(
    iter: &mut I,
) -> anyhow::Result<()> {
    let table = TablePrinter::new([
        ("archive_file_id", 16),
        ("disk_file_id", 0),
        ("dfi_deleted", 0),
        ("disk_instance", 16),
        ("vid", 16),
        ("path", 50),
        ("copy_nb", 0),
    ]);

    while let Some(res) = iter.next().await {
        match res {
            Ok(Data::RtflsItem(item)) => {
                log::info!("{item:#?}");
                table.print_row([
                    Cow::Owned(item.archive_file_id.to_string()),
                    Cow::Borrowed(&item.disk_file_id),
                    Cow::Borrowed(&item.disk_file_id_when_deleted),
                    Cow::Borrowed(&item.disk_instance),
                    Cow::Borrowed(&item.vid),
                    Cow::Borrowed(&item.disk_file_path),
                    Cow::Owned(item.copy_nb.to_string()),
                ]);
            }
            Ok(d) => anyhow::bail!("Unexpected item: {d:#?}"),
            Err(e) => anyhow::bail!("Error: {e:#?}"),
        }
    }

    table.close();
    Ok(())
}

pub async fn output_as_json<I: Stream<Item = Result<Data, ResponseError>> + Unpin>(
    iter: &mut I,
) -> anyhow::Result<()> {
    while let Some(res) = iter.next().await {
        match res {
            Ok(Data::RtflsItem(item)) => {
                log::info!("{item:#?}");
                let checksum_json: Vec<_> = item
                    .checksum
                    .iter()
                    .map(|c| json!({ "type": c.r#type, "value": c.value }))
                    .collect();

                let json = json!({
                    "vid": item.vid,
                    "fseq": item.fseq,
                    "block_id": item.block_id,
                    "copy_nb": item.copy_nb,
                    "tape_file_creation_time": item.tape_file_creation_time,
                    "archive_file_id": item.archive_file_id,
                    "disk_instance": item.disk_instance,
                    "disk_file_id": item.disk_file_id,
                    "disk_file_id_when_deleted": item.disk_file_id_when_deleted,
                    "disk_file_uid": item.disk_file_uid,
                    "disk_file_gid": item.disk_file_gid,
                    "size_in_bytes": item.size_in_bytes,
                    "checksum": checksum_json,
                    "storage_class": item.storage_class,
                    "archive_file_creation_time": item.archive_file_creation_time,
                    "reconciliation_time": item.reconciliation_time,
                    "collocation_hint": item.collocation_hint,
                    "disk_file_path": item.disk_file_path,
                    "reason_log": item.reason_log,
                    "recycle_log_time": item.recycle_log_time,
                    "virtual_organization": item.virtual_organization,
                    "instance_name": item.instance_name,
                });
                println!("{json}");
            }
            Ok(d) => anyhow::bail!("Unexpected item: {d:#?}"),
            Err(e) => anyhow::bail!("Error: {e:#?}"),
        }
    }
    Ok(())
}
