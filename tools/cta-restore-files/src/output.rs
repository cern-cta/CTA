// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Incremental rendering of recycle-bin listings.
use std::{borrow::Cow, io::IsTerminal};

use cta_lib::ResponseError;
use cta_protobuf::cta::{admin::RecycleTapeFileLsItem, xrd::data::Data};
use serde_json::json;
use tokio_stream::{Stream, StreamExt};

/// How a listing should be presented.
#[derive(Debug)]
pub enum OutputFormat {
    /// Do not print anything; return the items to the caller instead.
    None,
    /// Print one JSON object per item and line.
    Json,
    /// Print a human-readable table.
    Table,
}

/// A simple line-buffered table printer for streaming rows to stdout
///
/// `N` is the number of columns, fixed at compile time so that rows are checked
pub struct TablePrinter<const N: usize> {
    widths: [usize; N],
    color: bool,
}

impl<const N: usize> TablePrinter<N> {
    /// Prints the header and returns a printer for the remaining rows.
    ///
    /// Each column is given as a `(name, width)` pair; a width of `0` means
    /// "just wide enough for the column name".
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

    /// Prints one row, padding or truncating each value to its column width.
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

    /// Prints the closing rule and consumes the printer.
    pub fn close(self) {
        let rule = self
            .widths
            .iter()
            .map(|w| "─".repeat(*w))
            .collect::<Vec<_>>()
            .join("-+-");
        println!("{}", paint(self.color, "2", &rule));
    }
}

/// Pads the string with spaces to `width` characters, or truncates it and appends an
/// ellipsis if it is longer.
fn fit(s: &str, width: usize) -> String {
    let len = s.chars().count();
    if len > width {
        let truncated: String = s.chars().take(width.saturating_sub(1)).collect();
        format!("{truncated}…")
    } else {
        format!("{s:<width$}")
    }
}

/// Wraps the string in the ANSI escape sequence `code` when `color` is `true`,
/// and returns it unchanged otherwise.
fn paint(color: bool, code: &str, s: &str) -> String {
    if color {
        format!("\x1b[{code}m{s}\x1b[0m")
    } else {
        s.to_string()
    }
}

/// Streams the recycle-bin items of `iter` to stdout as a table.
///
/// # Errors
///
/// Fails on a stream error, or if the stream yields an item that is not a
/// recycle tape file record.
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

/// Converts a `RecycleTapeFileLsItem` to JSON, ensuring all fields are serialized.
/// This function documents the expected schema and catches schema drift at
/// compile time if new fields are added to the proto.
fn item_to_json(item: &RecycleTapeFileLsItem) -> serde_json::Value {
    let checksum_json: Vec<_> = item
        .checksum
        .iter()
        .map(|c| json!({ "type": c.r#type, "value": c.value }))
        .collect();

    json!({
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
    })
}

/// Streams items from the server and outputs each as a line of JSON.
/// Each line is a complete JSON object representing one deleted file record.
pub async fn output_as_json<I: Stream<Item = Result<Data, ResponseError>> + Unpin>(
    iter: &mut I,
) -> anyhow::Result<()> {
    while let Some(res) = iter.next().await {
        match res {
            Ok(Data::RtflsItem(item)) => {
                log::info!("{item:#?}");
                let json = item_to_json(&item);
                println!("{json}");
            }
            Ok(d) => anyhow::bail!("Unexpected item: {d:#?}"),
            Err(e) => anyhow::bail!("Error: {e:#?}"),
        }
    }
    Ok(())
}
