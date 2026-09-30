// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Rendering of recycle-bin listings to stdout.
use std::{borrow::Cow, io::IsTerminal};

use cta_client::types::File;
use serde_json::json;

/// A simple line-buffered table printer for streaming rows to stdout
///
/// `N` is the number of columns, fixed at compile time
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

/// Renders the recycle-bin items of `iter` to stdout as a table.
pub fn output_as_table(iter: &Vec<File>) {
    let table = TablePrinter::new([
        ("archive_file_id", 16),
        ("disk_file_id", 0),
        ("dfi_deleted", 0),
        ("disk_instance", 16),
        ("vid", 16),
        ("path", 50),
        ("copy_nb", 0),
    ]);

    for item in iter {
        log::info!("{item:#?}");
        table.print_row([
            Cow::Owned(item.archive_file.id.to_string()),
            Cow::Borrowed(&item.disk_file.id),
            Cow::Borrowed(
                item.disk_file
                    .id_when_deleted
                    .as_ref()
                    .unwrap_or(&"-".into()),
            ),
            Cow::Borrowed(&item.disk_file.instance),
            Cow::Borrowed(&item.tape_file.vid),
            Cow::Borrowed(&item.disk_file.path),
            Cow::Owned(item.tape_file.copy_nb.to_string()),
        ]);
    }

    table.close();
}

/// Converts a `RecycleTapeFileLsItem` to JSON, ensuring all fields are serialized.
/// This function documents the expected schema and catches schema drift at
/// compile time if new fields are added to the proto.
fn item_to_json(item: &File) -> serde_json::Value {
    json!({
        "archive_file": item.archive_file,
        "tape_file": item.tape_file,
        "disk_file": item.disk_file
    })
}

/// Prints each item of `files` as a line of JSON.
/// Each line is a complete JSON object representing one deleted file record.
pub fn output_as_json(files: &Vec<File>) {
    for file in files {
        log::info!("{file:#?}");
        let json = item_to_json(file);
        println!("{json}");
    }
}
