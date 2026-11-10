// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Catalogue queries of the tool, built on the typed admin commands of [`cta_client`]: the tape file and recycle bin
//! listings as [`File`]s, and the restore of a recycle bin entry.

use anyhow::bail;
use cta_client::{
    admin::AdminClient,
    commands::{recycle_tape_file, tape_file},
    errors::AdminError,
    types::File,
};
use tokio_stream::{Stream, StreamExt};

/// The only record of `records`, `None` if there is none.
async fn single_record<T: std::fmt::Debug>(
    mut records: impl Stream<Item = Result<T, AdminError>> + Unpin,
    archive_file_id: u64,
) -> anyhow::Result<Option<T>> {
    let Some(first) = records.next().await.transpose()? else {
        return Ok(None);
    };
    log::debug!("Retrieved tape file information: {first:?}");
    if let Some(next) = records.next().await.transpose()? {
        bail!("more than one tape file with archive file ID {archive_file_id}, next one: {next:?}");
    }
    Ok(Some(first))
}

/// The tape file with the given archive file ID (`tapefile ls --id`), `None` if the catalogue has none.
pub(crate) async fn get_file(
    client: &AdminClient,
    archive_file_id: u64,
) -> anyhow::Result<Option<File>> {
    let cmd = tape_file::Ls::builder()
        .archive_file_id(archive_file_id)
        .build();
    let record = single_record(client.stream(cmd).await?, archive_file_id).await?;
    Ok(record.map(File::try_from).transpose()?)
}

/// The files of the recycle bin matching `query` (`recycletf ls`).
pub(crate) async fn list_deleted_files(
    client: &AdminClient,
    query: recycle_tape_file::Ls,
) -> anyhow::Result<impl Stream<Item = anyhow::Result<File>> + use<>> {
    Ok(client
        .stream(query)
        .await?
        .map(|record| Ok(File::try_from(record?)?)))
}

/// Restores the recycle bin entry of `file` in the catalogue (`recycletf restore`).
///
/// The entry is identified by the tape, disk instance, archive file ID and copy number of `file`. The restored copy is
/// attached to `file.disk_file.id`, so callers that recreated the disk file must set it to the new ID first.
pub(crate) async fn restore_deleted_file_copy(
    client: &AdminClient,
    file: &File,
) -> anyhow::Result<()> {
    log::info!(
        "Restoring file copy in CTA catalogue: {}",
        file.disk_file.path
    );
    let cmd = recycle_tape_file::Restore::builder()
        .vid(&file.tape_file.vid)
        .instance(&file.disk_file.instance)
        .disk_file_id(&file.disk_file.id)
        .archive_file_id(file.archive_file.id)
        .copy_nb(u64::from(file.tape_file.copy_nb))
        .build();

    let response = client.unary(cmd).await?;
    log::info!(
        "Restored file copy in CTA catalogue: {}",
        response.message_txt
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn single_record_of_no_record_is_none() {
        let record = single_record(tokio_stream::iter([] as [Result<u32, _>; 0]), 1)
            .await
            .unwrap();

        assert!(record.is_none());
    }

    #[tokio::test]
    async fn single_record_of_one_record() {
        let records = tokio_stream::iter([Ok(42)]);

        let record = single_record(records, 42).await.unwrap();

        assert_eq!(record, Some(42));
    }

    #[tokio::test]
    async fn single_record_rejects_more_than_one_record() {
        let records = tokio_stream::iter([Ok(42), Ok(42)]);

        let error = single_record(records, 42).await.unwrap_err();

        assert!(
            error.to_string().starts_with("more than one tape file"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn single_record_propagates_stream_errors() {
        let records = tokio_stream::iter([Err::<u32, _>(AdminError::InvalidResponse {
            message: "garbled".to_owned(),
            source: None,
        })]);

        let error = single_record(records, 42).await.unwrap_err();

        assert!(error.downcast_ref::<AdminError>().is_some(), "{error}");
    }
}
