// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! The framing of the admin protocol: wrapping a command into a request, and checking the responses and the frames of
//! a stream (one success header, then records).

use cta_protobuf::cta::{
    admin::AdminCmd,
    xrd::{
        Request, StreamResponse, data::Data, request::Request as RequestType,
        response::ResponseType, stream_response::Contents,
    },
};
use tokio_stream::{Stream, StreamExt};
use tonic::Streaming;

use super::AdminError;

pub(super) fn request(cmd: AdminCmd) -> Request {
    Request {
        request: Some(RequestType::Admincmd(Box::new(cmd))),
    }
}

/// Checks the type of a unary response or stream header.
///
/// The frontend reports every error as the status of the call, so anything other than `RSP_SUCCESS` breaks the
/// protocol.
pub(super) fn check_response_type(r#type: ResponseType, message: &str) -> Result<(), AdminError> {
    match r#type {
        ResponseType::RspSuccess => Ok(()),
        other => Err(AdminError::invalid_response(format!(
            "unexpected response type {}: {message}",
            other.as_str_name()
        ))),
    }
}

/// Reads the first frame of `stream`, which must be a success header.
pub(super) async fn check_header(stream: &mut Streaming<StreamResponse>) -> Result<(), AdminError> {
    match stream.message().await? {
        Some(StreamResponse {
            contents: Some(Contents::Header(header)),
        }) => check_response_type(header.r#type(), &header.message_txt),
        Some(StreamResponse {
            contents: Some(Contents::Data(_)),
        }) => Err(AdminError::invalid_response(
            "stream data received before the header",
        )),
        Some(StreamResponse { contents: None }) => Err(AdminError::invalid_response(
            "empty frame received before the header",
        )),
        None => Err(AdminError::invalid_response(
            "stream ended without a header",
        )),
    }
}

/// The records of `stream` (after the header), extracted with `item` (the [`StreamingAdminCmd::item`] of the
/// command), ending after the first error.
pub(super) fn records<T: 'static>(
    stream: Streaming<StreamResponse>,
    item: fn(Data) -> Result<T, Box<Data>>,
) -> impl Stream<Item = Result<T, AdminError>> + Send {
    let mut failed = false;
    stream
        .filter_map(move |frame| match frame {
            Ok(StreamResponse {
                contents: Some(Contents::Data(data)),
            }) => data.data.map(|data| {
                item(data).map_err(|other| {
                    AdminError::invalid_response(format!("unexpected record: {other:?}"))
                })
            }),
            Ok(StreamResponse {
                contents: Some(Contents::Header(_)),
            }) => Some(Err(AdminError::invalid_response(
                "second stream header received",
            ))),
            // frames without a payload carry nothing to report
            Ok(StreamResponse { contents: None }) => None,
            Err(status) => Some(Err(status.into())),
        })
        // `Streaming` already ends after a failed status; this also ends the stream after a protocol error
        .map_while(move |item| {
            if failed {
                return None;
            }
            failed = item.is_err();
            Some(item)
        })
}
