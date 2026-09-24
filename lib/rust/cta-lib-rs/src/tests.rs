// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

use std::convert::Infallible;

use bytes::{BufMut, Bytes, BytesMut};
use cta_protobuf::cta::{
    admin::RecycleTapeFileLsItem,
    xrd::{Data as XrdData, Response as XrdResponse},
};
use eos_protobuf::eos::rpc::MdResponse;
use http_body::Frame;
use http_body_util::StreamBody;
use tokio_stream::StreamExt;
use tonic::{
    Status, Streaming,
    codec::{Codec, Decoder},
    codegen::http::{HeaderMap, HeaderValue, StatusCode},
};
use tonic_prost::ProstCodec;

use super::*;

// Helpers to build gRPC response streams without a server, for unit tests.
//
// A [`Streaming<T>`] is constructed directly from a [`http_body::Body`]
// through [`Streaming::new_response`], and we simply hand-roll the gRPC wire
// format:
//
// * every message is length-delimited by a 5-byte prefix (1 byte compression
//   flag + 4 bytes big-endian length);
// * the body ends with a trailer frame carrying `grpc-status: 0`, which tonic
//   requires to consider the stream properly terminated.
//
// This keeps the stream adapters testable at unit-test speed, with no
// sockets, ports or spawned servers involved.

/// Wraps `payload` in a gRPC length-delimited frame.
fn frame(payload: &[u8]) -> Bytes {
    let mut buf = BytesMut::with_capacity(payload.len() + 5);
    buf.put_u8(0); // not compressed
    buf.put_u32(payload.len() as u32); // big-endian length
    buf.put_slice(payload);
    buf.freeze()
}

/// Encodes `messages` into a single gRPC body.
fn body_of<T: prost::Message>(messages: &[T]) -> Bytes {
    let mut buf = BytesMut::new();
    for message in messages {
        buf.put_slice(&frame(&message.encode_to_vec()));
    }
    buf.freeze()
}

/// Wraps raw gRPC frames into a body terminated by a successful trailer, as a
/// real server would send it.
fn grpc_body(data: Bytes) -> StreamBody<tokio_stream::Iter<GrpcFrames>> {
    let mut trailers = HeaderMap::new();
    trailers.insert("grpc-status", HeaderValue::from_static("0"));

    StreamBody::new(tokio_stream::iter(vec![
        Ok(Frame::data(data)),
        Ok(Frame::trailers(trailers)),
    ]))
}

/// The frame sequence of a canned gRPC body.
type GrpcFrames = std::vec::IntoIter<Result<Frame<Bytes>, Infallible>>;

/// Builds a decoder for `T` using the same prost codec the generated clients use.
fn decoder_for<T>() -> impl Decoder<Item = T, Error = Status> + Send + 'static
where
    T: prost::Message + Default + 'static,
{
    ProstCodec::<T, T>::default().decoder()
}

/// Builds a response stream that yields `messages`, as if a server had sent
/// them.
pub(crate) fn streaming_response<T>(messages: &[T]) -> Streaming<T>
where
    T: prost::Message + Default + 'static,
{
    streaming_response_from_bytes(body_of(messages))
}

/// Builds a response stream from raw frames, for tests that need malformed
/// or hand-crafted input.
pub(crate) fn streaming_response_from_bytes<T>(body: Bytes) -> Streaming<T>
where
    T: prost::Message + Default + 'static,
{
    Streaming::new_response(
        decoder_for::<T>(),
        grpc_body(body),
        StatusCode::OK,
        None,
        None,
    )
}

/// Builds a response stream whose single frame does not decode as `T`.
pub(crate) fn corrupt_streaming_response<T>() -> Streaming<T>
where
    T: prost::Message + Default + 'static,
{
    // Field number 1 with wire type 6, which does not exist.
    streaming_response_from_bytes(frame(&[0x0e, 0xff, 0xff]))
}

/// A `StreamResponse` carrying just a header with the given response type.
fn header(r#type: ResponseType) -> StreamResponse {
    StreamResponse {
        contents: Some(Contents::Header(Box::new(XrdResponse {
            r#type: r#type.into(),
            ..Default::default()
        }))),
    }
}

/// A `StreamResponse` carrying a recycle-bin item with the given VID.
fn rtfls_item(vid: &str) -> StreamResponse {
    StreamResponse {
        contents: Some(Contents::Data(Box::new(XrdData {
            data: Some(Data::RtflsItem(RecycleTapeFileLsItem {
                vid: vid.into(),
                ..Default::default()
            })),
        }))),
    }
}

/// A `StreamResponse` whose data frame carries no payload at all.
fn empty_data() -> StreamResponse {
    StreamResponse {
        contents: Some(Contents::Data(Box::new(XrdData { data: None }))),
    }
}

#[tokio::test]
async fn cta_stream_yields_payloads_after_a_success_header() {
    let mut response = streaming_response(&[
        header(ResponseType::RspSuccess),
        rtfls_item("V01001"),
        rtfls_item("V01002"),
    ]);

    let items: Vec<_> = response
        .stream_response()
        .collect::<Result<Vec<_>, _>>()
        .await
        .expect("stream should succeed");

    let vids: Vec<_> = items
        .iter()
        .map(|d| match d {
            Data::RtflsItem(item) => item.vid.clone(),
            other => panic!("unexpected payload: {other:#?}"),
        })
        .collect();

    assert_eq!(vids, ["V01001", "V01002"]);
}

#[tokio::test]
async fn cta_stream_reports_a_failing_header_as_an_error() {
    let mut response =
        streaming_response(&[header(ResponseType::RspErrUser), rtfls_item("V01001")]);

    let first = response
        .stream_response()
        .next()
        .await
        .expect("stream should yield an item");

    match first {
        Err(ResponseError::CtaStreamError(t)) => assert_eq!(t, ResponseType::RspErrUser),
        other => panic!("expected a CtaStreamError, got {other:#?}"),
    }
}

#[tokio::test]
async fn cta_stream_skips_frames_without_a_payload() {
    let mut response = streaming_response(&[
        header(ResponseType::RspSuccess),
        empty_data(),
        rtfls_item("V01001"),
        empty_data(),
    ]);

    let items: Vec<_> = response
        .stream_response()
        .collect::<Result<Vec<_>, _>>()
        .await
        .expect("stream should succeed");

    assert_eq!(items.len(), 1, "only the frame with a payload is yielded");
}

#[tokio::test]
async fn cta_stream_of_an_empty_body_is_an_error() {
    let mut response = streaming_response::<StreamResponse>(&[]);

    let first = response.stream_response().next().await;

    match first {
        Some(Err(ResponseError::CtaStreamError(ResponseType::RspErrUser))) => {
            // Correct: error for missing header
        }
        other => panic!("expected CtaStreamError for missing header, got {other:#?}"),
    }
}

#[tokio::test]
async fn cta_stream_maps_a_decoding_failure_to_a_grpc_error() {
    let mut response = corrupt_streaming_response::<StreamResponse>();

    let first = response
        .stream_response()
        .next()
        .await
        .expect("stream should yield an item");

    assert!(
        matches!(first, Err(ResponseError::GrpcError(_))),
        "expected a GrpcError, got {first:#?}"
    );
}

#[tokio::test]
async fn cta_stream_rejects_data_before_header() {
    let mut response = streaming_response(&[
        rtfls_item("V01001"), // Data first, no header
        header(ResponseType::RspSuccess),
    ]);

    let first = response
        .stream_response()
        .next()
        .await
        .expect("stream should yield an item");

    match first {
        Err(ResponseError::CtaStreamError(ResponseType::RspErrUser)) => {
            // Correct: error for data before header
        }
        other => panic!("expected CtaStreamError for data before header, got {other:#?}"),
    }
}

#[tokio::test]
async fn cta_stream_rejects_multiple_headers() {
    let mut response = streaming_response(&[
        header(ResponseType::RspSuccess),
        header(ResponseType::RspSuccess), // Second header
    ]);

    let _first = response.stream_response().next().await;

    // First header is ok (doesn't yield)
    let second = response
        .stream_response()
        .next()
        .await
        .expect("stream should yield an item");

    match second {
        Err(ResponseError::CtaStreamError(ResponseType::RspErrUser)) => {
            // Correct: error for duplicate header
        }
        other => panic!("expected CtaStreamError for duplicate header, got {other:#?}"),
    }
}

#[tokio::test]
async fn cta_stream_with_valid_sequence_yields_all_data() {
    let mut response = streaming_response(&[
        header(ResponseType::RspSuccess),
        rtfls_item("V01001"),
        rtfls_item("V01002"),
        StreamResponse { contents: None }, // Proper EOF
    ]);

    let items: Vec<_> = response
        .stream_response()
        .collect::<Result<Vec<_>, _>>()
        .await
        .expect("stream should succeed");

    assert_eq!(
        items.len(),
        2,
        "both items yielded, stream properly terminated"
    );
}

#[tokio::test]
async fn eos_stream_yields_every_response_in_order() {
    let responses = [
        MdResponse {
            r#type: 0,
            ..Default::default()
        },
        MdResponse {
            r#type: 1,
            ..Default::default()
        },
    ];
    let response = streaming_response(&responses);

    let items: Vec<_> = response
        .collect::<Result<Vec<_>, _>>()
        .await
        .expect("stream should succeed");

    assert_eq!(items, responses);
}

#[tokio::test]
async fn eos_stream_propagates_a_decoding_failure() {
    let mut response = corrupt_streaming_response::<MdResponse>();

    let first: Option<Result<MdResponse, Status>> = response.next().await;

    assert!(
        matches!(first, Some(Err(_))),
        "expected a Status error, got {first:#?}"
    );
}

#[tokio::test]
async fn eos_stream_of_an_empty_body_terminates_immediately() {
    let mut response = streaming_response_from_bytes::<MdResponse>(bytes::Bytes::new());

    assert!(response.next().await.is_none());
}
