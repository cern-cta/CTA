// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Helpers to build gRPC response streams without a server, for unit tests.
//!
//! A [`Streaming<T>`] is constructed directly from a [`http_body::Body`]
//! through [`Streaming::new_response`], and we simply hand-roll the gRPC wire
//! format:
//!
//! * every message is length-delimited by a 5-byte prefix (1 byte compression
//!   flag + 4 bytes big-endian length);
//! * the body ends with a trailer frame carrying `grpc-status: 0`, which tonic
//!   requires to consider the stream properly terminated.
//!
//! This keeps the stream adapters testable at unit-test speed, with no
//! sockets, ports or spawned servers involved.

use std::convert::Infallible;

use bytes::{BufMut, Bytes, BytesMut};
use http_body::Frame;
use http_body_util::StreamBody;
use tonic::{
    Status, Streaming,
    codec::{Codec, Decoder},
    codegen::http::{HeaderMap, HeaderValue, StatusCode},
};
use tonic_prost::ProstCodec;

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
