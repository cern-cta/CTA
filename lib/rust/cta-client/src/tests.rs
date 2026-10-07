// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

use cta_grpc_common::test_utils::{corrupt_streaming_response, streaming_response};
use cta_protobuf::cta::{
    admin::RecycleTapeFileLsItem,
    xrd::{
        Data as XrdData, Response as XrdResponse, StreamResponse, data::Data,
        response::ResponseType, stream_response::Contents,
    },
};
use tokio_stream::StreamExt;

use crate::{errors::Error, stream::StreamResponseExt, types::hex_to_byte_array};

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

mod cta_stream {
    use super::*;

    #[tokio::test]
    async fn yields_payloads_after_success_header() {
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
    async fn failing_header_is_error() {
        let mut response =
            streaming_response(&[header(ResponseType::RspErrUser), rtfls_item("V01001")]);

        let first = response
            .stream_response()
            .next()
            .await
            .expect("stream should yield an item");

        match first {
            Err(Error::UnexpectedResponseType(t)) => assert_eq!(t, ResponseType::RspErrUser),
            other => panic!("expected a CtaStreamError, got {other:#?}"),
        }
    }

    #[tokio::test]
    async fn skips_frames_without_payload() {
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
    async fn empty_body_is_error() {
        let mut response = streaming_response::<StreamResponse>(&[]);

        let first = response.stream_response().next().await;

        match first {
            Some(Err(Error::UnexpectedResponseType(ResponseType::RspErrUser))) => {
                // Correct: error for missing header
            }
            other => panic!("expected CtaStreamError for missing header, got {other:#?}"),
        }
    }

    #[tokio::test]
    async fn decoding_failure_is_rpc_error() {
        let mut response = corrupt_streaming_response::<StreamResponse>();

        let first = response
            .stream_response()
            .next()
            .await
            .expect("stream should yield an item");

        assert!(
            matches!(first, Err(Error::Rpc(_))),
            "expected a GrpcError, got {first:#?}"
        );
    }

    #[tokio::test]
    async fn rejects_data_before_header() {
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
            Err(Error::UnexpectedResponseType(ResponseType::RspErrUser)) => {
                // Correct: error for data before header
            }
            other => panic!("expected CtaStreamError for data before header, got {other:#?}"),
        }
    }

    #[tokio::test]
    async fn rejects_multiple_headers() {
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
            Err(Error::UnexpectedResponseType(ResponseType::RspErrUser)) => {
                // Correct: error for duplicate header
            }
            other => panic!("expected CtaStreamError for duplicate header, got {other:#?}"),
        }
    }

    #[tokio::test]
    async fn valid_sequence_yields_all_data() {
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
}

mod hex_to_byte_array {
    use super::*;

    #[test]
    fn plain_hex() {
        assert_eq!(hex_to_byte_array("1a2b").unwrap(), [0x1a, 0x2b]);
        assert_eq!(hex_to_byte_array("00").unwrap(), [0x00]);
        assert_eq!(hex_to_byte_array("ff").unwrap(), [0xff]);
    }

    #[test]
    fn both_prefix_spellings() {
        assert_eq!(hex_to_byte_array("0x1a2b").unwrap(), [0x1a, 0x2b]);
        assert_eq!(hex_to_byte_array("0X1a2b").unwrap(), [0x1a, 0x2b]);
    }

    #[test]
    fn odd_digits_left_padded() {
        assert_eq!(hex_to_byte_array("0x1a2").unwrap(), [0x01, 0xa2]);
        assert_eq!(hex_to_byte_array("f").unwrap(), [0x0f]);
        assert_eq!(hex_to_byte_array("abcde").unwrap(), [0x0a, 0xbc, 0xde]);
    }

    #[test]
    fn case_insensitive() {
        assert_eq!(
            hex_to_byte_array("DEADBEEF").unwrap(),
            hex_to_byte_array("deadbeef").unwrap()
        );
    }

    #[test]
    fn empty_input() {
        assert!(hex_to_byte_array("").unwrap().is_empty());
        assert!(hex_to_byte_array("0x").unwrap().is_empty());
    }

    #[test]
    fn rejects_non_hex() {
        assert!(hex_to_byte_array("12zz").is_err());
        assert!(hex_to_byte_array("hello").is_err());
        assert!(hex_to_byte_array("12 34").is_err());
    }

    /// Multi-byte characters used to be chunked on byte boundaries, which fed
    /// invalid UTF-8 into `str::from_utf8().unwrap()` and panicked instead of
    /// returning an error.
    #[test]
    fn rejects_multi_byte_without_panic() {
        for input in ["€", "0x€", "ä", "12€34", "aä", "🦀"] {
            assert!(
                hex_to_byte_array(input).is_err(),
                "{input:?} should be rejected"
            );
        }
    }
}
