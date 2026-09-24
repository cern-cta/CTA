// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Build script: generates the Rust bindings for the CTA frontend protobuf/gRPC
//! interface.

use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = PathBuf::from(
        "../../../lib/protobuf/external/xrootd-ssi-protobuf-interface/eos_cta/protobuf",
    );

    tonic_prost_build::configure()
        // do not build server bindings, as we only have client apps for now
        .build_server(false)
        // box these types, as the differences between variants can be quite big
        .boxed("StreamResponse.contents.header")
        .boxed("StreamResponse.contents.data")
        .boxed("Request.request.notification")
        .boxed("Request.request.admincmd")
        // actual .proto source files and includes
        .compile_protos(
            &[
                proto_dir.join("cta_eos.proto"),
                proto_dir.join("cta_admin.proto"),
                proto_dir.join("cta_common.proto"),
                proto_dir.join("cta_frontend.proto"),
            ],
            &[proto_dir],
        )?;
    Ok(())
}
