// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Build script: generates the Rust bindings for the CTA frontend protobuf/gRPC
//! interface. See the crate documentation in `src/lib.rs` for details.

use std::{env, path::PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = PathBuf::from(
        "../../../lib/protobuf/external/xrootd-ssi-protobuf-interface/eos_cta/protobuf",
    );

    let descriptor_path = PathBuf::from(env::var("OUT_DIR")?).join("cta_descriptor.bin");

    tonic_prost_build::configure()
        // keep the file descriptor set, as pbjson-build needs it below (with the `json` feature)
        .file_descriptor_set_path(&descriptor_path)
        // server bindings are not needed in client code
        .build_server(env::var_os("CARGO_FEATURE_SERVER").is_some())
        // box these types, as the differences between variants can be quite big
        .boxed("StreamResponse.contents.header")
        .boxed("StreamResponse.contents.data")
        .boxed("Request.request.notification")
        .boxed("Request.request.admincmd")
        // `impl TryFrom<Data> for StorageClassLsItem`, ... for every record of the stream frames
        .enum_attribute("cta.xrd.Data.data", "#[derive(::derive_more::TryInto)]")
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

    // proto3 JSON (serde) impls
    #[cfg(feature = "json")]
    pbjson_build::Builder::new()
        .register_descriptors(&std::fs::read(&descriptor_path)?)?
        .emit_fields()
        .build(&[".cta"])?;
    Ok(())
}
