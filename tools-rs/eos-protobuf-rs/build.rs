// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Build script: generates the Rust bindings for the EOS protobuf/gRPC
//! interface. See the crate documentation in `src/lib.rs` for details.
use std::env;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest_dir = env::var("CARGO_MANIFEST_DIR")?;
    let proto_dir = PathBuf::from(manifest_dir).join("../../lib/protobuf/external/eos-grpc-proto");

    tonic_prost_build::configure()
        // we only need client-side bindings
        .build_server(false)
        // the actual .proto source files and includes
        .compile_protos(
            &[
                proto_dir.join("Authentication.proto"),
                proto_dir.join("File.proto"),
                proto_dir.join("Metadata.proto"),
                proto_dir.join("Recycle.proto"),
                proto_dir.join("Rpc.proto"),
                proto_dir.join("Sched.proto"),
            ],
            &[proto_dir],
        )?;

    Ok(())
}
