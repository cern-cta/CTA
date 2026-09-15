// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Build script: generates the Rust bindings for the EOS protobuf/gRPC
//! interface. See the crate documentation in `src/lib.rs` for details.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        // we only need client-side bindings
        .build_server(false)
        // the actual .proto source files and includes
        .compile_protos(
            &[
                "external/eos-grpc-proto/Authentication.proto",
                "external/eos-grpc-proto/File.proto",
                "external/eos-grpc-proto/Metadata.proto",
                "external/eos-grpc-proto/Recycle.proto",
                "external/eos-grpc-proto/Rpc.proto",
                "external/eos-grpc-proto/Sched.proto",
            ],
            &["external/eos-grpc-proto/"],
        )?;

    Ok(())
}
