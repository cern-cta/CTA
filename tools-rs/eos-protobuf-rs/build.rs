// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .build_server(false)
        .compile_protos(
            &[
                "external/eos-grpc-proto/Authentication.proto",
                "external/eos-grpc-proto/File.proto",
                "external/eos-grpc-proto/Metadata.proto",
                "external/eos-grpc-proto/Recycle.proto",
                "external/eos-grpc-proto/Rpc.proto",
                "external/eos-grpc-proto/Sched.proto"
            ],
            &["external/eos-grpc-proto/"],
        )?;

    Ok(())
}
