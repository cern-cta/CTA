// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .build_server(false)
        // box these types, as the differences between variants can be quite big
        .boxed("StreamResponse.contents.header")
        .boxed("StreamResponse.contents.data")
        .boxed("Request.request.notification")
        .boxed("Request.request.admincmd")
        .compile_protos(
            &[
                "../../lib/protobuf/external/xrootd-ssi-protobuf-interface/eos_cta/protobuf/cta_eos.proto",
                "../../lib/protobuf/external/xrootd-ssi-protobuf-interface/eos_cta/protobuf/cta_admin.proto",
                "../../lib/protobuf/external/xrootd-ssi-protobuf-interface/eos_cta/protobuf/cta_common.proto",
                "../../lib/protobuf/external/xrootd-ssi-protobuf-interface/eos_cta/protobuf/cta_frontend.proto",
            ],
            &["../../lib/protobuf/external/xrootd-ssi-protobuf-interface/eos_cta/protobuf/"],
        )?;
    Ok(())
}
