// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

pub mod eos {
    pub mod console {
        tonic::include_proto!("eos.console");
    }
    pub mod rpc {
        tonic::include_proto!("eos.rpc");
    }
    pub mod traffic_shaping {
        tonic::include_proto!("eos.traffic_shaping");
    }
}
