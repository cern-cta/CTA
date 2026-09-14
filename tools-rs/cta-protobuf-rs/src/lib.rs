// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

pub mod cta {
    pub mod admin {
        tonic::include_proto!("cta.admin");
    }
    pub mod common {
        tonic::include_proto!("cta.common");
    }
    pub mod eos {
        tonic::include_proto!("cta.eos");
    }
    pub mod xrd {
        tonic::include_proto!("cta.xrd");
    }
}
