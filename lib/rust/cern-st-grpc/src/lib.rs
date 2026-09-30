// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Transport-level gRPC plumbing shared by the CTA and EOS clients:
//! endpoint configuration, JWT authentication and TLS.

mod errors;
mod rpc;
pub mod test_utils;
#[cfg(test)]
mod tests;
pub mod utils;

pub use errors::{Error, validate_scheme};
pub use rpc::{AuthorizationInterceptor, EndpointConfig, JwtAuth};
