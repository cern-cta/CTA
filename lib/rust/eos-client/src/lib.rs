// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

#![forbid(unsafe_code)]

//! Client library for the EOS namespace gRPC API, used by CTA tools.

mod client;
mod errors;
mod macros;
#[cfg(test)]
mod tests;

pub use client::{DEFAULT_FILE_MODE, EosEndpointMap, EosGrpcClient};
pub use errors::Error;
