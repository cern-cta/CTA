// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! The hardware: tape drives, and the logical and physical libraries they are in

mod drive;
mod logical;
mod physical;
mod state;
#[cfg(test)]
mod tests;

pub use drive::{Drive, DriveMetadata, DriveOperatingState, TapeSession, TapeSessionState};
pub use logical::LogicalLibrary;
pub use physical::PhysicalLibrary;
pub use state::LibraryState;
