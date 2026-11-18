// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Drives, logical and physical libraries

mod drive;
mod logical;
mod physical;
#[cfg(test)]
mod tests;

pub use drive::Drives;
pub use logical::LogicalLibraries;
pub use physical::PhysicalLibraries;
