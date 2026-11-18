// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Disk instances, their spaces and the disk systems used for backpressure

mod instance;
mod instance_space;
mod system;
#[cfg(test)]
mod tests;

pub use instance::DiskInstance;
pub use instance_space::DiskInstanceSpace;
pub use system::DiskSystem;
