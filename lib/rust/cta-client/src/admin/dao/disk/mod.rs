// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Disk instances, disk instance spaces and disk systems

mod instance;
mod instance_space;
mod system;
#[cfg(test)]
mod tests;

pub use instance::DiskInstances;
pub use instance_space::DiskInstanceSpaces;
pub use system::DiskSystems;
