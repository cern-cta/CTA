// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Utility functions

/// Util function to get the system time as seconds since the epoch. Start of 1970 UTC
/// is the standard in every supported system.
///
/// # Panics
///
/// Panics if the system clock is set before the UNIX epoch.
pub fn system_time_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("Current time is lower than UNIX_EPOCH")
        .as_secs()
}
