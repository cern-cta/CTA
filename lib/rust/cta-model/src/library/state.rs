// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! The state of a library

use serde::Serialize;
use strum::{Display, EnumString, IntoStaticStr};

/// Whether a (logical or physical) library is available for mounts.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Display, EnumString, IntoStaticStr,
)]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum LibraryState {
    /// Tapes of the library can be mounted
    Enabled,
    /// Tapes of the library cannot be mounted
    Disabled,
}

impl LibraryState {
    /// The state matching the `is_disabled` flag of the records.
    #[must_use]
    pub fn from_disabled(is_disabled: bool) -> Self {
        if is_disabled {
            Self::Disabled
        } else {
            Self::Enabled
        }
    }

    /// The `is_disabled` flag of the records (and the `--disabled` option of the commands).
    #[must_use]
    pub fn is_disabled(self) -> bool {
        self == Self::Disabled
    }
}
