// SPDX-FileCopyrightText: 2026 CERN
// SPDX-License-Identifier: GPL-3.0-or-later

//! Who manages CTA and on whose behalf data is stored: admin users and virtual organisations, and the versions
//! of the components

mod admin_user;
#[cfg(test)]
mod tests;
mod version;
mod virtual_organisation;

pub use admin_user::AdminUser;
pub use version::{ComponentVersion, CtaVersion};
pub use virtual_organisation::VirtualOrganisation;
