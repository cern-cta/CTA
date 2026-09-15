/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

namespace cta::tape::daemon {

// Whether the existing session logic permits scheduling another mount.
// This does not independently establish the hardware cleanup outcome.
enum class DriveUsability { Reusable, MustRemainDown };

}  // namespace cta::tape::daemon
