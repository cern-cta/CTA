/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

namespace cta::tape::daemon {

/**
 * @brief Whether the existing session logic permits scheduling another mount.
 */
enum class DriveUsability { Reusable, MustRemainDown };

}  // namespace cta::tape::daemon
