/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

namespace cta::tape::daemon {

/// Outcome of fetching work for a tape session.
enum class WorkFetchResult { WorkAvailable, NoWork, StopRequested };

}  // namespace cta::tape::daemon
