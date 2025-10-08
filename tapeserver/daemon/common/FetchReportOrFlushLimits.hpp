/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

namespace cta::tape::daemon::common {

/** The structure representing the maximum number of bytes and files
 cta-taped will fetch or report in one access to the object store*/
struct FetchReportOrFlushLimits {
  uint64_t maxBytes;
  uint64_t maxFiles;
  FetchReportOrFlushLimits(): maxBytes(0), maxFiles(0) {}
  FetchReportOrFlushLimits(uint64_t bytes, uint64_t files): maxBytes(bytes), maxFiles(files) {}
};

} // namespace cta::tape::daemon
