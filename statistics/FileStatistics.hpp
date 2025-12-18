/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <stdint.h>

namespace cta::statistics {

struct FileStatistics {
  FileStatistics() = default;
  FileStatistics(const FileStatistics& other) = default;
  FileStatistics& operator=(const FileStatistics& other) = default;
  FileStatistics& operator+=(const FileStatistics& other);

  uint64_t nbMasterFiles = 0;
  uint64_t masterDataInBytes = 0;
  uint64_t nbCopyNb1 = 0;
  uint64_t copyNb1InBytes = 0;
  uint64_t nbCopyNbGt1 = 0;
  uint64_t copyNbGt1InBytes = 0;
};

}  // namespace cta::statistics
