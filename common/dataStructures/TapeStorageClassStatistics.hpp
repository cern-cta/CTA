/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <stdint.h>
#include <string>

namespace cta::common::dataStructures {

struct TapeStorageClassStatistics {
  std::string storageClassName;
  uint64_t nbMasterFiles = 0;
  uint64_t masterDataInBytes = 0;
};

}  // namespace cta::common::dataStructures