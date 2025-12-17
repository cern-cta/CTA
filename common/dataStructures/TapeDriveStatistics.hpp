/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/dataStructures/EntryLog.hpp"

#include <optional>
#include <ostream>

namespace cta::common::dataStructures {

struct TapeDriveStatistics {
public:
  TapeDriveStatistics() = default;
  TapeDriveStatistics(const TapeDriveStatistics& statistics) = default;
  uint64_t bytesTransferedInSession;
  uint64_t filesTransferedInSession;
  uint64_t reportTime;
  EntryLog lastModificationLog;
};

}  // namespace cta::common::dataStructures
