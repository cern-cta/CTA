/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/utils/Timer.hpp"

#include <optional>
#include <string>

namespace castor::tape::tapeserver::daemon {

// Used to track the number of active transfer tasks (e.g. DiskReadTask, TapeReadTask, etc..)
class TransferTaskTracker {
public:
  TransferTaskTracker(std::string_view ioDirection, std::string_view ioMedium);
  ~TransferTaskTracker();
  TransferTaskTracker(const TransferTaskTracker&) = delete;
  TransferTaskTracker& operator=(const TransferTaskTracker&) = delete;

private:
  std::string m_ioDirection;
  std::string m_ioMedium;
};

}  // namespace castor::tape::tapeserver::daemon
