/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "session/TransferSessionResult.hpp"

#include <memory>
#include <optional>
#include <string>
#include <utility>

namespace cta {
class IScheduler;
class TapeMount;
}  // namespace cta

namespace cta::tape::daemon {

class TapeSessionTracker;

// External operations needed by the drive lifecycle.
class DriveOperations {
public:
  virtual ~DriveOperations() = default;

  virtual IScheduler& scheduler() = 0;
  virtual bool logicalLibraryExists() = 0;
  virtual std::pair<bool, std::optional<std::string>> probeDrive() = 0;
  virtual std::unique_ptr<TapeMount> getNextMount() = 0;
  virtual TransferSessionResult transfer(TapeMount& mount, TapeSessionTracker& tracker) = 0;
  virtual bool clean(const std::optional<std::string>& vid, bool waitMediaInDrive, TapeSessionTracker& tracker) = 0;
  virtual void sleep(unsigned int seconds) = 0;
};

}  // namespace cta::tape::daemon
