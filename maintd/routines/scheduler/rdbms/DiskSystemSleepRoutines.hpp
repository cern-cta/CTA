/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/log/LogContext.hpp"
#include "maintd/IRoutine.hpp"
#include "scheduler/rdbms/RelationalDB.hpp"

namespace cta::maintd {

/**
 * @brief Periodic routine that deletes the expired disk system sleep entries.
 *
 * A disk system reporting insufficient free space leaves an entry in
 * DISK_SYSTEM_SLEEP_TRACKING, which holds the moment it was written and for how long the retrieve
 * queues of that disk system are to be slept. Once that time has elapsed the entry no longer has
 * any effect, as the scheduling paths only read the entries which are still sleeping, and the row
 * is only left to be collected.
 *
 * The routine takes no age or batch size: an entry expires by its own SLEEP_TIME and
 * LAST_UPDATE_TIME, and there is at most one row per disk system.
 */
class DeleteExpiredDiskSystemSleepEntriesRoutine final : public IRoutine {
public:
  std::string getName() const final { return m_routineName; };

  void execute();

  virtual ~DeleteExpiredDiskSystemSleepEntriesRoutine() = default;

  DeleteExpiredDiskSystemSleepEntriesRoutine(log::LogContext& lc, RelationalDB& pgs);

private:
  cta::log::LogContext& m_lc;
  cta::RelationalDB& m_RelationalDB;
  const std::string m_routineName = "DeleteExpiredDiskSystemSleepEntriesRoutine";
};

}  // namespace cta::maintd
