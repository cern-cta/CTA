/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "DiskSystemSleepRoutines.hpp"

namespace cta::maintd {

DeleteExpiredDiskSystemSleepEntriesRoutine::DeleteExpiredDiskSystemSleepEntriesRoutine(log::LogContext& lc,
                                                                                       RelationalDB& pgs)
    : m_lc(lc),
      m_RelationalDB(pgs) {
  m_lc.log(cta::log::INFO, "Created " + std::string(m_routineName));
};

void DeleteExpiredDiskSystemSleepEntriesRoutine::execute() {
  // Removes the entries of the disk systems whose sleep time has elapsed. The rows to delete are
  // selected by the database from the values they hold, hence no age has to be passed here.
  m_RelationalDB.deleteExpiredDiskSystemSleepEntries(m_lc);
};

}  // namespace cta::maintd
