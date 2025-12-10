/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "QueueCleanupRoutine.hpp"

namespace cta::maintd {

QueueCleanupRoutine::QueueCleanupRoutine(cta::log::LogContext& lc,
                                         SchedulerDatabase& oStoreDb,
                                         catalogue::Catalogue& catalogue,
                                         int batchSize)
    : m_lc(lc),
      m_queueCleanup(lc, oStoreDb, catalogue, batchSize) {
  log::ScopedParamContainer params(m_lc);
  params.add("batchSize", batchSize);
  m_lc.log(cta::log::INFO, "Created QueueCleanupRoutine");
}

void QueueCleanupRoutine::execute() {
  m_queueCleanup.cleanupQueues();
}

std::string QueueCleanupRoutine::getName() const {
  return "QueueCleanupRoutine";
}

}  // namespace cta::maintd
