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

#include <sys/types.h>
#include <signal.h>
#include <unistd.h>

#include "AgentHeartbeatThread.hpp"
#include "common/log/LogContext.hpp"
#include "common/Timer.hpp"
#include "common/utils/utils.hpp"

namespace cta {
namespace objectstore {

//------------------------------------------------------------------------------
// AgentHeartbeatThread::stopAndWaitThread
//------------------------------------------------------------------------------
void AgentHeartbeatThread::stopAndWaitThread() {
  m_exit.set_value();
  wait();
}

//------------------------------------------------------------------------------
// AgentHeartbeatThread::stopAndWaitThread
//------------------------------------------------------------------------------
void AgentHeartbeatThread::run() {
  log::LogContext lc(m_logger);
  auto exitFuture = m_exit.get_future();
  try {
    while (std::future_status::ready != exitFuture.wait_for(m_heartRate)) {
      utils::Timer t;
      m_agentReference.bumpHeatbeat(m_backend);
      auto updateTime = t.secs();
      if (updateTime > std::chrono::duration_cast<std::chrono::seconds>(m_heartbeatDeadline).count()) {
        log::ScopedParamContainer params(lc);
        params.add("HeartbeatDeadline", std::chrono::duration_cast<std::chrono::seconds>(m_heartbeatDeadline).count())
          .add("HeartbeatUpdateTime", updateTime);
        lc.log(log::CRIT, "In AgentHeartbeatThread::run(): Could not update heartbeat in time. Exiting (segfault).");
        cta::utils::segfault();
        ::exit(EXIT_FAILURE);
      }
    }
  }
  catch (cta::exception::Exception& ex) {
    log::ScopedParamContainer params(lc);
    params.add("Message", ex.getMessageValue());
    lc.log(log::CRIT,
           "In AgentHeartbeatThread::run(): exception while bumping heartbeat. Backtrace follows. Exiting (segfault).");
    lc.logBacktrace(log::INFO, ex.backtrace());
    cta::utils::segfault();
    ::exit(EXIT_FAILURE);
  }
}

}  // namespace objectstore
}  // namespace cta
