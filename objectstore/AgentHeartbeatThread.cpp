/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <sys/types.h>
#include <signal.h>
#include <unistd.h>

#include "AgentHeartbeatThread.hpp"
#include "common/log/LogContext.hpp"
#include "common/Timer.hpp"
#include "common/utils/utils.hpp"

#include "common/helgrind_annotator.hpp"

namespace cta { namespace objectstore {

//------------------------------------------------------------------------------
// AgentHeartbeatThread::stopAndWaitThread
//------------------------------------------------------------------------------
void AgentHeartbeatThread::stopAndWaitThread() {
  ANNOTATE_HAPPENS_BEFORE(&m_exit);
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
    ANNOTATE_HAPPENS_AFTER(&m_exit);
    ANNOTATE_HAPPENS_BEFORE_FORGET_ALL(&m_exit);
  } catch (cta::exception::Exception & ex) {
    log::ScopedParamContainer params(lc);
    params.add("Message", ex.getMessageValue());
    lc.log(log::CRIT, "In AgentHeartbeatThread::run(): exception while bumping heartbeat. Backtrace follows. Exiting (segfault).");
    lc.logBacktrace(log::ERR, ex.backtrace());
    cta::utils::segfault();
    ::exit(EXIT_FAILURE);
  }
}



}} // namespace cta::objectstore
