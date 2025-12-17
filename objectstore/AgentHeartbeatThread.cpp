/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "AgentHeartbeatThread.hpp"

#include "common/Timer.hpp"
#include "common/log/LogContext.hpp"
#include "common/utils/utils.hpp"

#include <signal.h>
#include <sys/types.h>
#include <unistd.h>

namespace cta::objectstore {

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
      if (updateTime > 0.5 * std::chrono::duration_cast<std::chrono::seconds>(m_heartbeatDeadline).count()) {
        log::ScopedParamContainer params(lc);
        params.add("HeartbeatUpdateTime", updateTime);
        lc.log(log::WARNING, "In AgentHeartbeatThread::run(): Late to update the heartbeat.");
      }
      if (updateTime > std::chrono::duration_cast<std::chrono::seconds>(m_heartbeatDeadline).count()) {
        log::ScopedParamContainer params(lc);
        params.add("HeartbeatDeadline", std::chrono::duration_cast<std::chrono::seconds>(m_heartbeatDeadline).count())
          .add("HeartbeatUpdateTime", updateTime);
        lc.log(log::CRIT, "In AgentHeartbeatThread::run(): Could not update heartbeat in time. Exiting.");
        ::exit(EXIT_FAILURE);
      }
    }
  } catch (cta::exception::Exception& ex) {
    log::ScopedParamContainer params(lc);
    params.add("exceptionMessage", ex.getMessageValue());
    lc.log(log::CRIT, "In AgentHeartbeatThread::run(): exception while bumping heartbeat. Backtrace follows. Exiting.");
    lc.logBacktrace(log::INFO, ex.backtrace());
    ::exit(EXIT_FAILURE);
  }
}

}  // namespace cta::objectstore
