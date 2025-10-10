/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "Agent.hpp"
#include "common/exception/NoSuchObject.hpp"
#include "common/Timer.hpp"

namespace cta::objectstore {

class AgentWatchdog {
public:
  AgentWatchdog(const std::string & name, Backend & os): m_agent(name, os),
    m_heartbeatCounter(readGCData().heartbeat) {
    m_agent.fetchNoLock();
    m_timeout = m_agent.getTimeout();
  }

  bool checkAlive() {
    struct gcData newGCData;
    try {
      newGCData = readGCData();
    } catch (cta::exception::NoSuchObject &) {
      // The agent could be gone. This is not an error. Mark it as alive,
      // and will be trimmed later.
      return true;
    }
    auto timer = m_timer.secs();
    // If GC is required, it's easy...
    if (newGCData.needsGC) return false;
    // If heartbeat has not moved for more than the timeout, we declare the agent dead.
    if (newGCData.heartbeat == m_heartbeatCounter && timer > m_timeout)
      return false;
    if (newGCData.heartbeat != m_heartbeatCounter) {
      m_heartbeatCounter = newGCData.heartbeat;
      m_timer.reset();
    }
    return true;
  }

  std::list<log::Param> getDeadAgentDetails() {
    std::list<log::Param> ret;
    auto gcData = readGCData();
    ret.push_back(log::Param("currentHeartbeat", gcData.heartbeat));
    ret.push_back(log::Param("GCRequested", gcData.needsGC?"true":"false"));
    ret.push_back(log::Param("timeout", m_timeout));
    ret.push_back(log::Param("timer", m_timer.secs()));
    ret.push_back(log::Param("heartbeatAtTimerStart", m_heartbeatCounter));
    return ret;
  }

  bool checkExists() {
    return m_agent.exists();
  }

  void setTimeout(double timeout) {
    m_timeout = timeout;
  }

private:
  cta::utils::Timer m_timer;
  Agent m_agent;
  uint64_t m_heartbeatCounter;
  double m_timeout;

  struct gcData {
    uint64_t heartbeat;
    bool needsGC;
  };
  struct gcData readGCData() {
    m_agent.fetchNoLock();
    struct gcData ret;
    ret.heartbeat = m_agent.getHeartbeatCount();
    ret.needsGC = m_agent.needsGarbageCollection();
    return ret;
  }
};

} // namespace cta::objectstore
