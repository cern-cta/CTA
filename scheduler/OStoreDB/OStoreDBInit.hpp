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

#pragma once

#include <objectstore/AgentHeartbeatThread.hpp>
#include <objectstore/BackendFactory.hpp>
#include <objectstore/BackendPopulator.hpp>
#include <objectstore/BackendVFS.hpp>
#include <scheduler/OStoreDB/OStoreDBWithAgent.hpp>

namespace cta {

class OStoreDBInit {
public:
  OStoreDBInit(const std::string& client_process,
               const std::string& db_conn_str,
               log::Logger& log,
               bool leaveNonEmptyAgentsBehind = false) {
    // Initialise the ObjectStore Backend
    m_backend = std::move(objectstore::BackendFactory::createBackend(db_conn_str, log));
    m_backendPopulator =
      std::make_unique<objectstore::BackendPopulator>(*m_backend, client_process, log::LogContext(log));
    if (leaveNonEmptyAgentsBehind) {
      m_backendPopulator->leaveNonEmptyAgentsBehind();
    }

    try {
      // If the backend is a VFS, don't delete it on exit
      dynamic_cast<objectstore::BackendVFS&>(*m_backend).noDeleteOnExit();
    } catch (std::bad_cast&) {
      // If not, never mind
    }

    // Start the heartbeat thread for the agent object. The thread is guaranteed to have started before we call the unique_ptr deleter
    auto aht = new objectstore::AgentHeartbeatThread(m_backendPopulator->getAgentReference(), *m_backend, log);
    aht->startThread();
    m_agentHeartbeat = std::move(UniquePtrAgentHeartbeatThread(aht));
  }

  std::unique_ptr<OStoreDBWithAgent> getSchedDB(catalogue::Catalogue& catalogue, log::Logger& log) {
    return std::make_unique<OStoreDBWithAgent>(*m_backend, m_backendPopulator->getAgentReference(), catalogue, log);
  }

  cta::objectstore::AgentReference& getAgentReference() { return m_backendPopulator->getAgentReference(); }

  objectstore::Backend& getBackend() { return *m_backend; }

private:
  /*!
   * Deleter for instances of the AgentHeartbeatThread class.
   *
   * Using a deleter (rather than calling from a destructor) guarantees that we can't call stopAndWaitThread()
   * before the AgentHeartbeatThread has been started.
   */
  struct AgentHeartbeatThreadDeleter {
    void operator()(objectstore::AgentHeartbeatThread* aht) { aht->stopAndWaitThread(); }
  };

  using UniquePtrAgentHeartbeatThread = std::unique_ptr<objectstore::AgentHeartbeatThread, AgentHeartbeatThreadDeleter>;

  // Member variables

  std::unique_ptr<objectstore::Backend> m_backend;                    //!< VFS backend for the objectstore DB
  std::unique_ptr<objectstore::BackendPopulator> m_backendPopulator;  //!< Object used to populate the backend
  UniquePtrAgentHeartbeatThread m_agentHeartbeat;                     //!< Agent heartbeat thread
};

using SchedulerDBInit_t = OStoreDBInit;
using SchedulerDB_t = OStoreDBWithAgent;

}  // namespace cta
