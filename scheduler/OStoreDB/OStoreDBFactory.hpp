/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "scheduler/SchedulerDatabaseFactory.hpp"
#include "scheduler/OStoreDB/OStoreDB.hpp"
#include "common/archiveNS/StorageClass.hpp"
#include "common/TapePool.hpp"
#include "common/archiveNS/Tape.hpp"
#include "common/dataStructures/SecurityIdentity.hpp"
#include "scheduler/LogicalLibrary.hpp"
#include "scheduler/RetrieveRequestDump.hpp"
#include "objectstore/RootEntry.hpp"
#include "objectstore/Agent.hpp"
#include "objectstore/BackendVFS.hpp"
#include "objectstore/BackendRados.hpp"
#include "objectstore/BackendFactory.hpp"
#include <memory>

namespace cta {
  
namespace objectstore {
  class Backend;
}

namespace {
/**
 * A self contained class providing a "backend+SchedulerDB" in a box,
 * allowing creation (and deletion) of an object store for each instance of the
 * of the unit tests, as they require a fresh object store for each unit test.
 * It can be instantiated with both types of backends
 */
template <class BackendType>
class OStoreDBWrapper: public SchedulerDatabase {
public:
  OStoreDBWrapper(const std::string &context, const std::string &URL = "");
  
  ~OStoreDBWrapper() throw () {}

  void queue(const cta::common::dataStructures::ArchiveRequest& request, const cta::common::dataStructures::ArchiveFileQueueCriteria& criteria) override {
    return m_OStoreDB.queue(request, criteria);
  }

  
  void deleteArchiveRequest(const common::dataStructures::SecurityIdentity& cliIdentity, uint64_t archiveFileId) override {
    m_OStoreDB.deleteArchiveRequest(cliIdentity, archiveFileId);
  }

  virtual std::unique_ptr<cta::SchedulerDatabase::ArchiveToFileRequestCancelation> markArchiveRequestForDeletion(const common::dataStructures::SecurityIdentity &cliIdentity, uint64_t fileId) override {
    return m_OStoreDB.markArchiveRequestForDeletion(cliIdentity, fileId);
  }

  virtual void deleteRetrieveRequest(const common::dataStructures::SecurityIdentity& cliIdentity, const std::string& remoteFile) override {
    m_OStoreDB.deleteRetrieveRequest(cliIdentity, remoteFile);
  }

  std::map<std::string, std::list<common::dataStructures::ArchiveJob> > getArchiveJobs() const override {
    return m_OStoreDB.getArchiveJobs();
  }
  
  std::list<cta::common::dataStructures::ArchiveJob> getArchiveJobs(const std::string& tapePoolName) const override {
    return m_OStoreDB.getArchiveJobs(tapePoolName);
  }

  virtual std::map<Tape, std::list<RetrieveRequestDump> > getRetrieveRequests() const override {
    return m_OStoreDB.getRetrieveRequests();
  }

  virtual std::list<RetrieveRequestDump> getRetrieveRequestsByVid(const std::string& vid) const override {
    return m_OStoreDB.getRetrieveRequestsByVid(vid);
  }
  
  std::list<RetrieveRequestDump> getRetrieveRequestsByRequester(const std::string& requester) const override {
    return m_OStoreDB.getRetrieveRequestsByRequester(requester);
  }

  
  virtual std::unique_ptr<TapeMountDecisionInfo> getMountInfo() override {
    return m_OStoreDB.getMountInfo();
  }

  virtual void queue(const common::dataStructures::RetrieveRequest& rqst,
    const common::dataStructures::RetrieveFileQueueCriteria &criteria) override {
    m_OStoreDB.queue(rqst, criteria);
  }
  
  virtual std::list<cta::common::DriveState> getDriveStates() const override {
    return m_OStoreDB.getDriveStates();
  }
private:
  std::unique_ptr <cta::objectstore::Backend> m_backend;
  cta::OStoreDB m_OStoreDB;
  objectstore::Agent m_agent;
};

template <>
OStoreDBWrapper<cta::objectstore::BackendVFS>::OStoreDBWrapper(
        const std::string &context, const std::string &URL) :
m_backend(new cta::objectstore::BackendVFS()),
m_OStoreDB(*m_backend), m_agent(*m_backend) {
  // We need to populate the root entry before using.
  objectstore::RootEntry re(*m_backend);
  re.initialize();
  re.insert();
  objectstore::ScopedExclusiveLock rel(re);
  re.fetch();
  m_agent.generateName("OStoreDBFactory");
  m_agent.initialize();
  objectstore::EntryLog cl("user0", "systemhost", time(NULL));
  re.addOrGetAgentRegisterPointerAndCommit(m_agent, cl);
  rel.release();
  m_agent.insertAndRegisterSelf();
  rel.lock(re);
  re.addOrGetDriveRegisterPointerAndCommit(m_agent, cl);
  re.addOrGetSchedulerGlobalLockAndCommit(m_agent, cl);
  rel.release();
  m_OStoreDB.setAgent(m_agent);
}

template <>
OStoreDBWrapper<cta::objectstore::BackendRados>::OStoreDBWrapper(
        const std::string &context, const std::string &URL) :
m_backend(cta::objectstore::BackendFactory::createBackend(URL).release()),
m_OStoreDB(*m_backend), m_agent(*m_backend) {
  // We need to first clean up possible left overs in the pool
  auto l = m_backend->list();
  for (auto o=l.begin(); o!=l.end(); o++) {
    try {
      m_backend->remove(*o);
    } catch (std::exception &) {}
  }
  // We need to populate the root entry before using.
  objectstore::RootEntry re(*m_backend);
  re.initialize();
  re.insert();
  objectstore::ScopedExclusiveLock rel(re);
  re.fetch();
  m_agent.generateName("OStoreDBFactory");
  m_agent.initialize();
  objectstore::EntryLog cl("user0", "systemhost", time(NULL));
  re.addOrGetAgentRegisterPointerAndCommit(m_agent, cl);
  rel.release();
  m_agent.insertAndRegisterSelf();
  rel.lock(re);
  re.addOrGetDriveRegisterPointerAndCommit(m_agent, cl);
  re.addOrGetSchedulerGlobalLockAndCommit(m_agent, cl);
  rel.release();
  m_OStoreDB.setAgent(m_agent);
}

}

/**
 * A concrete implementation of a scheduler database factory that creates mock
 * scheduler database objects.
 */
template <class BackendType>
class OStoreDBFactory: public SchedulerDatabaseFactory {
public:
  /**
   * Constructor
   */
  OStoreDBFactory(const std::string & URL = ""): m_URL(URL) {}
  
  /**
   * Destructor.
   */
  ~OStoreDBFactory() throw() {}
  
  /**
   * Returns a newly created scheduler database object.
   *
   * @return A newly created scheduler database object.
   */
  std::unique_ptr<SchedulerDatabase> create() const {
    return std::unique_ptr<SchedulerDatabase>(new OStoreDBWrapper<BackendType>("UnitTest", m_URL));
  }
  
  private:
    std::string m_URL;
}; // class OStoreDBFactory

} // namespace cta
