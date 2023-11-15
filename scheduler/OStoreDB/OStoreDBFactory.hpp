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

#include <memory>

#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/log/DummyLogger.hpp"
#include "objectstore/Agent.hpp"
#include "objectstore/AgentReference.hpp"
#include "objectstore/BackendFactory.hpp"
#include "objectstore/BackendRados.hpp"
#include "objectstore/BackendVFS.hpp"
#include "objectstore/RootEntry.hpp"
#include "scheduler/LogicalLibrary.hpp"
#include "scheduler/OStoreDB/OStoreDB.hpp"
#include "scheduler/RetrieveRequestDump.hpp"
#include "scheduler/SchedulerDatabaseFactory.hpp"

namespace cta {

namespace catalogue {
class Catalogue;
}

namespace objectstore {
  class Backend;

/**
 * As slight variation of the SchedulerDatabase interface allowing
 * access to the underlying backend (this is hence OStoreDB specific).
 * This interface allows tests to go behind the scenes and create "broken" situations
 * and test recovery.
 */

class OStoreDBWrapperInterface: public SchedulerDatabaseDecorator {
public:
  OStoreDBWrapperInterface(SchedulerDatabase &db) : SchedulerDatabaseDecorator(db) {}

  virtual objectstore::Backend & getBackend() = 0;
  virtual objectstore::AgentReference & getAgentReference() = 0;
  virtual cta::OStoreDB & getOstoreDB() = 0;

  //! Create a new agent to allow tests to continue after garbage collection
  virtual void replaceAgent(objectstore::AgentReference * agentReferencePtr) = 0;
};

} // namespace objectstore

namespace {

/**
 * A self contained class providing a "backend+SchedulerDB" in a box,
 * allowing creation (and deletion) of an object store for each instance of the
 * of the unit tests, as they require a fresh object store for each unit test.
 * It can be instantiated with both types of backends
 */
template <class BackendType>
class OStoreDBWrapper: public cta::objectstore::OStoreDBWrapperInterface {
public:
  OStoreDBWrapper(const std::string &context, std::unique_ptr<cta::catalogue::Catalogue>& catalogue, const std::string &URL = "");

  ~OStoreDBWrapper() throw () {}

  objectstore::Backend& getBackend() override { return *m_backend; }

  objectstore::AgentReference& getAgentReference() override { return *m_agentReferencePtr; }

  void replaceAgent(objectstore::AgentReference *agentReferencePtr) override {
    m_agentReferencePtr.reset(agentReferencePtr);
    objectstore::RootEntry re(*m_backend);
    objectstore::ScopedExclusiveLock rel(re);
    re.fetch();
    objectstore::Agent agent(m_agentReferencePtr->getAgentAddress(), *m_backend);
    agent.initialize();
    objectstore::EntryLogSerDeser cl("user0", "systemhost", time(nullptr));
    log::LogContext lc(*m_logger);
    re.addOrGetAgentRegisterPointerAndCommit(*m_agentReferencePtr, cl, lc);
    rel.release();
    agent.insertAndRegisterSelf(lc);
    rel.lock(re);
    re.fetch();
    re.addOrGetDriveRegisterPointerAndCommit(*m_agentReferencePtr, cl);
    re.addOrGetSchedulerGlobalLockAndCommit(*m_agentReferencePtr, cl);
    rel.release();
    m_OStoreDB.setAgentReference(m_agentReferencePtr.get());
  }

  cta::OStoreDB& getOstoreDB() override { return m_OStoreDB; }

private:
  std::unique_ptr <cta::log::Logger> m_logger;
  std::unique_ptr <cta::objectstore::Backend> m_backend;
  std::unique_ptr <cta::catalogue::Catalogue> & m_catalogue;
  cta::OStoreDB m_OStoreDB;
  std::unique_ptr<objectstore::AgentReference> m_agentReferencePtr;
};

template <>
OStoreDBWrapper<cta::objectstore::BackendVFS>::OStoreDBWrapper(
        const std::string &context, std::unique_ptr<cta::catalogue::Catalogue> & catalogue, const std::string &URL) :
OStoreDBWrapperInterface(m_OStoreDB),
m_logger(new cta::log::DummyLogger("", "")), m_backend(URL.empty() ? new cta::objectstore::BackendVFS() : new cta::objectstore::BackendVFS(URL)),
m_catalogue(catalogue),
m_OStoreDB(*m_backend, *m_catalogue, *m_logger),
  m_agentReferencePtr(new objectstore::AgentReference("OStoreDBFactory", *m_logger))
{
  // We need to populate the root entry before using.
  objectstore::RootEntry re(*m_backend);
  re.initialize();
  re.insert();
  objectstore::ScopedExclusiveLock rel(re);
  re.fetch();
  objectstore::Agent agent(m_agentReferencePtr->getAgentAddress(), *m_backend);
  agent.initialize();
  objectstore::EntryLogSerDeser cl("user0", "systemhost", time(nullptr));
  log::LogContext lc(*m_logger);
  re.addOrGetAgentRegisterPointerAndCommit(*m_agentReferencePtr, cl, lc);
  rel.release();
  agent.insertAndRegisterSelf(lc);
  rel.lock(re);
  re.fetch();
  re.addOrGetDriveRegisterPointerAndCommit(*m_agentReferencePtr, cl);
  re.addOrGetSchedulerGlobalLockAndCommit(*m_agentReferencePtr, cl);
  rel.release();
  m_OStoreDB.setAgentReference(m_agentReferencePtr.get());
}

} // namespace

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
  std::unique_ptr<SchedulerDatabase> create(std::unique_ptr<cta::catalogue::Catalogue>& catalogue) const {
    return std::unique_ptr<SchedulerDatabase>(new OStoreDBWrapper<BackendType>("UnitTest", catalogue, m_URL));
  }

  private:
    std::string m_URL;
}; // class OStoreDBFactory

} // namespace cta
