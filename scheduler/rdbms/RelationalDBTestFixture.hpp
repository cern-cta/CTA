/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/TapeForWriting.hpp"
#include "catalogue/dummy/DummyCatalogue.hpp"
#include "common/checksum/ChecksumBlob.hpp"
#include "common/dataStructures/VirtualOrganization.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/Logger.hpp"
#include "common/log/StringLogger.hpp"
#include "common/utils/utils.hpp"
#include "scheduler/RetrieveMount.hpp"
#include "scheduler/rdbms/RelationalDB.hpp"
#include "scheduler/rdbms/RelationalDBTest.hpp"
#include "scheduler/rdbms/RelationalDBTestFactory.hpp"
#include "scheduler/rdbms/postgres/Enums.hpp"

#include <memory>
#include <string>

// Shared RelationalDBTest fixture and queueing helpers, used by every RelationalDB*Test.cpp
// translation unit. Split out so the test suite can live in several files by topic instead of one
// ever-growing RelationalDBTest.cpp - each file includes this header, defines its own
// TEST_P(RelationalDBTest, ...) cases, and exactly one of them (RelationalDBTest.cpp) carries the
// single INSTANTIATE_TEST_CASE_P for the suite.

namespace unitTests {

inline const uint32_t DISK_FILE_OWNER_UID = 9751;
inline const uint32_t DISK_FILE_GID = 9752;
inline const std::string s_storageClassName = "TestStorageClass";

cta::common::dataStructures::MountPolicy makeMountPolicy(time_t t);

std::unique_ptr<cta::log::Logger> makeLogger();

void queueArchiveJob(cta::SchedulerDatabase& db,
                     cta::log::LogContext& lc,
                     uint64_t fileId,
                     const std::string& tapePool,
                     const std::string& suffix,
                     time_t creationTime);

void queueRetrieveJob(cta::SchedulerDatabase& db,
                      cta::log::LogContext& lc,
                      uint64_t archiveFileId,
                      const std::string& vid,
                      const std::string& suffix,
                      time_t creationTime);

// Queues a 2-copy user archive request, both copies routed to the same tape pool so a single
// mount can dispatch both.
void queueMultiCopyArchiveJob(cta::SchedulerDatabase& db,
                              cta::log::LogContext& lc,
                              uint64_t fileId,
                              const std::string& tapePool,
                              const std::string& suffix,
                              time_t creationTime);

/**
 * This structure is used to parameterize RelationalDB database tests.
 */
struct RelationalDBTestParams {
  cta::SchedulerDatabaseFactory& dbFactory;

  explicit RelationalDBTestParams(cta::SchedulerDatabaseFactory* dbFactory) : dbFactory(*dbFactory) {}
};  // struct RelationalDBTestParams

/**
 * The RelationalDB database test is a parameterized test.  It takes an
 * RelationalDB database factory as a parameter.
 */
class RelationalDBTest : public ::testing::TestWithParam<RelationalDBTestParams> {
public:
  RelationalDBTest() noexcept {}

  class FailedToGetDatabase : public std::exception {
  public:
    const char* what() const noexcept { return "Failed to get scheduler database"; }
  };

  virtual void SetUp() {
    // We do a deep reference to the member as the C++ compiler requires the function to be
    // already defined if called implicitly.
    const auto& factory = GetParam().dbFactory;
    m_catalogue = std::make_unique<cta::catalogue::DummyCatalogue>();
    // Get the PostgresSched DB from the factory.
    auto psdb = factory.create(m_catalogue);
    // Make sure the type of the SchedulerDatabase is correct (it should be an RelationalDBTestWrapper).
    dynamic_cast<cta::RelationalDBTestWrapper*>(psdb.get());
    // We know the cast will not fail, so we can safely do it (otherwise we could leak memory).
    m_db.reset(dynamic_cast<cta::RelationalDBTestWrapper*>(psdb.release()));
  }

  virtual void TearDown() {
    m_db.reset();
    m_catalogue.reset();
  }

  cta::RelationalDBTestWrapper& getDb() {
    cta::RelationalDBTestWrapper* const ptr = m_db.get();
    if (nullptr == ptr) {
      throw FailedToGetDatabase();
    }
    return *ptr;
  }

  static const std::string s_systemHost;
  static const std::string s_adminHost;
  static const std::string s_userHost;

  static const std::string s_system;
  static const std::string s_admin;
  static const std::string s_user;

  static const cta::common::dataStructures::SecurityIdentity s_systemOnSystemHost;

  static const cta::common::dataStructures::SecurityIdentity s_adminOnAdminHost;
  static const cta::common::dataStructures::SecurityIdentity s_adminOnUserHost;

  static const cta::common::dataStructures::SecurityIdentity s_userOnAdminHost;
  static const cta::common::dataStructures::SecurityIdentity s_userOnUserHost;

private:
  // Prevent copying
  RelationalDBTest(const RelationalDBTest&) = delete;

  // Prevent assignment
  RelationalDBTest& operator=(const RelationalDBTest&) = delete;

  std::unique_ptr<cta::RelationalDBTestWrapper> m_db;

  std::unique_ptr<cta::catalogue::Catalogue> m_catalogue;
};  // class RelationalDBTest

}  // namespace unitTests
