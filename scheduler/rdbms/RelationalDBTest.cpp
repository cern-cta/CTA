/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "scheduler/rdbms/RelationalDB.hpp"

#include "catalogue/dummy/DummyCatalogue.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/Logger.hpp"
#include "common/log/StringLogger.hpp"
#include "scheduler/rdbms/RelationalDBTest.hpp"
#include "scheduler/rdbms/RelationalDBTestFactory.hpp"

#include <limits>
#include <list>
#include <memory>
#include <string>
#include <utility>

namespace unitTests {

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
    auto psdb = std::move(factory.create(m_catalogue));
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

TEST_P(RelationalDBTest, DISABLED_getBatchArchiveJob) {
  ASSERT_EQ(0, 1);
}

static cta::RelationalDBTestFactory RelationalDBTestFactoryStatic;
INSTANTIATE_TEST_CASE_P(RelationalDBTest,
                        RelationalDBTest,
                        ::testing::Values(RelationalDBTestParams(&RelationalDBTestFactoryStatic)));

}  // namespace unitTests
