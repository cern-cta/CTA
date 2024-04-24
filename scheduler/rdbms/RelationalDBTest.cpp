/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
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

#include <limits>
#include <list>
#include <memory>
#include <string>
#include <utility>

#include "catalogue/dummy/DummyCatalogue.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/Logger.hpp"
#include "common/log/StringLogger.hpp"
#include "scheduler/rdbms/RelationalDB.hpp"
#include "scheduler/rdbms/RelationalDBFactory.hpp"
#include "scheduler/rdbms/RelationalDBTest.hpp"

namespace unitTests {

/**
 * This structure is used to parameterize RelationalDB database tests.
 */
struct RelationalDBTestParams {
  cta::SchedulerDatabaseFactory &dbFactory;

  explicit RelationalDBTestParams(cta::SchedulerDatabaseFactory *dbFactory) :
    dbFactory(*dbFactory) {}
};  // struct RelationalDBTestParams


/**
 * The RelationalDB database test is a parameterized test.  It takes an
 * RelationalDB database factory as a parameter.
 */
class RelationalDBTest: public
  ::testing::TestWithParam<RelationalDBTestParams> {
 public:
  RelationalDBTest() noexcept { }

  class FailedToGetDatabase: public std::exception {
   public:
    const char *what() const noexcept {
      return "Failed to get scheduler database";
    }
  };

  virtual void SetUp() {
    // We do a deep reference to the member as the C++ compiler requires the function to be
    // already defined if called implicitly.
    const auto &factory = GetParam().dbFactory;
    m_catalogue = std::make_unique<cta::catalogue::DummyCatalogue>();
    // Get the PostgresSched DB from the factory.
    auto psdb = std::move(factory.create(m_catalogue));
    // Make sure the type of the SchedulerDatabase is correct (it should be an RelationalDBWrapper).
    dynamic_cast<cta::RelationalDBWrapper *> (psdb.get());
    // We know the cast will not fail, so we can safely do it (otherwise we could leak memory).
    m_db.reset(dynamic_cast<cta::RelationalDBWrapper *> (psdb.release()));
  }

  virtual void TearDown() {
    m_db.reset();
    m_catalogue.reset();
  }

  cta::RelationalDBWrapper &getDb() {
    cta::RelationalDBWrapper *const ptr = m_db.get();
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
  RelationalDBTest(const RelationalDBTest &) = delete;

  // Prevent assignment
  RelationalDBTest & operator= (const RelationalDBTest &) = delete;

  std::unique_ptr<cta::RelationalDBWrapper> m_db;

  std::unique_ptr<cta::catalogue::Catalogue> m_catalogue;
};  // class RelationalDBTest

TEST_P(RelationalDBTest, getBatchArchiveJob) {
  ASSERT_EQ(0,1);
}

static cta::RelationalDBFactory RelationalDBFactoryStatic;
INSTANTIATE_TEST_CASE_P(RelationalDBTest, RelationalDBTest,
    ::testing::Values(RelationalDBTestParams(&RelationalDBFactoryStatic)));

}  // namespace unitTests
