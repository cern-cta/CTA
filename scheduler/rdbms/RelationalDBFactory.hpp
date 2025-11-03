/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/dataStructures/SecurityIdentity.hpp"
#include "common/log/DummyLogger.hpp"
#include "rdbms/Login.hpp"
#include "scheduler/LogicalLibrary.hpp"
#include "scheduler/rdbms/RelationalDB.hpp"
#include "scheduler/rdbms/TemporaryPostgresInstance.hpp"
#include "scheduler/RetrieveRequestDump.hpp"
#include "scheduler/SchedulerDatabaseFactory.hpp"
#include "scheduler/rdbms/RelationalDB.hpp"

#include <memory>
#include <string>

namespace cta::catalogue {

class Catalogue;
}

namespace cta {

/**
 * Wrapper for RelationalDB that provides schema isolation for tests.
 * Each instance gets its own PostgreSQL schema, providing test isolation
 * similar to how ObjectStore creates a fresh backend per test.
 */
class RelationalDBWrapper : public SchedulerDatabaseDecorator {
private:
  std::unique_ptr<cta::log::Logger> m_logger;
  cta::catalogue::Catalogue& m_catalogue;
  RelationalDB m_RelationalDB;
    std::string m_schemaName;

public:
  RelationalDBWrapper(const std::string &ownerId,
                         std::unique_ptr<cta::log::Logger> logger,
                         catalogue::Catalogue &catalogue,
                         const rdbms::Login &login,
                         const uint64_t nbConns,
                         const std::string &schemaName = "public") :
      SchedulerDatabaseDecorator(m_RelationalDB),
      m_logger(std::move(logger)), m_catalogue(catalogue),
      m_RelationalDB(ownerId, *logger, catalogue, login, nbConns),
      m_schemaName(schemaName)
   {
     // empty
   }

  ~RelationalDBWrapper() noexcept {
    // Drop the schema we created for test isolation
    // (but never drop "public" schema)
    if (!m_schemaName.empty() && m_schemaName != "public") {
      if (schedulerdb::g_tempPostgresEnv) {
        schedulerdb::g_tempPostgresEnv->dropSchema(m_schemaName);
      }
    }
  }

  std::string getSchemaName() const { return m_schemaName; }
};

/**
 * A concrete implementation of a scheduler database factory that creates
 * RelationalDB instances for testing.
 *
 * This factory provides test isolation through per-test PostgreSQL schemas:
 * - Each call to create() generates a unique schema
 * - The schema is automatically created with scheduler tables
 * - The schema is dropped when the RelationalDBWrapper is destroyed
 *
 * This mimics the ObjectStore approach where each test gets a fresh backend.
 */
class RelationalDBFactory : public SchedulerDatabaseFactory {
public:
  /**
   * Constructor
   */
  explicit RelationalDBFactory(const std::string& URL = "") : m_URL(URL) {}

  /**
   * Destructor.
   */
  ~RelationalDBFactory() noexcept {}

  /**
   * Returns a newly created scheduler database object with its own isolated schema.
   *
   * If the global TemporaryPostgresEnvironment (g_tempPostgresEnv) is set:
   * 1. Generates a unique schema name (e.g., "test_12345_0")
   * 2. Creates the schema in PostgreSQL
   * 3. Creates all scheduler tables in that schema
   * 4. Returns a wrapper that will drop the schema on destruction
   *
   * Otherwise, falls back to dummy credentials (which will fail to connect).
   *
   * @return A newly created scheduler database object with isolated schema.
   */
  std::unique_ptr<SchedulerDatabase> create(std::unique_ptr<cta::catalogue::Catalogue>& catalogue) const {
    auto dummylogger = std::make_unique<cta::log::DummyLogger>("", "");
    auto logger = std::unique_ptr<cta::log::Logger>(std::move(dummylogger));

    if (schedulerdb::g_tempPostgresEnv) {
      // Create a unique schema for this test (like ObjectStore creates a fresh backend)
      std::string schemaName = schedulerdb::g_tempPostgresEnv->generateUniqueSchemaName();

      // Create the schema
      schedulerdb::g_tempPostgresEnv->createSchema(schemaName);

      // Create scheduler tables in this schema
      schedulerdb::g_tempPostgresEnv->createSchedulerSchemaInSchema(schemaName);

      // Get login with this schema as the namespace
      cta::rdbms::Login login = schedulerdb::g_tempPostgresEnv->getLogin(schemaName);

      // Create wrapper that will drop the schema on destruction
      auto pgwrapper = std::make_unique<RelationalDBWrapper>(
        "UnitTest", std::move(logger), *catalogue, login, 2, schemaName);

      return std::unique_ptr<SchedulerDatabase>(std::move(pgwrapper));

    } else {
      // Fallback to dummy credentials (tests will fail)
      cta::rdbms::Login login(cta::rdbms::Login::DBTYPE_POSTGRESQL,
                              "user", "password", "", "host", 0, "public");

      auto pgwrapper = std::make_unique<RelationalDBWrapper>(
        "UnitTest", std::move(logger), *catalogue, login, 2, "public");

      return std::unique_ptr<SchedulerDatabase>(std::move(pgwrapper));
    }
  }

private:
  std::string m_URL;
};  // class RelationalDBFactory

}  // namespace cta
