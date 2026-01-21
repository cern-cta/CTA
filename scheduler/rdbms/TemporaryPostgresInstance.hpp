/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "rdbms/Login.hpp"

#include <gtest/gtest.h>
#include <string>
#include <vector>

namespace cta::schedulerdb {

/**
  * GoogleTest Environment that manages a temporary PostgreSQL instance for tests.
  *
  * This environment:
  * 1. Creates a temporary data directory
  * 2. Initializes a fresh PostgreSQL database (initdb)
  * 3. Starts PostgreSQL on port 15432
  * 4. Provides connection info to tests
  * 5. Stops PostgreSQL and cleans up after tests
  *
  * Requirements:
  * - PostgreSQL must be installed (pg_ctl, initdb, psql in PATH or at known locations)
  * - Write access to /tmp or /dev/shm

  * We decided to start a separate PostgreSQL instance as part of the unit test binary.
  * Managing the PostgreSQL instance within the test binary itself, rather than externally
  * via a provided connection string, simplifies unit test execution and maintenance when
  * switching between Objectstore and PostgreSQL compilation options for the backend.
  * 
  * Many of the existing unit tests already have external dependencies and therefore
  * function more as integration tests than pure unit tests. This is consistent with the
  * current testing approach: the Objectstore backend depends on VFS, the Catalogue depends
  * on SQLite, and we are now adding PostgreSQL for the relational database scheduler backend
  * within the same framework.
  * 
  * Avoiding this additional dependency by using an in-memory SQLite database, as done for the
  * Catalogue, would require a substantial amount of work. Most queries would need to be rewritten
  * for SQLite, and it would not be possible to test the same production logic because the scheduler
  * relies on PostgreSQL-specific features not available in SQLite. Given the small size of the
  * development team, introducing a new, fully isolated unit testing framework without these dependencies
  * is not feasible at this time.
  */
class TemporaryPostgresEnvironment : public ::testing::Environment {
public:
  /* This is used to avoid shell injection */
  int runCommand(const std::vector<std::string>& args);

  TemporaryPostgresEnvironment();
  ~TemporaryPostgresEnvironment() override;

  /**
   * Called before any tests run.
   * Initializes and starts a temporary PostgreSQL instance.
   */
  void SetUp() override;

  /**
   * Called after all tests complete.
   * Cleanup is handled by the destructor (RAII).
   */
  void TearDown() override;

  /**
   * Get a Login object configured to connect to the test database.
   *
   * @param schemaName Optional schema name. If not provided, uses "scheduler".
   */
  rdbms::Login getLogin(const std::string& schemaName = "scheduler") const;

  /**
   * Get connection string
   */
  std::string getConnectionString() const {
    return "postgresql://" + m_username + "@localhost:" + std::to_string(m_port) + "/" + m_database;
  }

  int getPort() const { return m_port; }

  std::string getDatabase() const { return m_database; }

  std::string getDataDir() const { return m_dataDir; }

  /**
   * Generate a unique schema name for test isolation.
   * Each call returns a different schema name.
   */
  std::string generateUniqueSchemaName();

  /**
   * Drop a schema from the database.
   */
  void dropSchema(const std::string& schemaName);

  /**
   * Create CTA scheduler schema in the test database
   */
  void createSchedulerSchema(const std::string& username, const std::string& schemaName);

private:
  /**
   * Find PostgreSQL binaries (pg_ctl, initdb, psql)
   */
  void findPostgresBinaries();

  /**
   * Initialize database cluster with initdb
   */
  void initializeDatabase();

  /**
   * Start PostgreSQL server
   */
  void startPostgres();

  /**
   * Wait for PostgreSQL to accept connections
   */
  void waitForPostgresReady();

  /**
   * Create test database
   */
  void createTestDatabase();

  /**
   * Stop PostgreSQL server (noexcept so safe to call from destructor)
   */
  void stopPostgres() noexcept;

  /**
   * Clean up temporary directory (noexcept - safe to call from destructor)
   */
  void cleanup() noexcept;

  std::string m_dataDir;
  std::string m_logFile;
  std::string m_pgCtl;
  std::string m_initdb;
  std::string m_psql;
  int m_port;
  std::string m_username;
  std::string m_database;
  std::string m_namespace;
  bool m_useShm;  // Use /dev/shm (tmpfs) for in-memory storage
};

/**
 * Global pointer to access the environment from tests
 */
inline TemporaryPostgresEnvironment* g_tempPostgresEnv = nullptr;

}  // namespace cta::schedulerdb
