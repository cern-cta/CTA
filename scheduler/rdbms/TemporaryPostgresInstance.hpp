/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "rdbms/ConnPool.hpp"
#include "rdbms/Login.hpp"
#include "scheduler/rdbms/schema/PostgresSchedulerSchema.hpp"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <fstream>
#include <gtest/gtest.h>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>

namespace cta::schedulerdb {

/**
 * GoogleTest Environment that manages a temporary PostgreSQL instance for tests.
 *
 * This environment:
 * 1. Creates a temporary data directory
 * 2. Initializes a fresh PostgreSQL database (initdb)
 * 3. Starts PostgreSQL on a free port
 * 4. Provides connection info to tests
 * 5. Stops PostgreSQL and cleans up after tests
 *
 * Requirements:
 * - PostgreSQL must be installed (pg_ctl, initdb, psql in PATH or at known locations)
 * - Write access to /tmp or /dev/shm
 *
 * Advantages over Docker:
 * - No Docker dependency
 * - Faster startup
 * - Uses system PostgreSQL installation
 * - Easier debugging
 *
 * Usage:
 *
 *   In your test main (e.g., tests/unit_tests.cpp):
 *
 *   int main(int argc, char **argv) {
 *     ::testing::InitGoogleTest(&argc, argv);
 *
 *     cta::schedulerdb::g_tempPostgresEnv =
 *         new cta::schedulerdb::TemporaryPostgresEnvironment();
 *     ::testing::AddGlobalTestEnvironment(g_tempPostgresEnv);
 *
 *     return RUN_ALL_TESTS();
 *   }
 *
 *   In your test fixtures:
 *
 *   void SetUp() override {
 *     auto login = g_tempPostgresEnv->getLogin();
 *     m_db = std::make_unique<RelationalDB>(..., login, ...);
 *   }
 */
class TemporaryPostgresEnvironment : public ::testing::Environment {
public:
  TemporaryPostgresEnvironment()
      : m_port(0),  // Will be auto-assigned
        m_username("cta_test"),
        m_database("cta_scheduler"),
        m_namespace("public"),
        m_useShm(false)  // Use /tmp instead of /dev/shm (containers often have small shm)
  {
    // Generate unique temp directory name
    char tmpTemplate[] = "/tmp/cta-pg-test-XXXXXX";
    char shmTemplate[] = "/dev/shm/cta-pg-test-XXXXXX";

    char* tmpDir = mkdtemp(m_useShm ? shmTemplate : tmpTemplate);
    if (!tmpDir) {
      // Fallback to /tmp if /dev/shm fails
      tmpDir = mkdtemp(tmpTemplate);
      m_useShm = false;
    }

    if (!tmpDir) {
      throw std::runtime_error("Failed to create temporary directory");
    }

    m_dataDir = tmpDir;
    m_logFile = m_dataDir + "/postgres.log";
  }

  ~TemporaryPostgresEnvironment() override = default;

  /**
   * Called before any tests run.
   * Initializes and starts a temporary PostgreSQL instance.
   */
  void SetUp() override {
    std::cout << "\n=== TemporaryPostgresEnvironment Setup ===" << std::endl;
    std::cout << "Creating temporary PostgreSQL instance..." << std::endl;

    // Clean up any leftovers from previous failed tests BEFORE creating new temp dir
    std::cout << "Cleaning up leftovers from previous tests..." << std::endl;
    system("pkill -9 -f \"postgres.*cta-pg-test\" 2>/dev/null || true");
    if (m_useShm) {
      system("rm -rf /dev/shm/cta-pg-test-* 2>/dev/null || true");
    } else {
      system("rm -rf /tmp/cta-pg-test-* 2>/dev/null || true");
    }

    std::cout << "Data directory: " << m_dataDir;
    if (m_useShm) {
      std::cout << " (in-memory tmpfs)";
    }
    std::cout << std::endl;

    try {
      // Find PostgreSQL binaries
      findPostgresBinaries();

      // Initialize database cluster
      initializeDatabase();

      // Find available port
      m_port = findAvailablePort();

      // Start PostgreSQL server
      startPostgres();

      // Wait for server to be ready
      waitForPostgresReady();

      // Create test database
      createTestDatabase();

      // Create CTA scheduler schema
      createSchedulerSchema();

      std::cout << "✓ Temporary Postgres ready" << std::endl;
      std::cout << "  Host: localhost" << std::endl;
      std::cout << "  Port: " << m_port << std::endl;
      std::cout << "  Database: " << m_database << std::endl;
      std::cout << "  User: " << m_username << std::endl;
      std::cout << "  Storage: " << (m_useShm ? "tmpfs (RAM)" : "disk") << std::endl;
      std::cout << "  Schema: CTA Scheduler (created)" << std::endl;
      std::cout << "======================================\n" << std::endl;

    } catch (const std::exception& e) {
      std::cerr << "Failed to start temporary Postgres: " << e.what() << std::endl;
      cleanup();
      throw;
    }
  }

  /**
   * Called after all tests complete.
   * Stops PostgreSQL and removes temporary files.
   */
  void TearDown() override {
    std::cout << "\n=== TemporaryPostgresEnvironment Teardown ===" << std::endl;
    std::cout << "Stopping temporary PostgreSQL instance..." << std::endl;

    stopPostgres();
    cleanup();

    std::cout << "✓ Cleanup complete" << std::endl;
    std::cout << "==========================================\n" << std::endl;
  }

  /**
   * Get a Login object configured to connect to the test database.
   *
   * @param schemaName Optional schema name. If not provided, uses "public".
   */
  rdbms::Login getLogin(const std::string& schemaName = "public") const {
    return rdbms::Login(rdbms::Login::DBTYPE_POSTGRESQL,
                        m_username,
                        "",  // No password needed for local Unix socket or trust auth
                        m_database,
                        "localhost",
                        m_port,
                        schemaName  // Schema name as namespace
    );
  }

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
  std::string generateUniqueSchemaName() {
    static std::atomic<uint64_t> counter {0};
    uint64_t id = counter.fetch_add(1);

    std::stringstream ss;
    ss << "test_" << getpid() << "_" << id;
    return ss.str();
  }

  /**
   * Create a new schema in the database.
   * Returns the schema name.
   */
  std::string createSchema(const std::string& schemaName) {
    try {
      auto login = getLogin();
      rdbms::ConnPool connPool(login, 1);
      auto conn = connPool.getConn();

      // Create the schema
      auto stmt = conn.createStmt("CREATE SCHEMA " + schemaName);
      stmt.executeNonQuery();

      conn.commit();

      return schemaName;
    } catch (const std::exception& e) {
      throw std::runtime_error("Failed to create schema " + schemaName + ": " + e.what());
    }
  }

  /**
   * Drop a schema from the database.
   */
  void dropSchema(const std::string& schemaName) {
    if (schemaName.empty() || schemaName == "public") {
      return;  // Don't drop public schema
    }

    try {
      auto login = getLogin();
      rdbms::ConnPool connPool(login, 1);
      auto conn = connPool.getConn();

      // Drop the schema and all its contents
      auto stmt = conn.createStmt("DROP SCHEMA IF EXISTS " + schemaName + " CASCADE");
      stmt.executeNonQuery();

      conn.commit();
    } catch (const std::exception& e) {
      std::cerr << "Warning: Failed to drop schema " << schemaName << ": " << e.what() << std::endl;
      // Don't throw - this is cleanup
    }
  }

  /**
   * Create CTA scheduler schema in the test database
   */
  void createSchedulerSchema();

  /**
   * Create scheduler schema tables in a specific schema.
   */
  void createSchedulerSchemaInSchema(const std::string& schemaName);

  /**
   * Execute multiple SQL statements separated by semicolons
   */
  void executeNonQueries(rdbms::Conn& conn, const std::string& sqlStmts);

private:
  /**
   * Find PostgreSQL binaries (pg_ctl, initdb, psql)
   */
  void findPostgresBinaries() {
    // Try common PostgreSQL binary locations
    const char* pgCtlPaths[] = {"/usr/bin/pg_ctl",
                                "/usr/local/bin/pg_ctl",
                                "/usr/pgsql-15/bin/pg_ctl",
                                "/usr/pgsql-14/bin/pg_ctl",
                                "/usr/pgsql-13/bin/pg_ctl",
                                "/usr/lib/postgresql/15/bin/pg_ctl",
                                "/usr/lib/postgresql/14/bin/pg_ctl",
                                nullptr};

    // First try to find in PATH
    if (system("which pg_ctl >/dev/null 2>&1") == 0) {
      m_pgCtl = "pg_ctl";
      m_initdb = "initdb";
      m_psql = "psql";
      return;
    }

    // Try known locations
    for (int i = 0; pgCtlPaths[i] != nullptr; ++i) {
      if (access(pgCtlPaths[i], X_OK) == 0) {
        m_pgCtl = pgCtlPaths[i];

        // Derive initdb and psql from same directory
        std::string binDir = m_pgCtl.substr(0, m_pgCtl.rfind('/'));
        m_initdb = binDir + "/initdb";
        m_psql = binDir + "/psql";

        if (access(m_initdb.c_str(), X_OK) == 0 && access(m_psql.c_str(), X_OK) == 0) {
          return;
        }
      }
    }

    throw std::runtime_error("PostgreSQL binaries not found. Please install PostgreSQL or ensure "
                             "pg_ctl, initdb, and psql are in PATH.");
  }

  /**
   * Initialize database cluster with initdb
   */
  void initializeDatabase() {
    std::cout << "  Initializing database cluster..." << std::endl;

    // initdb must be run as the postgres user
    // Change ownership of data directory to postgres user first
    std::string chownCmd = "chown -R postgres:postgres " + m_dataDir + " 2>/dev/null";
    system(chownCmd.c_str());

    std::string cmd = "runuser -u postgres -- " + m_initdb + " -D " + m_dataDir + " -U " + m_username
                      + " --no-locale --encoding=UTF8" + " -A trust" +  // Trust authentication for localhost
                      " >/dev/null 2>&1";

    int result = system(cmd.c_str());
    if (result != 0) {
      throw std::runtime_error("initdb failed with code " + std::to_string(result));
    }

    // Configure PostgreSQL for testing (speed over durability)
    std::string confFile = m_dataDir + "/postgresql.conf";
    std::ofstream conf(confFile, std::ios::app);
    if (conf.is_open()) {
      conf << "\n# Test optimizations\n";
      conf << "fsync = off\n";
      conf << "synchronous_commit = off\n";
      conf << "full_page_writes = off\n";
      conf << "max_connections = 100\n";
      conf << "shared_buffers = 128MB\n";
      conf << "unix_socket_directories = '" << m_dataDir << "'\n";
      conf.close();
    }

    std::cout << "  ✓ Database cluster initialized" << std::endl;
  }

  /**
   * Find an available port for PostgreSQL
   */
  int findAvailablePort() {
    // Simple approach: try ports starting from 15432
    // In production you might want to bind to port 0 and let OS assign
    for (int port = 15432; port < 15532; ++port) {
      std::string checkCmd = "nc -z localhost " + std::to_string(port) + " >/dev/null 2>&1";
      if (system(checkCmd.c_str()) != 0) {
        return port;  // Port is available
      }
    }

    // Fallback: just use 15432 and hope for the best
    return 15432;
  }

  /**
   * Start PostgreSQL server
   */
  void startPostgres() {
    std::cout << "  Starting PostgreSQL on port " << m_port << "..." << std::endl;

    // PostgreSQL must be started as the postgres user
    std::string cmd = "runuser -u postgres -- " + m_pgCtl + " -D " + m_dataDir + " -o \"-p " + std::to_string(m_port)
                      + "\"" + " -l " + m_logFile + " start >/dev/null 2>&1";

    std::cout << "Ran command to start postgres: " << cmd << std::endl;

    int result = system(cmd.c_str());
    if (result != 0) {
      // Try to get log contents for debugging
      std::ifstream log(m_logFile);
      std::string logContents((std::istreambuf_iterator<char>(log)), std::istreambuf_iterator<char>());
      throw std::runtime_error("Failed to start PostgreSQL. Log:\n" + logContents);
    }

    std::cout << "  ✓ PostgreSQL started" << std::endl;
  }

  /**
   * Wait for PostgreSQL to accept connections
   */
  void waitForPostgresReady() {
    std::cout << "  Waiting for PostgreSQL to accept connections..." << std::endl;

    const int maxAttempts = 30;
    for (int i = 0; i < maxAttempts; ++i) {
      // Run psql as postgres user
      std::string checkCmd = "runuser -u postgres -- " + m_psql + " -h localhost -p " + std::to_string(m_port) + " -U "
                             + m_username + " -d postgres -c 'SELECT 1;' >/dev/null 2>&1";

      if (system(checkCmd.c_str()) == 0) {
        std::cout << "  ✓ PostgreSQL ready after ~" << i << " second(s)" << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        return;
      }

      if (i < maxAttempts - 1) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
      }
    }

    // Failed to connect - get logs
    std::ifstream log(m_logFile);
    std::string logContents((std::istreambuf_iterator<char>(log)), std::istreambuf_iterator<char>());

    throw std::runtime_error("PostgreSQL failed to become ready. Log:\n" + logContents);
  }

  /**
   * Create test database
   */
  void createTestDatabase() {
    // Run psql as postgres user to create database
    std::string cmd = "runuser -u postgres -- " + m_psql + " -h localhost -p " + std::to_string(m_port) + " -U "
                      + m_username + " -d postgres -c 'CREATE DATABASE " + m_database + ";' " + ">/dev/null 2>&1";

    int result = system(cmd.c_str());
    if (result != 0) {
      // Database might already exist, that's ok
    }
  }

  /**
   * Stop PostgreSQL server
   */
  void stopPostgres() {
    if (m_pgCtl.empty()) {
      std::cout << "PostgreSQL was never started" << std::endl;
      return;  // Never started
    }

    // Stop PostgreSQL as the postgres user
    std::string cmd = "runuser -u postgres -- " + m_pgCtl + " -D " + m_dataDir + " stop -m fast >/dev/null 2>&1";

    std::cout << "about to shut down postgres with the following command " << cmd << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(30));

    system(cmd.c_str());

    // Give it a moment to shut down
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  /**
   * Clean up temporary directory
   */
  void cleanup() {
    if (!m_dataDir.empty() && m_dataDir.find("/cta-pg-test-") != std::string::npos) {
      std::string cmd = "rm -rf " + m_dataDir;
      system(cmd.c_str());
    }
  }

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
