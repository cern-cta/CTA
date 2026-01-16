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
#include <fcntl.h>
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
  int runCommand(const std::vector<std::string>& args) {
    if (args.empty()) {
      throw std::invalid_argument("No arguments provided to runCommand!");
    }
    pid_t pid = fork();
    if (pid < 0) {
      throw std::runtime_error("Failed to fork!");
    }
    if (pid == 0) {
      /* child process, execute command */
      int fd = open("/dev/null", O_WRONLY);
      if (fd < 0) {
        _exit(126);  // 126: command invoked cannot execute
      }

      dup2(fd, STDOUT_FILENO);  // stdout -> /dev/null
      dup2(fd, STDERR_FILENO);  // stderr -> /dev/null
      close(fd);
      // convert args to char* for exec
      std::vector<char*> argv;
      for (const auto& arg : args) {
        argv.push_back(const_cast<char*>(arg.c_str()));
      }
      argv.push_back(nullptr);       // null-terminated
      execvp(argv[0], argv.data());  // will not return unless error occurs
      _exit(EXIT_FAILURE);
    } else {
      // parent
      int status;
      waitpid(pid, &status, 0);
      return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
    }
  }

  TemporaryPostgresEnvironment()
      : m_port(15432),
        m_username("cta_test"),
        m_database("cta_scheduler"),
        m_namespace("public"),
        m_useShm(false)  // Use /tmp instead of /dev/shm (containers often have small shm)
  {
    // Generate unique temp directory name
    char tmpTemplate[] = "/tmp/cta-pg-test-XXXXXX";
    char shmTemplate[] = "/dev/shm/cta-pg-test-XXXXXX";

    char* tmpDir = mkdtemp(
      m_useShm ? shmTemplate : tmpTemplate);  // mkdtemp will replace the Xs with actual values to make dir unique

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
    runCommand({"pkill", "-9", "-f", "postgres.*cta-pg-test"});
    if (m_useShm) {
      runCommand({"rm", "-rf", "/dev/shm/cta-pg-test-*"});
    } else {
      runCommand({"rm", "-rf", "/tmp/cta-pg-test-*"});
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

      // Start PostgreSQL server
      startPostgres();

      // Wait for server to be ready
      waitForPostgresReady();

      // Create test database
      createTestDatabase();

      // Create CTA scheduler schema
      createSchedulerSchema();

      std::cout << "  Temporary Postgres ready" << std::endl;
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

    std::cout << "Cleanup complete" << std::endl;
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

private:
  /**
   * Find PostgreSQL binaries (pg_ctl, initdb, psql)
   */
  void findPostgresBinaries() {
    if (system("which pg_ctl >/dev/null 2>&1") == 0) {
      m_pgCtl = "pg_ctl";
      m_initdb = "initdb";
      m_psql = "psql";
      return;
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
    runCommand({"chown", "-R", "postgres:postgres", m_dataDir});

    int result = runCommand({"runuser",
                             "-u",
                             "postgres",
                             "--",
                             m_initdb,
                             "-D",
                             m_dataDir,
                             "-U",
                             m_username,
                             "--no-locale",
                             "--encoding=UTF8",
                             "-A",
                             "trust"});  // Trust authentication for localhost
    if (result != 0) {
      throw std::runtime_error("initdb failed with code " + std::to_string(result));
    }

    // Configure PostgreSQL for testing (speed over durability)
    std::string confFile = m_dataDir + "/postgresql.conf";
    std::ofstream conf(confFile, std::ios::app);  // append to the config file
    if (conf.is_open()) {
      conf << "\n# Test optimizations\n";
      conf << "fsync = off\n";
      conf << "synchronous_commit = off\n";
      conf << "full_page_writes = off\n";
      conf << "max_connections = 100\n";
      conf << "shared_buffers = 64MB\n";
      conf << "unix_socket_directories = '" << m_dataDir << "'\n";
      conf.close();
    }

    std::cout << "  Database cluster initialized" << std::endl;
  }

  /**
   * Start PostgreSQL server
   */
  void startPostgres() {
    std::cout << "  Starting PostgreSQL on port " << m_port << "..." << std::endl;

    // PostgreSQL must be started as the postgres user
    int result = runCommand({"runuser",
                             "-u",
                             "postgres",
                             "--",
                             m_pgCtl,
                             "-D",
                             m_dataDir,
                             "-o",
                             "\"-p" + std::to_string(m_port) + "\"",
                             "-l",
                             m_logFile,
                             "start"});

    if (result != 0) {
      throw std::runtime_error("Failed to start PostgreSQL.");
    }

    std::cout << "  PostgreSQL started" << std::endl;
  }

  /**
   * Wait for PostgreSQL to accept connections
   */
  void waitForPostgresReady() {
    std::cout << "  Waiting for PostgreSQL to accept connections..." << std::endl;

    const int maxAttempts = 30;
    int result;
    for (int i = 0; i < maxAttempts; ++i) {
      // Run psql as postgres user
      result = runCommand({"runuser",
                           "-u",
                           "postgres",
                           "--",
                           m_psql,
                           "-h",
                           "localhost",
                           "-p",
                           std::to_string(m_port),
                           "-U",
                           m_username,
                           "-d",
                           "postgres",
                           "-c",
                           "SELECT 1;"});

      if (result == 0) {
        std::cout << "  PostgreSQL ready after ~" << i << " second(s)" << std::endl;
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        return;
      }

      if (i < maxAttempts - 1) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
      }
    }

    throw std::runtime_error("PostgreSQL failed to become ready.");
  }

  /**
   * Create test database
   */
  void createTestDatabase() {
    // Run psql as postgres user to create database

    int result = runCommand({"runuser",
                             "-u",
                             "postgres",
                             "--",
                             m_psql,
                             "-h",
                             "localhost",
                             "-p",
                             std::to_string(m_port),
                             "-U",
                             m_username,
                             "-d",
                             "postgres",
                             "-c",
                             "CREATE DATABASE " + m_database + ";"});

    if (result != 0) {
      // Database might already exist, that's ok
      std::cout << "database not created" << std::endl;
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
    int result = runCommand({"runuser", "-u", "postgres", "--", m_pgCtl, "-D", m_dataDir, "stop", "-m", "fast"});
    if (result != 0) {
      throw std::runtime_error("Failed to shutdown postgres");
    }
    // Give it a moment to shut down
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  /**
   * Clean up temporary directory
   */
  void cleanup() {
    if (!m_dataDir.empty() && m_dataDir.find("/cta-pg-test-") != std::string::npos) {
      runCommand({"rm", "-rf", m_dataDir});
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
