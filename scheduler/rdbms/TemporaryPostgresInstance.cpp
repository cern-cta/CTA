/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "scheduler/rdbms/TemporaryPostgresInstance.hpp"

#include "common/utils/utils.hpp"
#include "rdbms/ConnPool.hpp"
#include "scheduler/rdbms/schema/PostgresSchedulerSchema.hpp"

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <sys/wait.h>
#include <system_error>
#include <thread>
#include <unistd.h>

namespace cta::schedulerdb {

/**
 * Execute multiple SQL statements separated by semicolons.
 * This is copied from CreateSchemaCmd::executeNonQueries.
 */
static void executeNonQueries(rdbms::Conn& conn, const std::string& sqlStmts) {
  std::string::size_type searchPos = 0;
  std::string::size_type findResult = std::string::npos;

  while (std::string::npos != (findResult = sqlStmts.find(';', searchPos))) {
    // Calculate the length of the current statement without the trailing ';'
    const std::string::size_type stmtLen = findResult - searchPos;
    const std::string sqlStmt = utils::trimString(sqlStmts.substr(searchPos, stmtLen));
    searchPos = findResult + 1;

    if (0 < sqlStmt.size()) {  // Ignore empty statements
      conn.executeNonQuery(sqlStmt);
    }
  }
}

/**
 * Drop a schema from the database.
 */
void TemporaryPostgresEnvironment::dropSchema(const std::string& schemaName) {
  if (schemaName.empty() || schemaName == "public" || schemaName == "scheduler") {
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

TemporaryPostgresEnvironment::TemporaryPostgresEnvironment()
    : m_port(15432),
      m_username("cta_test"),
      m_database("cta_scheduler"),
      m_namespace("scheduler"),
      m_useShm(false)  // Use /tmp instead of /dev/shm (containers often have small shm)
{
  // Generate unique temp directory name
  char tmpTemplate[] = "/tmp/cta-pg-test-XXXXXX";
  char shmTemplate[] = "/dev/shm/cta-pg-test-XXXXXX";

  char* tmpDir =
    mkdtemp(m_useShm ? shmTemplate : tmpTemplate);  // mkdtemp will replace the Xs with actual values to make dir unique

  if (!tmpDir) {
    throw std::system_error(errno, std::generic_category(), "failed to create temporary directory");
  }

  m_dataDir = tmpDir;
  m_logFile = m_dataDir + "/postgres.log";
}

TemporaryPostgresEnvironment::~TemporaryPostgresEnvironment() {
  stopPostgres();
  cleanup();
}

void TemporaryPostgresEnvironment::SetUp() {
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

    std::cout << "  Temporary Postgres ready" << std::endl;
    std::cout << "  Host: localhost" << std::endl;
    std::cout << "  Port: " << m_port << std::endl;
    std::cout << "  Database: " << m_database << std::endl;
    std::cout << "  User: " << m_username << std::endl;
    std::cout << "  Storage: " << (m_useShm ? "tmpfs (RAM)" : "disk") << std::endl;
    std::cout << "======================================\n" << std::endl;

  } catch (const std::exception& e) {
    std::cerr << "Failed to start temporary Postgres: " << e.what() << std::endl;
    cleanup();
    throw;
  }
}

void TemporaryPostgresEnvironment::TearDown() {
  std::cout << "\n=== TemporaryPostgresEnvironment Teardown ===" << std::endl;
}

/* This is used to avoid shell injection */
int TemporaryPostgresEnvironment::runCommand(const std::vector<std::string>& args) {
  if (args.empty()) {
    throw std::invalid_argument("No arguments provided to runCommand!");
  }
  pid_t pid = fork();
  if (pid < 0) {
    throw std::system_error(errno, std::generic_category(), "failed to fork");
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

/**
 * Find PostgreSQL binaries (pg_ctl, initdb, psql)
 */
void TemporaryPostgresEnvironment::findPostgresBinaries() {
  if (system("command -v pg_ctl >/dev/null 2>&1") == 0) {
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
void TemporaryPostgresEnvironment::initializeDatabase() {
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
void TemporaryPostgresEnvironment::startPostgres() {
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
void TemporaryPostgresEnvironment::waitForPostgresReady() {
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
void TemporaryPostgresEnvironment::createTestDatabase() {
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
 * Stop PostgreSQL server (noexcept so safe to call from destructor)
 */
void TemporaryPostgresEnvironment::stopPostgres() noexcept {
  // Stop PostgreSQL as the postgres user
  try {
    runCommand({"runuser", "-u", "postgres", "--", m_pgCtl, "-D", m_dataDir, "stop", "-m", "fast"});
  } catch (const std::exception& e) {
    std::cerr << "Warning: failed to stop PostgreSQL: " << e.what() << std::endl;
  }
  // Give it a moment to shut down
  std::this_thread::sleep_for(std::chrono::seconds(1));
}

/**
 * Clean up temporary directory (noexcept - safe to call from destructor)
 */
void TemporaryPostgresEnvironment::cleanup() noexcept {
  if (!m_dataDir.empty() && m_dataDir.find("/cta-pg-test-") != std::string::npos) {
    try {
      runCommand({"rm", "-rf", m_dataDir});
    } catch (const std::exception& e) {
      std::cerr << "Warning: failed to cleanup " << m_dataDir << ": " << e.what() << std::endl;
    }
  }
}

/**
  * Create CTA scheduler schema in the test database
  */
void TemporaryPostgresEnvironment::createSchedulerSchema(const std::string& username, const std::string& schemaName) {
  std::cout << "  Creating CTA scheduler schema..." << std::endl;

  try {
    // Get the schema SQL
    PostgresSchedulerSchema schema(username, schemaName);

    // Connect to database and create schema
    auto login = getLogin();
    rdbms::ConnPool connPool(login, 1);
    auto conn = connPool.getConn();

    // Execute schema creation SQL
    executeNonQueries(conn, schema.sql);

    conn.commit();

    std::cout << "  Schema created successfully" << std::endl;

  } catch (const std::exception& e) {
    std::cerr << "  Warning: Failed to create schema: " << e.what() << std::endl;
    // Don't throw - let tests handle missing schema
  }
}

/**
 * Get a Login object configured to connect to the test database.
 *
 * @param schemaName Optional schema name. If not provided, uses "public".
 */
rdbms::Login TemporaryPostgresEnvironment::getLogin(const std::string& schemaName) const {
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
 * Generate a unique schema name for test isolation.
 * Each call returns a different schema name.
 */
std::string TemporaryPostgresEnvironment::generateUniqueSchemaName() {
  static std::atomic<uint64_t> counter {0};
  uint64_t id = counter.fetch_add(1);

  std::stringstream ss;
  ss << "test_" << getpid() << "_" << id;
  return ss.str();
}

}  // namespace cta::schedulerdb
