/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <gmock/gmock.h>
#include <google/protobuf/stubs/common.h>
#include <gtest/gtest.h>
#include <sqlite3.h>
#include <string_view>

#ifdef CTA_PGSCHED
#include "scheduler/rdbms/TemporaryPostgresInstance.hpp"
#endif

int main(int argc, char** argv) {
  // The unit tests use SQLite it must be initialized before they are run
  if (SQLITE_OK != sqlite3_initialize()) {
    std::cerr << "Failed to initialize SQLite" << std::endl;
    return 1;  // Error
  }

#ifdef CTA_PGSCHED
  // Threadsafe death-test children inherit PostgreSQL; only the parent owns its lifecycle.
  // Inspect argv before Google Test consumes it, avoiding version-specific internal flag namespaces.
  bool ownsPostgres = true;
  constexpr std::string_view deathTestFlag = "--gtest_internal_run_death_test=";
  for (int i = 1; i < argc; ++i) {
    const std::string_view argument(argv[i]);
    if (argument.starts_with(deathTestFlag) && argument.size() > deathTestFlag.size()) {
      ownsPostgres = false;
    }
  }
#endif

  // Initialize Google Mock and Google Test before registering the test environment.
  ::testing::InitGoogleMock(&argc, argv);

#ifdef CTA_PGSCHED
  cta::schedulerdb::g_tempPostgresEnv = new cta::schedulerdb::TemporaryPostgresEnvironment(ownsPostgres);
  ::testing::AddGlobalTestEnvironment(cta::schedulerdb::g_tempPostgresEnv);
#endif

  const int ret = RUN_ALL_TESTS();

  // Keep standard streams open so shutdown failures remain visible.
  // The unit tests used SQLite and so it should shut down in order to release
  // its resources
  if (SQLITE_OK != sqlite3_shutdown()) {
    std::cerr << "Failed to shutdown SQLite" << std::endl;
    return 1;  // Error
  }

  ::google::protobuf::ShutdownProtobufLibrary();

  return ret;
}
