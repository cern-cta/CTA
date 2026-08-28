/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "scheduler/rdbms/TemporaryPostgresInstance.hpp"

#include "rdbms/ConnPool.hpp"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <unistd.h>

namespace cta::schedulerdb {

TEST(TemporaryPostgresDeathTest, ChildReusesClusterAndLeavesParentConnectionUsable) {
  ::testing::FLAGS_gtest_death_test_style = "threadsafe";
  ASSERT_NE(nullptr, g_tempPostgresEnv);
  cta::rdbms::ConnPool pool(g_tempPostgresEnv->getLogin(), 1);
  auto connection = pool.getConn();
  connection.executeNonQuery("SET application_name = 'cta_fixture_test'");

  ASSERT_EXIT(
    {
      alarm(5);
      // Destruction of a borrowing environment must not stop or remove the parent's cluster.
      {
        TemporaryPostgresEnvironment borrowed(false);
        borrowed.SetUp();
        cta::rdbms::ConnPool borrowedPool(borrowed.getLogin(), 1);
        auto borrowedConnection = borrowedPool.getConn();
        borrowedConnection.executeNonQuery("SET application_name = 'cta_fixture_child_test'");
      }
      connection.executeNonQuery("SET application_name = 'cta_fixture_test'");
      _exit(0);
    },
    ::testing::ExitedWithCode(0),
    "");

  // This existing connection is lost if the child restarts the parent's server.
  EXPECT_NO_THROW(connection.executeNonQuery("SET application_name = 'cta_fixture_test'"));
}

TEST(TemporaryPostgresEnvironmentTest, CommandsDoNotInheritUnrelatedDescriptors) {
  ASSERT_NE(nullptr, g_tempPostgresEnv);
  // Use a high descriptor so the executed command cannot accidentally reuse it during startup.
  const int descriptor = fcntl(STDOUT_FILENO, F_DUPFD, 200);
  ASSERT_GE(descriptor, 200);
  const auto path = "/proc/self/fd/" + std::to_string(descriptor);
  const int result = g_tempPostgresEnv->runCommand({"test", "!", "-e", path});
  close(descriptor);
  EXPECT_EQ(0, result);
}

}  // namespace cta::schedulerdb
