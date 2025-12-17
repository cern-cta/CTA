/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "rdbms/ConnTest.hpp"
#include "tests/RdbmsUnitTestsCmdLineArgs.hpp"

namespace {

/**
 * Creates Login objects from a database configuration file passed on the
 * command-line to the unit-tests program.
 */
class DbConfigFileVersionOfConnTestLoginFactory : public cta::rdbms::LoginFactory {
public:
  /**
   * Destructor.
   */
  virtual ~DbConfigFileVersionOfConnTestLoginFactory() {}

  /**
   * Returns a newly created Login object.
   *
   * @return A newly created Login object.
   */
  virtual cta::rdbms::Login create() { return cta::rdbms::Login::parseFile(g_cmdLineArgs.dbConfigPath); }
};  // class OracleLoginFactory

DbConfigFileVersionOfConnTestLoginFactory g_dbConfigFileVersionOfConnTestLoginFactory;

}  // anonymous namespace

namespace unitTests {

INSTANTIATE_TEST_CASE_P(
  DbConfigFile,
  cta_rdbms_ConnTest,
  ::testing::Values(dynamic_cast<cta::rdbms::LoginFactory*>(&g_dbConfigFileVersionOfConnTestLoginFactory)));

}  // namespace unitTests
