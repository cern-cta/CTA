/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "rdbms/StmtTest.hpp"
#include "tests/RdbmsUnitTestsCmdLineArgs.hpp"

namespace {

/**
 * Creates Login objects from a database configuration file passed on the
 * command-line to the unit-tests program.
 */
class DbConfigFileVersionOfStmtTestLoginFactory: public cta::rdbms::LoginFactory {
public:

  /**
   * Destructor.
   */
  virtual ~DbConfigFileVersionOfStmtTestLoginFactory() {
  }

  /**
   * Returns a newly created Login object.
   *
   * @return A newly created Login object.
   */
  virtual cta::rdbms::Login create() {
    return cta::rdbms::Login::parseFile(g_cmdLineArgs.dbConfigPath);
  }
}; // class OracleLoginFactory

DbConfigFileVersionOfStmtTestLoginFactory g_dbConfigFileVersionOfStmtTestLoginFactory;

} // anonymous namespace

namespace unitTests {

INSTANTIATE_TEST_CASE_P(DbConfigFile, cta_rdbms_StmtTest,
  ::testing::Values(dynamic_cast<cta::rdbms::LoginFactory*>(&g_dbConfigFileVersionOfStmtTestLoginFactory)));

} // namespace unitTests
