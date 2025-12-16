/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
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
