/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "catalogue/CatalogueTest.hpp"
#include "tests/CatalogueUnitTestsCmdLineArgs.hpp"

namespace {

/**
 * Creates DbLogin objects for the test catalogue database whose database login
 * details are specified in a database configuration file passed on the
 * command-line to the catalogue unit-tests program.
 */
class DbConfigFileLoginFactory: public cta::catalogue::DbLoginFactory {
public:

  /**
   * Destructor.
   */
  virtual ~DbConfigFileLoginFactory() {
  }

  /**
   * Returns a newly created DbLogin object.
   *
   * @return A newly created DbLogin object.
   */
  virtual cta::catalogue::DbLogin create() {
    using namespace cta::catalogue;
    return DbLogin::parseFile(g_cmdLineArgs.dbConfigPath);
  }
}; // class OracleDbLoginFactory

DbConfigFileLoginFactory g_dbConfigLoginFactory;

} // anonymous namespace

namespace unitTests {

INSTANTIATE_TEST_CASE_P(DbConfigFile, cta_catalogue_CatalogueTest,
  ::testing::Values(dynamic_cast<cta::catalogue::DbLoginFactory*>(&g_dbConfigLoginFactory)));

} // namespace unitTests
