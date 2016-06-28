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
#include "tests/OraUnitTestsCmdLineArgs.hpp"

namespace {

/**
 * Creates DbLogin objects for in-memory catalogue databases.
 */
class OracleDbLoginFactory: public cta::catalogue::DbLoginFactory {
public:

  /**
   * Destructor.
   */
  virtual ~OracleDbLoginFactory() {
  }

  /**
   * Returns a newly created DbLogin object.
   *
   * @return A newly created DbLogin object.
   */
  virtual cta::catalogue::DbLogin create() {
    using namespace cta::catalogue;
    return DbLogin::parseFile(g_cmdLineArgs.oraDbConnFile);
  }
}; // class OracleDbLoginFactory

OracleDbLoginFactory g_oracleDbLoginFactory;

} // anonymous namespace

namespace unitTests {

INSTANTIATE_TEST_CASE_P(Oracle, cta_catalogue_CatalogueTest,
  ::testing::Values(dynamic_cast<cta::catalogue::DbLoginFactory*>(&g_oracleDbLoginFactory)));

} // namespace unitTests
