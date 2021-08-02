/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "rdbms/ConnTest.hpp"

namespace unitTests {

namespace {

/**
 * Creates Login objects for in-memory catalogue databases.
 */
class RdbmsInMemoryLoginFactory: public cta::rdbms::LoginFactory {
public:

  /**
   * Destructor.
   */
  virtual ~RdbmsInMemoryLoginFactory() {
  }

  /**
   * Returns a newly created Login object.
   *
   * @return A newly created Login object.
   */
  virtual cta::rdbms::Login create() {
    return cta::rdbms::Login(cta::rdbms::Login::DBTYPE_IN_MEMORY, "", "", "", "", 0);
  }
}; // class RdbmsInMemoryLoginFactory

RdbmsInMemoryLoginFactory g_inMemoryLoginFactory;

} // anonymous namespace

INSTANTIATE_TEST_CASE_P(InMemory, cta_rdbms_ConnTest,
  ::testing::Values(dynamic_cast<cta::rdbms::LoginFactory*>(&g_inMemoryLoginFactory)));

} // namespace unitTests
