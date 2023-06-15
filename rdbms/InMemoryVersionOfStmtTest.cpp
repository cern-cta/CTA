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

#include "rdbms/StmtTest.hpp"

namespace unitTests {

namespace {

/**
 * Creates Login objects for in-memory catalogue databases.
 */
class RdbmsInMemoryLoginFactory : public cta::rdbms::LoginFactory {
public:
  /**
   * Destructor.
   */
  virtual ~RdbmsInMemoryLoginFactory() {}

  /**
   * Returns a newly created Login object.
   *
   * @return A newly created Login object.
   */
  virtual cta::rdbms::Login create() {
    return cta::rdbms::Login(cta::rdbms::Login::DBTYPE_IN_MEMORY, "", "", "", "", 0);
  }
};  // class RdbmsInMemoryLoginFactory

RdbmsInMemoryLoginFactory g_inMemoryLoginFactory;

}  // anonymous namespace

INSTANTIATE_TEST_CASE_P(InMemory,
                        cta_rdbms_StmtTest,
                        ::testing::Values(dynamic_cast<cta::rdbms::LoginFactory*>(&g_inMemoryLoginFactory)));

}  // namespace unitTests
