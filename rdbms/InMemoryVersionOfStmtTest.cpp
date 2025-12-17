/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
    return cta::rdbms::Login(cta::rdbms::Login::DBTYPE_IN_MEMORY, "", "", "", "", 0, "");
  }
};  // class RdbmsInMemoryLoginFactory

RdbmsInMemoryLoginFactory g_inMemoryLoginFactory;

}  // anonymous namespace

INSTANTIATE_TEST_CASE_P(InMemory,
                        cta_rdbms_StmtTest,
                        ::testing::Values(dynamic_cast<cta::rdbms::LoginFactory*>(&g_inMemoryLoginFactory)));

}  // namespace unitTests
