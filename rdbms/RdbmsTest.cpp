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

#include "rdbms/rdbms.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class cta_rdbms_rdbmsTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_rdbms_rdbmsTest, getSqlForException) {
  using namespace cta::rdbms;

  const std::string::size_type maxSqlLenInExceptions = 7;
  const std::string orginalSql = "1234567890";
  const std::string expectedSql = "1234...";

  const std::string resultingSql = getSqlForException(orginalSql, maxSqlLenInExceptions);
  ASSERT_EQ(expectedSql, resultingSql);
}

} // namespace unitTests
