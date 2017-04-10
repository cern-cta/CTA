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

#include "OcciColumn.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class cta_rdbms_OcciColumnTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_rdbms_OcciColumnTest, getColName) {
  using namespace cta;
  using namespace cta::rdbms;

  const std::string colName = "TEST_COLUMN";
  const size_t nbRows = 1;
  OcciColumn col(colName, nbRows);

  ASSERT_EQ(colName, col.getColName());
}

TEST_F(cta_rdbms_OcciColumnTest, getNbRows) {
  using namespace cta;
  using namespace cta::rdbms;

  const std::string colName = "TEST_COLUMN";
  const size_t nbRows = 1;
  OcciColumn col(colName, nbRows);

  ASSERT_EQ(nbRows, col.getNbRows());
}

TEST_F(cta_rdbms_OcciColumnTest, setFieldLen) {
  using namespace cta;
  using namespace cta::rdbms;

  const std::string colName = "TEST_COLUMN";
  const size_t nbRows = 1;
  OcciColumn col(colName, nbRows);

  ASSERT_EQ(0, col.getMaxFieldLength());

  const ub2 field0Value = 1234;
  col.setFieldLenToValueLen(0, field0Value);
  ASSERT_EQ(5, col.getMaxFieldLength());
}

TEST_F(cta_rdbms_OcciColumnTest, setFieldLenToValueLen_stringValue) {
  using namespace cta;
  using namespace cta::rdbms;

  const std::string colName = "TEST_COLUMN";
  const size_t nbRows = 1;
  OcciColumn col(colName, nbRows);

  ASSERT_EQ(0, col.getMaxFieldLength());

  const std::string field0Value = "field0Value";
  col.setFieldLenToValueLen(0, field0Value);
  ASSERT_EQ(field0Value.length() + 1, col.getMaxFieldLength());
}

TEST_F(cta_rdbms_OcciColumnTest, setFieldLenToValueLen_uint64_tValue) {
  using namespace cta;
  using namespace cta::rdbms;

  const std::string colName = "TEST_COLUMN";
  const size_t nbRows = 1;
  OcciColumn col(colName, nbRows);

  ASSERT_EQ(0, col.getMaxFieldLength());

  const uint64_t field0Value = 1234;
  col.setFieldLenToValueLen(0, field0Value);
  ASSERT_EQ(5, col.getMaxFieldLength());
}

TEST_F(cta_rdbms_OcciColumnTest, setFieldLen_tooLate) {
  using namespace cta;
  using namespace cta::rdbms;

  const std::string colName = "TEST_COLUMN";
  const size_t nbRows = 2;
  OcciColumn col(colName, nbRows);

  const ub2 field0Value = 1234;
  const ub2 field1Value = 5678;
  col.setFieldLenToValueLen(0, field0Value);

  col.getBuffer();

  ASSERT_THROW(col.setFieldLenToValueLen(1, field1Value), exception::Exception);
}

TEST_F(cta_rdbms_OcciColumnTest, setFieldLen_invalidIndex) {
  using namespace cta;
  using namespace cta::rdbms;

  const std::string colName = "TEST_COLUMN";
  const size_t nbRows = 1;
  OcciColumn col(colName, nbRows);

  const ub2 field1Value = 5678;
  ASSERT_THROW(col.setFieldLenToValueLen(1, field1Value), exception::Exception);
}

TEST_F(cta_rdbms_OcciColumnTest, getFieldLengths) {
  using namespace cta;
  using namespace cta::rdbms;

  const std::string colName = "TEST_COLUMN";
  const size_t nbRows = 3;
  OcciColumn col(colName, nbRows);

  const ub2 field0Value = 1;
  const ub2 field1Value = 22;
  const ub2 field2Value = 333;
  col.setFieldLenToValueLen(0, field0Value); // Field Length is 1 + 1
  col.setFieldLenToValueLen(1, field1Value); // Field length is 2 + 1
  col.setFieldLenToValueLen(2, field2Value); // Field length is 3 + 1

  ub2 *const fieldLens = col.getFieldLengths();

  ASSERT_EQ(2, fieldLens[0]);
  ASSERT_EQ(3, fieldLens[1]);
  ASSERT_EQ(4, fieldLens[2]);
}

TEST_F(cta_rdbms_OcciColumnTest, getBuffer) {
  using namespace cta;
  using namespace cta::rdbms;

  const std::string colName = "TEST_COLUMN";
  const size_t nbRows = 1;
  OcciColumn col(colName, nbRows);

  const ub2 field0Value = 1234;
  col.setFieldLenToValueLen(0, field0Value);

  char *const buf = col.getBuffer();
  ASSERT_NE(nullptr, buf);
}

TEST_F(cta_rdbms_OcciColumnTest, getBuffer_tooEarly) {
  using namespace cta;
  using namespace cta::rdbms;

  const std::string colName = "TEST_COLUMN";
  const size_t nbRows = 1;
  OcciColumn col(colName, nbRows);

  ASSERT_THROW(col.getBuffer(), exception::Exception);
}

TEST_F(cta_rdbms_OcciColumnTest, getMaxFieldLength) {
  using namespace cta;
  using namespace cta::rdbms;

  const std::string colName = "TEST_COLUMN";
  const size_t nbRows = 4;
  OcciColumn col(colName, nbRows);

  const ub2 field0Value = 1;
  const ub2 field1Value = 22;
  const ub2 field2Value = 333; // Max field length is 3 + 1
  const ub2 field3Value = 1;
  col.setFieldLenToValueLen(0, field0Value);
  col.setFieldLenToValueLen(1, field1Value);
  col.setFieldLenToValueLen(2, field2Value);
  col.setFieldLenToValueLen(3, field3Value);

  ASSERT_EQ(4, col.getMaxFieldLength());
}

TEST_F(cta_rdbms_OcciColumnTest, copyStrIntoField_1_oneField) {
  using namespace cta;
  using namespace cta::rdbms;

  const std::string colName = "TEST_COLUMN";
  const size_t nbRows = 1;
  OcciColumn col(colName, nbRows);

  const std::string field0Value = "FIELD 0 VALUE";
  col.setFieldLenToValueLen(0, field0Value);
  col.setFieldValue(0, field0Value);

  char *const buf = col.getBuffer();
  ASSERT_EQ(field0Value, std::string(buf));
}

TEST_F(cta_rdbms_OcciColumnTest, setFieldValue_twoFields) {
  using namespace cta;
  using namespace cta::rdbms;

  const std::string colName = "TEST_COLUMN";
  const size_t nbRows = 2;
  OcciColumn col(colName, nbRows);

  const std::string field0Value = "FIELD 0 VALUE";
  const std::string field1Value = "FIELD 1 VALUE";
  col.setFieldLenToValueLen(0, field0Value);
  col.setFieldLenToValueLen(1, field1Value);
  col.setFieldValue(0, field0Value);
  col.setFieldValue(1, field1Value);

  char *const buf = col.getBuffer();
  const char *const bufField0 = buf;
  const char *const bufField1 = buf + col.getMaxFieldLength();
  ASSERT_EQ(field0Value, std::string(bufField0));
  ASSERT_EQ(field1Value, std::string(bufField1));
}

TEST_F(cta_rdbms_OcciColumnTest, setFieldValue_tooLong) {
  using namespace cta;
  using namespace cta::rdbms;

  const std::string colName = "TEST_COLUMN";
  const size_t nbRows = 1;
  OcciColumn col(colName, nbRows);

  const std::string tooShortValue = "SHORT";
  const std::string field0Value = "FIELD 0 VALUE";
  col.setFieldLenToValueLen(0, tooShortValue);
  ASSERT_THROW(col.setFieldValue(0, field0Value), exception::Exception);
}

} // namespace unitTests
