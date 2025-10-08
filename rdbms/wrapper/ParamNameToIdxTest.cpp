/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/exception/Exception.hpp"
#include "rdbms/wrapper/ParamNameToIdx.hpp"

#include <gtest/gtest.h>
#include <sstream>

namespace unitTests {

class cta_rdbms_wrapper_ParamNameToIdxTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_rdbms_wrapper_ParamNameToIdxTest, getIdx_existing_params) {
  using namespace cta::rdbms::wrapper;

  const char* const sql = R"SQL(
    INSERT INTO ADMIN_USER(
      ADMIN_USER_NAME,

      USER_COMMENT,

      CREATION_LOG_USER_NAME,
      CREATION_LOG_GROUP_NAME,
      CREATION_LOG_HOST_NAME,
      CREATION_LOG_TIME,

      LAST_UPDATE_USER_NAME,
      LAST_UPDATE_GROUP_NAME,
      LAST_UPDATE_HOST_NAME,
      LAST_UPDATE_TIME)
    VALUES(
      :ADMIN_USER_NAME,

      :USER_COMMENT,

      :CREATION_LOG_USER_NAME,
      :CREATION_LOG_GROUP_NAME,
      :CREATION_LOG_HOST_NAME,
      :CREATION_LOG_TIME,

      :LAST_UPDATE_USER_NAME,
      :LAST_UPDATE_GROUP_NAME,
      :LAST_UPDATE_HOST_NAME,
      :LAST_UPDATE_TIME)
  )SQL";

  const ParamNameToIdx paramNameToIdx(sql);
  ASSERT_EQ(1, paramNameToIdx.getIdx(":ADMIN_USER_NAME"));
  ASSERT_EQ(2, paramNameToIdx.getIdx(":USER_COMMENT"));
  ASSERT_EQ(3, paramNameToIdx.getIdx(":CREATION_LOG_USER_NAME"));
  ASSERT_EQ(4, paramNameToIdx.getIdx(":CREATION_LOG_GROUP_NAME"));
  ASSERT_EQ(5, paramNameToIdx.getIdx(":CREATION_LOG_HOST_NAME"));
  ASSERT_EQ(6, paramNameToIdx.getIdx(":CREATION_LOG_TIME"));
  ASSERT_EQ(7, paramNameToIdx.getIdx(":LAST_UPDATE_USER_NAME"));
  ASSERT_EQ(8, paramNameToIdx.getIdx(":LAST_UPDATE_GROUP_NAME"));
  ASSERT_EQ(9, paramNameToIdx.getIdx(":LAST_UPDATE_HOST_NAME"));
  ASSERT_EQ(10, paramNameToIdx.getIdx(":LAST_UPDATE_TIME"));
}

TEST_F(cta_rdbms_wrapper_ParamNameToIdxTest, getIdx_non_existing_param) {
  using namespace cta;
  using namespace cta::rdbms::wrapper;

  const char* const sql = R"SQL(
    String containing no bind parameters
  )SQL";

  const ParamNameToIdx paramNameToIdx(sql);
  ASSERT_THROW(paramNameToIdx.getIdx(":NON_EXISTING_PARAM"), exception::Exception);
}

} // namespace unitTests
