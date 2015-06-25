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

#include "common/ByteArray.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class cta_ByteArrayTest : public ::testing::Test {
protected:
  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_ByteArrayTest, default_constructor) {
  using namespace cta;

  const ByteArray byteArray;
  
  ASSERT_EQ((uint32_t)0, byteArray.getSize());
  ASSERT_EQ(NULL, byteArray.getBytes());
}

TEST_F(cta_ByteArrayTest, two_param_constructor) {
  using namespace cta;

  const uint32_t arraySize = 4;
  const uint8_t bytes[4] = {10, 20, 30, 40};
  const ByteArray byteArray(arraySize, bytes);

  ASSERT_EQ(arraySize, byteArray.getSize());
  ASSERT_EQ((uint8_t)10, byteArray.getBytes()[0]);
  ASSERT_EQ((uint8_t)20, byteArray.getBytes()[1]);
  ASSERT_EQ((uint8_t)30, byteArray.getBytes()[2]);
  ASSERT_EQ((uint8_t)40, byteArray.getBytes()[3]);
}

TEST_F(cta_ByteArrayTest, template_constructor) {
  using namespace cta;

  const uint8_t bytes[4] = {10, 20, 30, 40};
  const ByteArray byteArray(bytes);

  ASSERT_EQ((uint32_t)4, byteArray.getSize());
  ASSERT_EQ((uint8_t)10, byteArray.getBytes()[0]);
  ASSERT_EQ((uint8_t)20, byteArray.getBytes()[1]);
  ASSERT_EQ((uint8_t)30, byteArray.getBytes()[2]);
  ASSERT_EQ((uint8_t)40, byteArray.getBytes()[3]);
}

TEST_F(cta_ByteArrayTest, string_constructor) {
  using namespace cta;

  const std::string bytes("Hello");
  const ByteArray byteArray(bytes);

  ASSERT_EQ((uint32_t)5, byteArray.getSize());
  ASSERT_EQ((uint8_t)'H', byteArray.getBytes()[0]);
  ASSERT_EQ((uint8_t)'e', byteArray.getBytes()[1]);
  ASSERT_EQ((uint8_t)'l', byteArray.getBytes()[2]);
  ASSERT_EQ((uint8_t)'l', byteArray.getBytes()[3]);
  ASSERT_EQ((uint8_t)'o', byteArray.getBytes()[4]);
}

TEST_F(cta_ByteArrayTest, copy_constructor) {
  using namespace cta;

  const uint32_t arraySize = 4;
  const uint8_t bytes[4] = {10,20,30,40};
  const ByteArray byteArray1(arraySize, bytes);

  ASSERT_EQ(arraySize, byteArray1.getSize());
  ASSERT_EQ((uint8_t)10, byteArray1.getBytes()[0]);
  ASSERT_EQ((uint8_t)20, byteArray1.getBytes()[1]);
  ASSERT_EQ((uint8_t)30, byteArray1.getBytes()[2]);
  ASSERT_EQ((uint8_t)40, byteArray1.getBytes()[3]);

  const ByteArray byteArray2(byteArray1);

  ASSERT_EQ(arraySize, byteArray2.getSize());
  ASSERT_EQ((uint8_t)10, byteArray2.getBytes()[0]);
  ASSERT_EQ((uint8_t)20, byteArray2.getBytes()[1]);
  ASSERT_EQ((uint8_t)30, byteArray2.getBytes()[2]);
  ASSERT_EQ((uint8_t)40, byteArray2.getBytes()[3]);

  ASSERT_NE(byteArray1.getBytes(), byteArray2.getBytes());
}

TEST_F(cta_ByteArrayTest, assignment_operator) {
  using namespace cta;

  const uint32_t arraySize1 = 4;
  const uint8_t bytes1[4] = {10,20,30,40};
  const ByteArray byteArray1(arraySize1, bytes1);

  ASSERT_EQ(arraySize1, byteArray1.getSize());
  ASSERT_EQ((uint8_t)10, byteArray1.getBytes()[0]);
  ASSERT_EQ((uint8_t)20, byteArray1.getBytes()[1]);
  ASSERT_EQ((uint8_t)30, byteArray1.getBytes()[2]);
  ASSERT_EQ((uint8_t)40, byteArray1.getBytes()[3]);


  const uint32_t arraySize2 = 10;
  const uint8_t bytes2[10] = {10, 9, 8, 7 ,6 , 5, 4, 3, 2, 1};
  ByteArray byteArray2(arraySize2, bytes2);

  ASSERT_EQ(arraySize2, byteArray2.getSize());
  ASSERT_EQ((uint8_t)10, byteArray2.getBytes()[0]);
  ASSERT_EQ( (uint8_t)9, byteArray2.getBytes()[1]);
  ASSERT_EQ( (uint8_t)8, byteArray2.getBytes()[2]);
  ASSERT_EQ( (uint8_t)7, byteArray2.getBytes()[3]);
  ASSERT_EQ( (uint8_t)6, byteArray2.getBytes()[4]);
  ASSERT_EQ( (uint8_t)5, byteArray2.getBytes()[5]);
  ASSERT_EQ( (uint8_t)4, byteArray2.getBytes()[6]);
  ASSERT_EQ( (uint8_t)3, byteArray2.getBytes()[7]);
  ASSERT_EQ( (uint8_t)2, byteArray2.getBytes()[8]);
  ASSERT_EQ( (uint8_t)1, byteArray2.getBytes()[9]);

  byteArray2 = byteArray1;

  ASSERT_EQ(arraySize1, byteArray2.getSize());
  ASSERT_EQ((uint8_t)10, byteArray2.getBytes()[0]);
  ASSERT_EQ((uint8_t)20, byteArray2.getBytes()[1]);
  ASSERT_EQ((uint8_t)30, byteArray2.getBytes()[2]);
  ASSERT_EQ((uint8_t)40, byteArray2.getBytes()[3]);

  ASSERT_NE(byteArray1.getBytes(), byteArray2.getBytes());
}

} // namespace unitTests
