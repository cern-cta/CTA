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

#include "common/Checksum.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class cta_ChecksumTest : public ::testing::Test {
protected:
  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_ChecksumTest, default_constructor) {
  using namespace cta;

  const Checksum checksum;

  ASSERT_EQ(Checksum::CHECKSUMTYPE_NONE, checksum.getType());
  ASSERT_TRUE(checksum.str().empty());

  const ByteArray &byteArray = checksum.getByteArray();
  
  ASSERT_EQ((uint32_t)0, byteArray.getSize());
  ASSERT_EQ(NULL, byteArray.getBytes());
}

TEST_F(cta_ChecksumTest, two_param_constructor) {
  using namespace cta;

  const Checksum::ChecksumType checksumType = Checksum::CHECKSUMTYPE_ADLER32;
  const uint8_t bytes[4] = {10, 20, 30, 40};
  const ByteArray byteArray(bytes);
  const Checksum checksum(checksumType, bytes);

  ASSERT_EQ(Checksum::CHECKSUMTYPE_ADLER32, checksum.getType());
  ASSERT_EQ((uint32_t)4, checksum.getByteArray().getSize());
  ASSERT_EQ((uint8_t)10, checksum.getByteArray().getBytes()[0]);
  ASSERT_EQ((uint8_t)20, checksum.getByteArray().getBytes()[1]);
  ASSERT_EQ((uint8_t)30, checksum.getByteArray().getBytes()[2]);
  ASSERT_EQ((uint8_t)40, checksum.getByteArray().getBytes()[3]);
}

} // namespace unitTests
