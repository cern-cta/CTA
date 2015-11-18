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

#include "common/checksum/Checksum.hpp"
#include <gtest/gtest.h>
#include <arpa/inet.h>

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

  ASSERT_EQ((uint32_t)0, checksum.getByteArray().size());
}

TEST_F(cta_ChecksumTest, two_param_constructor) {
  using namespace cta;

  const Checksum::ChecksumType checksumType = Checksum::CHECKSUMTYPE_ADLER32;
  const uint32_t val = ntohl(0x0A141E28);
  const Checksum checksum(checksumType, val);

  ASSERT_EQ(Checksum::CHECKSUMTYPE_ADLER32, checksum.getType());
  ASSERT_EQ((uint32_t)4, checksum.getByteArray().size());
  ASSERT_EQ((uint8_t)10, checksum.getByteArray()[0]);
  ASSERT_EQ((uint8_t)20, checksum.getByteArray()[1]);
  ASSERT_EQ((uint8_t)30, checksum.getByteArray()[2]);
  ASSERT_EQ((uint8_t)40, checksum.getByteArray()[3]);
}

TEST_F(cta_ChecksumTest, url_constructor) {
  using namespace cta;
  
  const Checksum checksum("adler32:0x12345678");
  
  ASSERT_EQ(Checksum::CHECKSUMTYPE_ADLER32, checksum.getType());
  ASSERT_EQ(0x12345678, checksum.getNumeric<uint32_t>());
}

} // namespace unitTests
