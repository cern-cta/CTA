/*!
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2019 CERN
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

#include "common/checksum/ChecksumBlob.hpp"
#include <gtest/gtest.h>

namespace unitTests {

class cta_ChecksumBlobTest : public ::testing::Test {
protected:
  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(cta_ChecksumBlobTest, default_constructor) {
  using namespace cta;
  using namespace checksum;

  const ChecksumBlob checksumBlob;

  ASSERT_EQ(checksumBlob.length(), 0);
}

TEST_F(cta_ChecksumBlobTest, adler32) {
  using namespace cta;
  using namespace checksum;

  ChecksumBlob checksumBlob;
  checksumBlob.insert(checksum::ADLER32, 0x0A141E28);
  ASSERT_EQ(checksumBlob.length(), 1);

  std::string bytearray = checksumBlob.at(checksum::ADLER32);

  ASSERT_EQ(4, bytearray.length());
  ASSERT_EQ(static_cast<uint8_t>(10), bytearray[0]);
  ASSERT_EQ(static_cast<uint8_t>(20), bytearray[1]);
  ASSERT_EQ(static_cast<uint8_t>(30), bytearray[2]);
  ASSERT_EQ(static_cast<uint8_t>(40), bytearray[3]);
}

} // namespace unitTests
