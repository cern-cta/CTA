/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/checksum/ChecksumBlob.hpp"

#include <gtest/gtest.h>

namespace unitTests {

TEST(cta_ChecksumBlobSerDeserTest, serialize_deserialize) {
  using namespace cta::checksum;

  ChecksumBlob checksumBlob1;

  checksumBlob1.insert(NONE, "");                      // 0 bits
  checksumBlob1.insert(ADLER32, 0x3e80001);            // 32 bits
  checksumBlob1.insert(CRC32, "0");                    // 32 bits
  checksumBlob1.insert(CRC32C, "FFFF");                // 32 bits
  checksumBlob1.insert(MD5, "1234567890123456");       // 128 bits
  checksumBlob1.insert(SHA1, "12345678901234567890");  // 160 bits

  auto len = checksumBlob1.length();
  auto bytearray = checksumBlob1.serialize();
  ASSERT_EQ(len, bytearray.length());

  ChecksumBlob checksumBlob2;
  checksumBlob2.deserialize(bytearray);
  ASSERT_EQ(checksumBlob1, checksumBlob2);
}

}  // namespace unitTests
