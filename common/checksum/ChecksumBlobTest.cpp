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

#include "common/checksum/ChecksumBlob.hpp"
#include <gtest/gtest.h>

namespace unitTests {

class cta_ChecksumBlobTest : public ::testing::Test {
protected:
  virtual void SetUp() {}

  virtual void TearDown() {}
};

TEST_F(cta_ChecksumBlobTest, checksum_types) {
  using namespace cta::checksum;
  using namespace cta::exception;

  ChecksumBlob checksumBlob;
  ASSERT_EQ(checksumBlob.empty(), true);
  ASSERT_EQ(checksumBlob.size(), 0);

  // Checksum type not in blob
  ASSERT_THROW(checksumBlob.at(NONE), ChecksumTypeMismatch);
  ASSERT_THROW(checksumBlob.at(ADLER32), ChecksumTypeMismatch);
  ASSERT_THROW(checksumBlob.at(CRC32), ChecksumTypeMismatch);
  ASSERT_THROW(checksumBlob.at(CRC32C), ChecksumTypeMismatch);
  ASSERT_THROW(checksumBlob.at(MD5), ChecksumTypeMismatch);
  ASSERT_THROW(checksumBlob.at(SHA1), ChecksumTypeMismatch);

  // valid insertions
  checksumBlob.insert(NONE, "");  // 0 bits
  ASSERT_EQ(checksumBlob.size(), 1);
  ASSERT_EQ(checksumBlob.empty(), false);
  checksumBlob.insert(ADLER32, "1234");  // 32 bits
  ASSERT_EQ(checksumBlob.size(), 2);
  checksumBlob.insert(CRC32, "1234");  // 32 bits
  ASSERT_EQ(checksumBlob.size(), 3);
  checksumBlob.insert(CRC32C, "1234");  // 32 bits
  ASSERT_EQ(checksumBlob.size(), 4);
  checksumBlob.insert(MD5, "1234567890123456");  // 128 bits
  ASSERT_EQ(checksumBlob.size(), 5);
  checksumBlob.insert(SHA1, "12345678901234567890");  // 160 bits
  ASSERT_EQ(checksumBlob.size(), 6);

  // check each of the checksums in turn
  ASSERT_EQ(checksumBlob.contains(NONE, ""), true);
  ASSERT_EQ(checksumBlob.contains(ADLER32, "1234"), true);
  ASSERT_EQ(checksumBlob.contains(CRC32, "1234"), true);
  ASSERT_EQ(checksumBlob.contains(CRC32C, "1234"), true);
  ASSERT_EQ(checksumBlob.contains(MD5, "1234567890123456"), true);
  ASSERT_EQ(checksumBlob.contains(SHA1, "12345678901234567890"), true);

  // invalid insertions
  ASSERT_THROW(checksumBlob.insert(NONE, "0"), ChecksumValueMismatch);
  ASSERT_THROW(checksumBlob.insert(ADLER32, "12345"), ChecksumValueMismatch);
  ASSERT_THROW(checksumBlob.insert(CRC32, "12345"), ChecksumValueMismatch);
  ASSERT_THROW(checksumBlob.insert(CRC32C, "12345"), ChecksumValueMismatch);
  ASSERT_THROW(checksumBlob.insert(MD5, "12345678901234567"), ChecksumValueMismatch);
  ASSERT_THROW(checksumBlob.insert(SHA1, "123456789012345678901"), ChecksumValueMismatch);
  ASSERT_THROW(checksumBlob.insert(MD5, 0x12345678), ChecksumTypeMismatch);
  ASSERT_THROW(checksumBlob.insert(SHA1, 0x12345678), ChecksumTypeMismatch);

  // Blob types are different
  ChecksumBlob checksumBlob2, checksumBlob3;
  checksumBlob2.insert(NONE, "");
  checksumBlob3.insert(ADLER32, "1234");
  ASSERT_THROW(checksumBlob2.validate(checksumBlob3), ChecksumTypeMismatch);
  ASSERT_NE(checksumBlob2, checksumBlob3);

  // Blob sizes are different
  checksumBlob3.insert(NONE, "");
  ASSERT_THROW(checksumBlob2.validate(checksumBlob3), ChecksumBlobSizeMismatch);
  ASSERT_NE(checksumBlob2, checksumBlob3);

  // Blob values are different
  checksumBlob2 = checksumBlob;
  checksumBlob2.insert(ADLER32, 0x0a0b0c0d);
  ASSERT_THROW(checksumBlob.validate(checksumBlob2), ChecksumValueMismatch);
  ASSERT_NE(checksumBlob, checksumBlob2);

  // Clear the blob
  checksumBlob.clear();
  ASSERT_EQ(checksumBlob.empty(), true);
  ASSERT_EQ(checksumBlob.size(), 0);
}

TEST_F(cta_ChecksumBlobTest, hex_to_byte_array) {
  using namespace cta::checksum;

  // Check some limiting cases
  ASSERT_EQ(ChecksumBlob::ByteArrayToHex(ChecksumBlob::HexToByteArray("0")), "00");
  ASSERT_EQ(ChecksumBlob::ByteArrayToHex(ChecksumBlob::HexToByteArray("0xFFFFFFFF")), "ffffffff");
  ASSERT_EQ(ChecksumBlob::ByteArrayToHex(ChecksumBlob::HexToByteArray("0X10a0BFC")), "010a0bfc");
  ASSERT_EQ(ChecksumBlob::ByteArrayToHex(ChecksumBlob::HexToByteArray("000a000b000c000d000e000f000abcdef1234567890")),
            "0000a000b000c000d000e000f000abcdef1234567890");
}

TEST_F(cta_ChecksumBlobTest, adler32) {
  using namespace cta::checksum;
  using namespace cta::exception;

  ChecksumBlob checksumBlob;
  ASSERT_THROW(checksumBlob.validate(ADLER32, "invalid"), ChecksumTypeMismatch);
  ASSERT_EQ(checksumBlob.contains(ADLER32, "invalid"), false);

  checksumBlob.insert(ADLER32, 0x0A141E28);
  ASSERT_EQ(checksumBlob.size(), 1);
  ASSERT_THROW(checksumBlob.validate(ADLER32, "invalid"), ChecksumValueMismatch);
  ASSERT_EQ(checksumBlob.contains(ADLER32, "invalid"), false);

  // Check internal representation
  std::string bytearray = checksumBlob.at(ADLER32);
  ASSERT_EQ(4, bytearray.length());
  ASSERT_EQ(static_cast<uint8_t>(0x28), bytearray[0]);
  ASSERT_EQ(static_cast<uint8_t>(0x1E), bytearray[1]);
  ASSERT_EQ(static_cast<uint8_t>(0x14), bytearray[2]);
  ASSERT_EQ(static_cast<uint8_t>(0x0A), bytearray[3]);

  // Check we can convert back to the original hex value
  ASSERT_EQ(ChecksumBlob::ByteArrayToHex(bytearray), "0a141e28");

  // Check construction by bytearray yields the same result
  ChecksumBlob checksumBlob2;
  checksumBlob2.insert(ADLER32, bytearray);
  ASSERT_EQ(checksumBlob2.size(), 1);
  ASSERT_EQ(checksumBlob, checksumBlob2);

  // Check construction by hex string yields the same result
  ChecksumBlob checksumBlob3;
  checksumBlob3.insert(ADLER32, ChecksumBlob::HexToByteArray("0x0A141E28"));
  ASSERT_EQ(checksumBlob3.size(), 1);

  // Check alternate hex string yields the same result
  ChecksumBlob checksumBlob4;
  checksumBlob4.insert(ADLER32, ChecksumBlob::HexToByteArray("a141e28"));
  ASSERT_EQ(checksumBlob4.size(), 1);
  ASSERT_EQ(checksumBlob, checksumBlob4);
}

TEST_F(cta_ChecksumBlobTest, serialize_deserialize) {
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
