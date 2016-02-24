/******************************************************************************
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 *
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "castor/utils/CRC.hpp"

#include <gtest/gtest.h>

#include <stdint.h>

namespace unitTests {

class castor_CRC : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(castor_CRC, testCRCRS_sw) {
  using namespace castor::utils::CRC;
  
  const uint8_t block1[] = {2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43,
    47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97, 101, 103, 107, 109, 113, 127,
    131, 137, 139, 149, 151, 157};
  
  const uint8_t block2[] = {163, 167, 173, 179, 181, 191, 193, 197, 199, 211, 
    223, 227, 229, 233, 239, 241, 251};

  const uint32_t computedCRC1 = crcRS_sw(0, sizeof(block1), block1);
  const uint32_t computedCRC2 = crcRS_sw(computedCRC1, sizeof(block2), block2);
  const uint32_t computedCRC3 = crcRS_sw(crcRS_sw(0, sizeof(block1), block1), 
    sizeof(block2), block2);
  
  ASSERT_EQ(computedCRC1, 0x733D4DCA);
  ASSERT_EQ(computedCRC2, 0x754ED37E);
  ASSERT_EQ(computedCRC3, 0x754ED37E);
}

TEST_F(castor_CRC, testCRC32C_sw) {
  using namespace castor::utils::CRC;
  
  const uint8_t block1[] = {2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43,
    47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97, 101, 103, 107, 109, 113, 127,
    131, 137, 139, 149, 151, 157};
  
  const uint8_t block2[] = {163, 167, 173, 179, 181, 191, 193, 197, 199, 211, 
    223, 227, 229, 233, 239, 241, 251};

  const uint32_t computedCRC1 = crc32c_sw(0xFFFFFFFF, sizeof(block1), block1);
  const uint32_t computedCRC2 = crc32c_sw(computedCRC1, sizeof(block2), block2);
  const uint32_t computedCRC3 = crc32c_sw(crc32c_sw(0xFFFFFFFF, sizeof(block1),
    block1), sizeof(block2), block2);
  
  ASSERT_EQ(computedCRC1, 0xE8174F48);
  ASSERT_EQ(computedCRC2, 0x56DAB0A6);
  ASSERT_EQ(computedCRC3, 0x56DAB0A6);
}
TEST_F(castor_CRC, testCRC32C_hw) {
  using namespace castor::utils::CRC;
  
  const uint8_t block1[] = {2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43,
    47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97, 101, 103, 107, 109, 113, 127,
    131, 137, 139, 149, 151, 157};
  
  const uint8_t block2[] = {163, 167, 173, 179, 181, 191, 193, 197, 199, 211, 
    223, 227, 229, 233, 239, 241, 251};

  const uint32_t computedCRC1 = crc32c_hw(0xFFFFFFFF, sizeof(block1), block1);
  const uint32_t computedCRC2 = crc32c_hw(computedCRC1, sizeof(block2), block2);
  const uint32_t computedCRC3 = crc32c_hw(crc32c_hw(0xFFFFFFFF, sizeof(block1),
    block1), sizeof(block2), block2);
  
  ASSERT_EQ(computedCRC1, 0xE8174F48);
  ASSERT_EQ(computedCRC2, 0x56DAB0A6);
  ASSERT_EQ(computedCRC3, 0x56DAB0A6);
}
TEST_F(castor_CRC, testCRC32C) {
  using namespace castor::utils::CRC;
  
  const uint8_t block1[] = {2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37, 41, 43,
    47, 53, 59, 61, 67, 71, 73, 79, 83, 89, 97, 101, 103, 107, 109, 113, 127,
    131, 137, 139, 149, 151, 157};
  
  const uint8_t block2[] = {163, 167, 173, 179, 181, 191, 193, 197, 199, 211, 
    223, 227, 229, 233, 239, 241, 251};

  const uint32_t computedCRC1 = crc32c(0xFFFFFFFF, sizeof(block1), block1);
  const uint32_t computedCRC2 = crc32c(computedCRC1, sizeof(block2), block2);
  const uint32_t computedCRC3 = crc32c(crc32c(0xFFFFFFFF, sizeof(block1),
    block1), sizeof(block2), block2);
  
  ASSERT_EQ(computedCRC1, 0xE8174F48);
  ASSERT_EQ(computedCRC2, 0x56DAB0A6);
  ASSERT_EQ(computedCRC3, 0x56DAB0A6);
}
} // namespace unitTests
