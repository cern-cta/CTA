// ----------------------------------------------------------------------
// File: SCSI/DeviceTest.cc
// Author: Eric Cano - CERN
// ----------------------------------------------------------------------

/************************************************************************
 * Tape Server                                                          *
 * Copyright (C) 2013 CERN/Switzerland                                  *
 *                                                                      *
 * This program is free software: you can redistribute it and/or modify *
 * it under the terms of the GNU General Public License as published by *
 * the Free Software Foundation, either version 3 of the License, or    *
 * (at your option) any later version.                                  *
 *                                                                      *
 * This program is distributed in the hope that it will be useful,      *
 * but WITHOUT ANY WARRANTY; without even the implied warranty of       *
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the        *
 * GNU General Public License for more details.                         *
 *                                                                      *
 * You should have received a copy of the GNU General Public License    *
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.*
 ************************************************************************/

#include <gtest/gtest.h>
#include <gmock/gmock-cardinalities.h>
#include "Device.hh"
#include "Structures.hh"
#include "../System/Wrapper.hh"

using ::testing::AtLeast;
using ::testing::Return;
using ::testing::_;

namespace UnitTests {
  TEST(SCSI_Structures, inquiryData_t_multi_byte_numbers_strings) {
    /* Validate the bit field behavior of the struct inquiryData_t,
     which represents the standard INQUIRY data format as defined in 
     SPC-4. This test also validates the handling of multi-bytes numbers,
     as SCSI structures are big endian (and main development target is 
     little endian.  */
    unsigned char inqBuff [100];
    memset(inqBuff, 0, sizeof(inqBuff));
    SCSI::Structures::inquiryData_t & inq = *((SCSI::Structures::inquiryData_t *) inqBuff);
    /* Peripheral device type */
    ASSERT_EQ(0, inq.perifDevType);
    inqBuff[0] |= (0x1A & 0x1F) << 0;
    ASSERT_EQ(0x1A, inq.perifDevType);
    
    /* Peripheral qualifier */
    ASSERT_EQ(0, inq.perifQualifyer);
    inqBuff[0] |= (0x5 & 0x7) << 5;
    ASSERT_EQ(0x5, inq.perifQualifyer);
    
    /* ... */
    /* sampling of bits */
    ASSERT_EQ(0, inq.VS1);
    inqBuff[6] |= (0x1 & 0x1) << 5;
    ASSERT_EQ(1, inq.VS1);
    
    ASSERT_EQ(0, inq.sync);
    inqBuff[7] |= (0x1 & 0x1) << 4;
    ASSERT_EQ(1, inq.sync);
    
    /* Test of strings: empty/full/boundary effect with next record */
    ASSERT_EQ("", SCSI::Structures::toString(inq.T10Vendor));
    inqBuff[8] = 'V';
    inqBuff[9] = 'i';
    inqBuff[10] = 'r';
    inqBuff[11] = 't';
    inqBuff[12] = 'u';
    inqBuff[13] = 'a';
    inqBuff[14] = 'l';
    inqBuff[15] = 's';
    ASSERT_EQ("Virtuals", SCSI::Structures::toString(inq.T10Vendor));
    
    /* Check there is no side effect from next record */
    inqBuff[16] = 'X';
    inqBuff[17] = 'X';
    inqBuff[18] = 'X';
    ASSERT_EQ("Virtuals", SCSI::Structures::toString(inq.T10Vendor));
    ASSERT_EQ("XXX", SCSI::Structures::toString(inq.prodId));
            
    /* Check that non-full record does not yield too long a string */
    inqBuff[8] = 'T';
    inqBuff[9] = 'a';
    inqBuff[10] = 'p';
    inqBuff[11] = 'e';
    inqBuff[12] = 's';
    inqBuff[13] = '\0';
    inqBuff[14] = '\0';
    inqBuff[15] = '\0';
    ASSERT_EQ("Tapes", SCSI::Structures::toString(inq.T10Vendor));

    /* Test of endian conversion */
    ASSERT_EQ(0, SCSI::Structures::toU16(inq.versionDescriptor[7]));
    inqBuff[72] = 0xCA;
    inqBuff[73] = 0xFE;
    ASSERT_EQ(0xCAFE, SCSI::Structures::toU16(inq.versionDescriptor[7]));
    
    /* Test last element */
    ASSERT_EQ(0, *inq.vendorSpecific2);
    inqBuff[96] = 0x12;
    ASSERT_EQ(0x12, *inq.vendorSpecific2);
  }

  TEST(SCSI_Structures, inquiryCDB_t) {
    SCSI::Structures::inquiryCDB_t inqCDB;
    memset(&inqCDB, 0, sizeof(inqCDB));
    unsigned char *buff = (unsigned char *)&inqCDB;
    
    ASSERT_EQ(6, sizeof(inqCDB));
    
    ASSERT_EQ(0, inqCDB.opCode);
    buff[0] = SCSI::Commands::INQUIRY;
    ASSERT_EQ(SCSI::Commands::INQUIRY, inqCDB.opCode);
    
    ASSERT_EQ(0, inqCDB.EVPD);
    buff[1] = 0x1;
    ASSERT_EQ(1, inqCDB.EVPD);
    
    ASSERT_EQ(0, inqCDB.control);
    buff[5] = 0xCA;
    ASSERT_EQ(0xCA, inqCDB.control);
  }
};
