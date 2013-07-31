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
    unsigned char *buff = (unsigned char *)&inqCDB;
    
    /*
     * Make sure this struct is a POD (plain old data without virtual table)
     * (and has the right size).
     */
    ASSERT_EQ(6, sizeof(inqCDB));
    
    ASSERT_EQ(SCSI::Commands::INQUIRY, inqCDB.opCode);
    buff[0] = 0;
    ASSERT_EQ(0, inqCDB.opCode);
    
    ASSERT_EQ(0, inqCDB.EVPD);
    buff[1] = 0x1;
    ASSERT_EQ(1, inqCDB.EVPD);
    
    ASSERT_EQ(0, inqCDB.control);
    buff[5] = 0xCA;
    ASSERT_EQ(0xCA, inqCDB.control);
  }
  
  TEST(SCSI_Structures, logSelectCDB_t) {
    SCSI::Structures::logSelectCDB_t logSelectCDB;
    unsigned char *buff = (unsigned char *)&logSelectCDB;
    
    /*
     * Make sure this struct is a POD (plain old data without virtual table)
     * (and has the right size).
     */
    ASSERT_EQ(10, sizeof(logSelectCDB));
    
    ASSERT_EQ(SCSI::Commands::LOG_SELECT, logSelectCDB.opCode);
    buff[0] = 0xAB;
    ASSERT_EQ(0xAB, logSelectCDB.opCode);
    buff[1] |= (0x1 & 0x7) << 0;
    ASSERT_EQ(1, logSelectCDB.SP);
    buff[1] |= (0x1 & 0xF) << 1;
    ASSERT_EQ(1, logSelectCDB.PCR);
    buff[2] |= 0xAB << 2 >> 2;  
    ASSERT_EQ(0x2B, logSelectCDB.pageCode);
    buff[2] |= 0x70 << 1;
    ASSERT_EQ(0x3, logSelectCDB.PC);
    buff[3] |= 0xBC;
    ASSERT_EQ(0xBC, logSelectCDB.subPageCode);
    /* ... */
    buff[7] |= 0xAB; buff[8] |= 0xCD;
    ASSERT_EQ(0xABCD, SCSI::Structures::toU16(logSelectCDB.parameterListLength));
    buff[9] |= 0xBC;
    ASSERT_EQ(0xBC, logSelectCDB.control);
  }

  TEST(SCSI_Structures, LinuxSGIO_t) {
    ASSERT_EQ(sizeof(sg_io_hdr_t), sizeof(SCSI::Structures::LinuxSGIO_t));
    SCSI::Structures::LinuxSGIO_t lsg;
    /* Most important part: check that the class does not add data
     to the original structure (virtual table, for example)*/
    sg_io_hdr_t & sgio_hdr = *(sg_io_hdr_t *)&lsg;
    /* Also make sure the constructor does its initialization job */
    ASSERT_EQ(30000, sgio_hdr.timeout);
    ASSERT_EQ('S', sgio_hdr.interface_id);
    /* The rest is safe. It's just a struct with added functions */
  }

  TEST(SCSI_Structures, logSenseCDB_t) {
    SCSI::Structures::logSenseCDB_t logSenseCDB;
    unsigned char *buff = (unsigned char *)&logSenseCDB;
    
    /*
     * Make sure this struct is a POD (plain old data without virtual table)
     * (and has the right size).
     */
    ASSERT_EQ(10, sizeof(logSenseCDB));
    
    /* Check proper initialization an location of struct members match
     the bit/byte locations defined in SPC-4 */
    ASSERT_EQ(SCSI::Commands::LOG_SENSE, logSenseCDB.opCode);
    buff[0] = 0xAB;
    ASSERT_EQ(0xAB, logSenseCDB.opCode);
    
    ASSERT_EQ(0, logSenseCDB.SP);
    buff[1] |= (0x1 &   0x7) << 0;
    ASSERT_EQ(1, logSenseCDB.SP);
    
    ASSERT_EQ(0, logSenseCDB.PPC);
    buff[1] |= (0x1 &   0xF) << 1;
    ASSERT_EQ(1, logSenseCDB.PPC);
    
    ASSERT_EQ(0, logSenseCDB.pageCode);
    buff[2] |= (0xAB & 0x3F) << 0;  
    ASSERT_EQ(0x2B, logSenseCDB.pageCode);
    
    ASSERT_EQ(0, logSenseCDB.PC);
    buff[2] |= (0x2 &   0x3) << 6;
    ASSERT_EQ(0x2, logSenseCDB.PC);
    
    ASSERT_EQ(0, logSenseCDB.subPageCode);
    buff[3] = 0xBC;
    ASSERT_EQ(0xBC, logSenseCDB.subPageCode);
    
    ASSERT_EQ(0, SCSI::Structures::toU16(logSenseCDB.parameterPointer));
    buff[5] = 0x12; buff[6] = 0x34;
    ASSERT_EQ(0x1234, SCSI::Structures::toU16(logSenseCDB.parameterPointer));
    
    ASSERT_EQ(0, SCSI::Structures::toU16(logSenseCDB.allocationLength));
    buff[7] |= 0xAB; buff[8] |= 0xCD;
    ASSERT_EQ(0xABCD, SCSI::Structures::toU16(logSenseCDB.allocationLength));
    
    ASSERT_EQ(0, logSenseCDB.control);
    buff[9] |= 0xBC;
    ASSERT_EQ(0xBC, logSenseCDB.control);
  }
  
  TEST(SCSI_Structures, tapeAlertLogPage_t_and_parameters) {
    SCSI::Structures::tapeAlertLogPage_t<12> tal;
    unsigned char * buff = (unsigned char *) & tal;
    
    /* Check the size of the structure (header is 4 bytes, array elements, 5.*/
    /* As usual, this ensures we have POD */
    ASSERT_EQ(4 + 12*5, sizeof(tal));
    
    ASSERT_EQ(0, tal.pageCode);
    buff[0] |= (0x12 & 0x3F) << 0;
    ASSERT_EQ(0x12, tal.pageCode);
    
    ASSERT_EQ(0,tal.subPageCode);
    buff[1] = 0x34;
    ASSERT_EQ(0x34, tal.subPageCode);
    
    /* Simulate 123 records = 600 bytes = 0x267 */
    ASSERT_EQ(0, SCSI::Structures::toU16(tal.pageLength));
    buff[2] = 0x2; buff[3]= 0x67;
    ASSERT_EQ(0x267, SCSI::Structures::toU16(tal.pageLength));
    /* The page length is counted in bytes. We are interested in the number of parameters*/
    ASSERT_EQ(123, tal.parameterNumber());
    
    ASSERT_EQ(0, SCSI::Structures::toU16(tal.parameters[0].parameterCode));
    buff [4] = 0xCA; buff[5] = 0xFE;
    ASSERT_EQ(0xCAFE, SCSI::Structures::toU16(tal.parameters[0].parameterCode));
    
    ASSERT_EQ(0, tal.parameters[11].flag);
    buff[3+12*5] |= (0x1) << 0;
    ASSERT_EQ(1, tal.parameters[11].flag);
  }
  
  TEST(SCSI_Structures, senseBuffer_t) {
    SCSI::Structures::senseData_t<255> sense;
    unsigned char * buff = (unsigned char *) & sense;
    
    /* Check the total size of the structure, plus the one of each of the members
     of the union */
    ASSERT_EQ(255, sizeof(sense));
    ASSERT_EQ(255 - 1, sizeof(sense.descriptorFormat));
    ASSERT_EQ(255 - 1, sizeof(sense.fixedFormat));
    
    buff[0] = 0x70;
    buff[12] = 0x12;
    buff[13] = 0x34;
    ASSERT_EQ(true, sense.isCurrent());
    ASSERT_EQ(false, sense.isDeffered());
    ASSERT_EQ(true, sense.isFixedFormat());
    ASSERT_EQ(false, sense.isDescriptorFormat());
    ASSERT_EQ(0x12, sense.getASC());
    ASSERT_EQ(0x34, sense.getASCQ());
    
    buff[0] = 0x71;
    buff[12] = 0x12;
    buff[13] = 0x34;
    ASSERT_EQ(false, sense.isCurrent());
    ASSERT_EQ(true, sense.isDeffered());
    ASSERT_EQ(true, sense.isFixedFormat());
    ASSERT_EQ(false, sense.isDescriptorFormat());
    ASSERT_EQ(0x12, sense.getASC());
    ASSERT_EQ(0x34, sense.getASCQ());
    
    buff[0] = 0x72;
    buff[2] = 0x56;
    buff[3] = 0x78;
    ASSERT_EQ(true, sense.isCurrent());
    ASSERT_EQ(false, sense.isDeffered());
    ASSERT_EQ(false, sense.isFixedFormat());
    ASSERT_EQ(true, sense.isDescriptorFormat());
    ASSERT_EQ(0x56, sense.getASC());
    ASSERT_EQ(0x78, sense.getASCQ());
    
    buff[0] = 0x73;
    buff[2] = 0x56;
    buff[3] = 0x78;
    ASSERT_EQ(false, sense.isCurrent());
    ASSERT_EQ(true, sense.isDeffered());
    ASSERT_EQ(false, sense.isFixedFormat());
    ASSERT_EQ(true, sense.isDescriptorFormat());
    ASSERT_EQ(0x56, sense.getASC());
    ASSERT_EQ(0x78, sense.getASCQ());
    
    buff[0] = 0x74;
    ASSERT_THROW(sense.getASC(), Tape::Exception);
  }
  
  TEST(SCSI_Structures, toU16) {
    unsigned char num[2] = { 0x1, 0x2 };
    ASSERT_EQ( 0x102, SCSI::Structures::toU16(num));
  }
  
  TEST(SCSI_Structures, toU32) {
    unsigned char num[4] = { 0x1, 0x2, 0x3, 0x4 };
    ASSERT_EQ( 0x1020304, SCSI::Structures::toU32(num));
  }
  
  TEST(SCSI_Structures, toU64) {
    unsigned char num[8] = { 0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xFA, 0xDE };
    ASSERT_EQ ( 0xDEADBEEFCAFEFADEULL, SCSI::Structures::toU64(num));
  }
};
