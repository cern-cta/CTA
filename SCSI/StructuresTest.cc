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
#include "Exception.hh"

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
    ASSERT_EQ(6U, sizeof(inqCDB));
    
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
    ASSERT_EQ(10U, sizeof(logSelectCDB));
    
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
    ASSERT_EQ(30000U, sgio_hdr.timeout);
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
    ASSERT_EQ(10U, sizeof(logSenseCDB));
    
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
  
  TEST(SCSI_Structures, locate10CDB_t) {
    SCSI::Structures::locate10CDB_t locate10CDB;
    unsigned char *buff = (unsigned char *)&locate10CDB;
    
    /*
     * Make sure this struct is a POD (plain old data without virtual table)
     * (and has the right size).
     */
    ASSERT_EQ(10U, sizeof(locate10CDB));  // that is why it called locate 10
    
    /* Check proper initialization an location of struct members match
     the bit/byte locations defined in SPC-4 */
    ASSERT_EQ(SCSI::Commands::LOCATE_10, locate10CDB.opCode);
    buff[0] = 0xAB;
    ASSERT_EQ(0xAB, locate10CDB.opCode);
    
    ASSERT_EQ(0, locate10CDB.IMMED);
    buff[1] |= (0x1 &   0x7) << 0;
    ASSERT_EQ(1, locate10CDB.IMMED);
    
    ASSERT_EQ(0, locate10CDB.CP);
    buff[1] |= (0x1 &   0xF) << 1;
    ASSERT_EQ(1, locate10CDB.CP);
    
    ASSERT_EQ(0, locate10CDB.BT);
    buff[1] |= (0x1 &   0xF) << 2;  
    ASSERT_EQ(1, locate10CDB.BT);
    
    ASSERT_EQ(0U, SCSI::Structures::toU32(locate10CDB.logicalObjectID));
    buff[3] |= 0x0A;buff[4] |= 0xBC;buff[5] |= 0xDE;buff[6] |= 0xF0;
    ASSERT_EQ(0x0ABCDEF0U, SCSI::Structures::toU32(locate10CDB.logicalObjectID));
    
    ASSERT_EQ(0, locate10CDB.partition);
    buff[8] = 0xAB;
    ASSERT_EQ(0xAB, locate10CDB.partition);
       
    ASSERT_EQ(0, locate10CDB.control);
    buff[9] |= 0xBC;
    ASSERT_EQ(0xBC, locate10CDB.control);
  }
  
  TEST(SCSI_Structures, readPositionCDB_t) {
    SCSI::Structures::readPositionCDB_t readPositionCDB;
    unsigned char *buff = (unsigned char *)&readPositionCDB;  
    /*
     * Make sure this struct is a POD (plain old data without virtual table)
     * (and has the right size).
     */
    ASSERT_EQ(10U, sizeof(readPositionCDB)); 
    
    /* Check proper initialization an location of struct members match
     the bit/byte locations defined in SPC-4 */
    ASSERT_EQ(SCSI::Commands::READ_POSITION, readPositionCDB.opCode);
    buff[0] = 0xAB;
    ASSERT_EQ(0xAB, readPositionCDB.opCode);
    
    ASSERT_EQ(0, readPositionCDB.serviceAction);
    buff[1] |= (0x15 &   0xFF) << 0;
    ASSERT_EQ(0x15, readPositionCDB.serviceAction);
    
    buff[2] |= 0xFF; buff[3] = 0xFF; buff[4] = 0xFF; buff[5] = 0xFF; buff[6] = 0xFF;
    
    ASSERT_EQ(0, SCSI::Structures::toU16(readPositionCDB.allocationLenght));
    buff[7] |= 0x0A;buff[8] |= 0xBC;
    ASSERT_EQ(0x0ABC, SCSI::Structures::toU16(readPositionCDB.allocationLenght));
           
    ASSERT_EQ(0, readPositionCDB.control);
    buff[9] |= 0xBC;
    ASSERT_EQ(0xBC, readPositionCDB.control);
  }
  
   TEST(SCSI_Structures, readPositionDataShortForm_t) {
    SCSI::Structures::readPositionDataShortForm_t readPositionData;
    unsigned char *buff = (unsigned char *)&readPositionData;  

    ASSERT_EQ(20U, sizeof(readPositionData)); 
    
    ASSERT_EQ(0, readPositionData.BPEW);
    buff[0] |= (0x1 &   0xFF) << 0;
    ASSERT_EQ(0x1, readPositionData.BPEW);
    ASSERT_EQ(0, readPositionData.PERR);
    buff[0] |= (0x2 &   0xFF) << 0;
    ASSERT_EQ(0x1, readPositionData.PERR);
    ASSERT_EQ(0, readPositionData.LOLU);
    buff[0] |= (0x4 &   0xFF) << 0;
    ASSERT_EQ(0x1, readPositionData.LOLU);
    ASSERT_EQ(0, readPositionData.BYCU);
    buff[0] |= (0x10 &   0xFF) << 0;
    ASSERT_EQ(0x1, readPositionData.BYCU);
    ASSERT_EQ(0, readPositionData.LOCU);
    buff[0] |= (0x20 &   0xFF) << 0;
    ASSERT_EQ(0x1, readPositionData.LOCU);
    ASSERT_EQ(0, readPositionData.EOP);
    buff[0] |= (0x40 &   0xFF) << 0;
    ASSERT_EQ(0x1, readPositionData.EOP);
    ASSERT_EQ(0, readPositionData.BOP);
    buff[0] |= (0x80 &   0xFF) << 0;
    ASSERT_EQ(0x1, readPositionData.BOP);
    
    ASSERT_EQ(0, readPositionData.partitionNumber);
    buff[1] |= 0xBC;
    ASSERT_EQ(0xBC, readPositionData.partitionNumber);
        
    buff[2] |= 0xFF; buff[3] = 0xFF;
    
    ASSERT_EQ(0U, SCSI::Structures::toU32(readPositionData.firstBlockLocation));
    buff[4] |= 0x0A;buff[5] |= 0xBC;buff[6] |= 0xDE;buff[7] |= 0xF0;
    ASSERT_EQ(0x0ABCDEF0U, SCSI::Structures::toU32(readPositionData.firstBlockLocation));
    
    ASSERT_EQ(0U, SCSI::Structures::toU32(readPositionData.lastBlockLocation));
    buff[8] |= 0x9A;buff[9] |= 0xBC;buff[10] |= 0xDE;buff[11] |= 0xF9;
    ASSERT_EQ(0x9ABCDEF9U, SCSI::Structures::toU32(readPositionData.lastBlockLocation));
    buff[12] |= 0xFF;
    ASSERT_EQ(0U, SCSI::Structures::toU32(readPositionData.blocksInBuffer));
    buff[13] |= 0x9A;buff[14] |= 0xBC;buff[15] |= 0xDE;
    ASSERT_EQ(0x009ABCDEU, SCSI::Structures::toU32(readPositionData.blocksInBuffer));
           
    ASSERT_EQ(0U, SCSI::Structures::toU32(readPositionData.bytesInBuffer));
    buff[16] |= 0x7A;buff[17] |= 0xBC;buff[18] |= 0xDE;buff[19] |= 0xF7;
    ASSERT_EQ(0x7ABCDEF7U, SCSI::Structures::toU32(readPositionData.bytesInBuffer));
  }
  
  TEST(SCSI_Structures, modeSelect6CDB_t) {
    SCSI::Structures::modeSelect6CDB_t modeSelect6CDB;
    unsigned char *buff = (unsigned char *)&modeSelect6CDB;  
    /*
     * Make sure this struct is a POD (plain old data without virtual table)
     * (and has the right size).
     */
    ASSERT_EQ(6U, sizeof(modeSelect6CDB)); 
    
    /* Check proper initialization an location of struct members match
     the bit/byte locations defined in SPC-4 */
    ASSERT_EQ(SCSI::Commands::MODE_SELECT_6, modeSelect6CDB.opCode);
    buff[0] = 0xAB;
    ASSERT_EQ(0xABU, modeSelect6CDB.opCode);
    
    ASSERT_EQ(0, modeSelect6CDB.SP);
    buff[1] |= (0x1 &   0xFF) << 0;
    ASSERT_EQ(0x1U, modeSelect6CDB.SP);
    
    ASSERT_EQ(0U, modeSelect6CDB.PF);
    buff[1] |= (0x10 &   0xFF) << 0;
    ASSERT_EQ(0x1U, modeSelect6CDB.PF);
    
    buff[2] |= 0xFF; buff[3] = 0xFF; 
    
    ASSERT_EQ(0U, modeSelect6CDB.paramListLength);
    buff[4] |= 0xBC;
    ASSERT_EQ(0xBCU, modeSelect6CDB.paramListLength);
           
    ASSERT_EQ(0, modeSelect6CDB.control);
    buff[5] |= 0xAB;
    ASSERT_EQ(0xAB, modeSelect6CDB.control);
  }
  
  TEST(SCSI_Structures, modeSense6CDB_t) {
    SCSI::Structures::modeSense6CDB_t modeSense6CDB;
    unsigned char *buff = (unsigned char *)&modeSense6CDB;  
    /*
     * Make sure this struct is a POD (plain old data without virtual table)
     * (and has the right size).
     */
    ASSERT_EQ(6U, sizeof(modeSense6CDB)); 
    
    /* Check proper initialization an location of struct members match
     the bit/byte locations defined in SPC-4 */
    ASSERT_EQ(SCSI::Commands::MODE_SENSE_6, modeSense6CDB.opCode);
    buff[0] = 0xAB;
    ASSERT_EQ(0xABU, modeSense6CDB.opCode);
    
    ASSERT_EQ(0, modeSense6CDB.DBD);
    buff[1] |= (0x8 &   0xFF) << 0;
    ASSERT_EQ(0x1U, modeSense6CDB.DBD);
    
    ASSERT_EQ(0U, modeSense6CDB.pageCode);
    buff[2] |= (0x2A &   0xFF) << 0;
    ASSERT_EQ(0x2AU,  modeSense6CDB.pageCode);
    
    ASSERT_EQ(0u, modeSense6CDB.PC);
    buff[2] |= (0x8B &   0xFF) << 0;
    ASSERT_EQ(0x2U,  modeSense6CDB.PC);
    
    ASSERT_EQ(0U,  modeSense6CDB.subPageCode);
    buff[3] |= 0xBC;
    ASSERT_EQ(0xBCU, modeSense6CDB.subPageCode);
           
    ASSERT_EQ(0U, modeSense6CDB.allocationLenght);
    buff[4] |= 0xAB;
    ASSERT_EQ(0xABU, modeSense6CDB.allocationLenght);
    
    ASSERT_EQ(0U, modeSense6CDB.control);
    buff[5] |= 0xCD;
    ASSERT_EQ(0xCDU, modeSense6CDB.control);
  }  
  
  TEST(SCSI_Structures, modeSenseDeviceConfiguration_t) {
    SCSI::Structures::modeSenseDeviceConfiguration_t devConfig;
    unsigned char *buff = (unsigned char *)&devConfig;  
    /*
     * Make sure this struct is a POD (plain old data without virtual table)
     * (and has the right size).
     */
    ASSERT_EQ(28U, sizeof(devConfig));
    ASSERT_EQ(4U, sizeof(devConfig.header));
    ASSERT_EQ(8U, sizeof(devConfig.blockDescriptor));
    ASSERT_EQ(16U, sizeof(devConfig.modePage));
    
    /* We will only check used by us parameters */
    ASSERT_EQ(0U, devConfig.header.modeDataLength);
    buff[0] |= 0xAB;
    ASSERT_EQ(0xABU, devConfig.header.modeDataLength);
    
    buff[1] = buff[2] = buff[3] = 0xFF;
    ASSERT_EQ(0U, devConfig.blockDescriptor.densityCode);
    buff[4] |= 0xCD;
    ASSERT_EQ(0xCDU, devConfig.blockDescriptor.densityCode);
    
    for(int i=5;i<26;i++) buff[i] = 0xFF; // fill the space
    ASSERT_EQ(0U, devConfig.modePage.selectDataComprAlgorithm );
    buff[26] |= 0xEF;
    ASSERT_EQ(0xEFU, devConfig.modePage.selectDataComprAlgorithm);
  }  
  
  TEST(SCSI_Structures, tapeAlertLogPage_t_and_parameters) {
    SCSI::Structures::tapeAlertLogPage_t<12> tal;
    unsigned char * buff = (unsigned char *) & tal;
    
    /* Check the size of the structure (header is 4 bytes, array elements, 5.*/
    /* As usual, this ensures we have POD */
    ASSERT_EQ(4U + 12U*5U, sizeof(tal));
    
    ASSERT_EQ(0U, tal.pageCode);
    buff[0] |= (0x12 & 0x3F) << 0;
    ASSERT_EQ(0x12U, tal.pageCode);
    
    ASSERT_EQ(0U,tal.subPageCode);
    buff[1] = 0x34;
    ASSERT_EQ(0x34U, tal.subPageCode);
    
    /* Simulate 123 records = 600 bytes = 0x267 */
    ASSERT_EQ(0U, SCSI::Structures::toU16(tal.pageLength));
    buff[2] = 0x2; buff[3]= 0x67;
    ASSERT_EQ(0x267U, SCSI::Structures::toU16(tal.pageLength));
    /* The page length is counted in bytes. We are interested in the number of parameters*/
    ASSERT_EQ(123U, tal.parameterNumber());
    
    ASSERT_EQ(0U, SCSI::Structures::toU16(tal.parameters[0].parameterCode));
    buff [4] = 0xCA; buff[5] = 0xFE;
    ASSERT_EQ(0xCAFEU, SCSI::Structures::toU16(tal.parameters[0].parameterCode));
    
    ASSERT_EQ(0U, tal.parameters[11].flag);
    buff[3+12*5] |= (0x1) << 0;
    ASSERT_EQ(1U, tal.parameters[11].flag);
  }
  
  TEST(SCSI_Structures, senseBuffer_t) {
    SCSI::Structures::senseData_t<255> sense;
    unsigned char * buff = (unsigned char *) & sense;
    
    /* Check the total size of the structure, plus the one of each of the members
     of the union */
    ASSERT_EQ(255U, sizeof(sense));
    ASSERT_EQ(255U - 1U, sizeof(sense.descriptorFormat));
    ASSERT_EQ(255U - 1U, sizeof(sense.fixedFormat));
    
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
    buff[2] = 0x0b;
    buff[3] = 0x08;
    ASSERT_EQ(false, sense.isCurrent());
    ASSERT_EQ(true, sense.isDeffered());
    ASSERT_EQ(false, sense.isFixedFormat());
    ASSERT_EQ(true, sense.isDescriptorFormat());
    ASSERT_EQ(0x0b, sense.getASC());
    ASSERT_EQ(0x08, sense.getASCQ());
    ASSERT_EQ("Warning - power loss expected", sense.getACSString());
    
    buff[2] = 0x40;
    buff[3] = 0xab;
    ASSERT_EQ("Diagnostic failure on component (ab)", sense.getACSString());
    
    buff[2] = 0x00;
    buff[3] = 0x1F;
    ASSERT_EQ("Unknown ASC/ASCQ:00/1f", sense.getACSString());
    
    buff[0] = 0x74;
    ASSERT_THROW(sense.getASC(), Tape::Exception);
    
    ASSERT_THROW(sense.getACSString(), Tape::Exception);
    
    try { sense.getACSString(); ASSERT_TRUE(false); }
    catch (Tape::Exception & ex) {
      std::string what(ex.shortWhat());
      ASSERT_NE(std::string::npos, what.find("response code not supported (0x74)"));
    }
  }
  
  TEST(SCSI_Structures, toU16) {
    unsigned char num[2] = { 0x1, 0x2 };
    ASSERT_EQ( 0x102, SCSI::Structures::toU16(num));
  }
  
  TEST(SCSI_Structures, toU32) {
    unsigned char num[4] = { 0x1, 0x2, 0x3, 0x4 };
    ASSERT_EQ( 0x1020304U, SCSI::Structures::toU32(num));
  }
  
  TEST(SCSI_Structures, toU32_3byte) {
    unsigned char num[3] = { 0xAA, 0xBB, 0xCC };
    ASSERT_EQ( 0x00AABBCCU, SCSI::Structures::toU32(num));
  }
  
  TEST(SCSI_Structures, toS32) {
    unsigned char num[4] = { 0xE6, 0x29, 0x66, 0x5B };
    ASSERT_EQ( -433494437, SCSI::Structures::toS32(num));
  }
  
  TEST(SCSI_Structures, toU64) {
    unsigned char num[8] = { 0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xFA, 0xDE };
    ASSERT_EQ ( 0xDEADBEEFCAFEFADEU, SCSI::Structures::toU64(num));
  }
  
  TEST(SCSI_Strucutres, Exception) {
    SCSI::Structures::senseData_t<255> sense;
    SCSI::Structures::LinuxSGIO_t sgio;
    sgio.setSenseBuffer(&sense);
    sgio.status = SCSI::Status::GOOD;
    ASSERT_NO_THROW(SCSI::ExceptionLauncher(sgio));
    sgio.status = SCSI::Status::CHECK_CONDITION;
    /* fill up the ASC part of the */
    sense.responseCode = 0x70;
    sense.fixedFormat.ASC = 0x14;
    sense.fixedFormat.ASCQ = 0x04;
    ASSERT_THROW(SCSI::ExceptionLauncher(sgio), SCSI::Exception);
    try { SCSI::ExceptionLauncher(sgio, "In exception validation:"); ASSERT_TRUE(false); }
    catch (SCSI::Exception & ex) {
      std::string what(ex.shortWhat());
      ASSERT_NE(std::string::npos, what.find("Block sequence error"));
      /* We check here that the formatting is also done correctly (space added when context
       not empty */
      ASSERT_NE(std::string::npos, what.find("In exception validation: "));
    }
  }
}
