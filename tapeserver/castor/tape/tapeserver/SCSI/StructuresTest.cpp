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

#include <gtest/gtest.h>
#include <gmock/gmock-cardinalities.h>
#include "Device.hpp"
#include "Structures.hpp"
#include "../system/Wrapper.hpp"
#include "Exception.hpp"

using ::testing::AtLeast;
using ::testing::Return;
using ::testing::_;

namespace unitTests {
  TEST(castor_tape_SCSI_Structures, inquiryData_t_multi_byte_numbers_strings) {
    /* Validate the bit field behavior of the struct inquiryData_t,
     which represents the standard INQUIRY data format as defined in 
     SPC-4. This test also validates the handling of multi-bytes numbers,
     as SCSI structures are big endian (and main development target is 
     little endian.  */
    castor::tape::SCSI::Structures::inquiryData_t inq;
    unsigned char *inqBuff = (unsigned char*)&inq;
    memset(inqBuff, 0, sizeof(inq));
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
    ASSERT_EQ("", castor::tape::SCSI::Structures::toString(inq.T10Vendor));
    inqBuff[8] = 'V';
    inqBuff[9] = 'i';
    inqBuff[10] = 'r';
    inqBuff[11] = 't';
    inqBuff[12] = 'u';
    inqBuff[13] = 'a';
    inqBuff[14] = 'l';
    inqBuff[15] = 's';
    ASSERT_EQ("Virtuals", castor::tape::SCSI::Structures::toString(inq.T10Vendor));
    
    /* Check there is no side effect from next record */
    inqBuff[16] = 'X';
    inqBuff[17] = 'Y';
    inqBuff[18] = 'Z';
    ASSERT_EQ("Virtuals", castor::tape::SCSI::Structures::toString(inq.T10Vendor));
    ASSERT_EQ("XYZ", castor::tape::SCSI::Structures::toString(inq.prodId));
            
    /* Check that non-full record does not yield too long a string */
    inqBuff[8] = 'T';
    inqBuff[9] = 'a';
    inqBuff[10] = 'p';
    inqBuff[11] = 'e';
    inqBuff[12] = 's';
    inqBuff[13] = '\0';
    inqBuff[14] = '\0';
    inqBuff[15] = '\0';
    ASSERT_EQ("Tapes", castor::tape::SCSI::Structures::toString(inq.T10Vendor));

    /* Test of endian conversion */
    ASSERT_EQ(0, castor::tape::SCSI::Structures::toU16(inq.versionDescriptor[7]));
    inqBuff[72] = 0xCA;
    inqBuff[73] = 0xFE;
    ASSERT_EQ(0xCAFE, castor::tape::SCSI::Structures::toU16(inq.versionDescriptor[7]));
    
    /* Test last element */
    ASSERT_EQ(0, *inq.vendorSpecific2);
    inqBuff[96] = 0x12;
    ASSERT_EQ(0x12, *inq.vendorSpecific2);
  }

  TEST(castor_tape_SCSI_Structures, inquiryCDB_t) {
    castor::tape::SCSI::Structures::inquiryCDB_t inqCDB;
    unsigned char *buff = (unsigned char *)&inqCDB;
    
    /*
     * Make sure this struct is a POD (plain old data without virtual table)
     * (and has the right size).
     */
    ASSERT_EQ(6U, sizeof(inqCDB));
    
    ASSERT_EQ(castor::tape::SCSI::Commands::INQUIRY, inqCDB.opCode);
    buff[0] = 0;
    ASSERT_EQ(0, inqCDB.opCode);
    
    ASSERT_EQ(0, inqCDB.EVPD);
    buff[1] = 0x1;
    ASSERT_EQ(1, inqCDB.EVPD);
    
    ASSERT_EQ(0, inqCDB.control);
    buff[5] = 0xCA;
    ASSERT_EQ(0xCA, inqCDB.control);
  }
  
    TEST(castor_tape_SCSI_Structures, inquiryUnitSerialNumberData_t) {
    castor::tape::SCSI::Structures::inquiryUnitSerialNumberData_t inqSerialNumber;
    unsigned char *buff = (unsigned char *)&inqSerialNumber;
    
    /*
     * Make sure this struct is a POD (plain old data without virtual table)
     * (and has the right size).
     */
    ASSERT_EQ(16U, sizeof(inqSerialNumber));
       
    ASSERT_EQ(0, inqSerialNumber.peripheralDeviceType);
    buff[0] |= (0x15 & 0x1F) << 0;
    ASSERT_EQ(0x15, inqSerialNumber.peripheralDeviceType);
    
    ASSERT_EQ(0, inqSerialNumber.peripheralQualifier);
    buff[0] |= (0x5 & 0x7) <<5;
    ASSERT_EQ(0x5,inqSerialNumber.peripheralQualifier);
    
    ASSERT_EQ(0, inqSerialNumber.pageCode);
    buff[1] |= 0xAB ;
    ASSERT_EQ(0xAB, inqSerialNumber.pageCode);
    
    buff[2] |= 0xFF;
    ASSERT_EQ(0, inqSerialNumber.pageLength);
    buff[3] |= 0xCD ;
    ASSERT_EQ(0xCD, inqSerialNumber.pageLength);
    
    ASSERT_EQ("", castor::tape::SCSI::Structures::toString(inqSerialNumber.productSerialNumber));
    const char serialNumber[13] = "XYZZY_A2    ";
    memcpy(&buff[4],serialNumber,12);
    ASSERT_EQ("XYZZY_A2    ", castor::tape::SCSI::Structures::toString(inqSerialNumber.productSerialNumber));
  }
  
  TEST(castor_tape_SCSI_Structures, logSelectCDB_t) {
    castor::tape::SCSI::Structures::logSelectCDB_t logSelectCDB;
    unsigned char *buff = (unsigned char *)&logSelectCDB;
    
    /*
     * Make sure this struct is a POD (plain old data without virtual table)
     * (and has the right size).
     */
    ASSERT_EQ(10U, sizeof(logSelectCDB));
    
    ASSERT_EQ(castor::tape::SCSI::Commands::LOG_SELECT, logSelectCDB.opCode);
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
    ASSERT_EQ(0xABCD, castor::tape::SCSI::Structures::toU16(logSelectCDB.parameterListLength));
    buff[9] |= 0xBC;
    ASSERT_EQ(0xBC, logSelectCDB.control);
  }

  TEST(castor_tape_SCSI_Structures, LinuxSGIO_t) {
    ASSERT_EQ(sizeof(sg_io_hdr_t), sizeof(castor::tape::SCSI::Structures::LinuxSGIO_t));
    castor::tape::SCSI::Structures::LinuxSGIO_t lsg;
    /* Most important part: check that the class does not add data
     to the original structure (virtual table, for example)*/
    sg_io_hdr_t & sgio_hdr = *(sg_io_hdr_t *)&lsg;
    /* Also make sure the constructor does its initialization job */
    ASSERT_EQ(900000U, sgio_hdr.timeout);
    ASSERT_EQ('S', sgio_hdr.interface_id);
    /* The rest is safe. It's just a struct with added functions */
  }

  TEST(castor_tape_SCSI_Structures, logSenseCDB_t) {
    castor::tape::SCSI::Structures::logSenseCDB_t logSenseCDB;
    unsigned char *buff = (unsigned char *)&logSenseCDB;
    
    /*
     * Make sure this struct is a POD (plain old data without virtual table)
     * (and has the right size).
     */
    ASSERT_EQ(10U, sizeof(logSenseCDB));
    
    /* Check proper initialization an location of struct members match
     the bit/byte locations defined in SPC-4 */
    ASSERT_EQ(castor::tape::SCSI::Commands::LOG_SENSE, logSenseCDB.opCode);
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
    
    ASSERT_EQ(0, castor::tape::SCSI::Structures::toU16(logSenseCDB.parameterPointer));
    buff[5] = 0x12; buff[6] = 0x34;
    ASSERT_EQ(0x1234, castor::tape::SCSI::Structures::toU16(logSenseCDB.parameterPointer));
    
    ASSERT_EQ(0, castor::tape::SCSI::Structures::toU16(logSenseCDB.allocationLength));
    buff[7] |= 0xAB; buff[8] |= 0xCD;
    ASSERT_EQ(0xABCD, castor::tape::SCSI::Structures::toU16(logSenseCDB.allocationLength));
    
    ASSERT_EQ(0, logSenseCDB.control);
    buff[9] |= 0xBC;
    ASSERT_EQ(0xBC, logSenseCDB.control);
  }
  
  TEST(castor_tape_SCSI_Structures, locate10CDB_t) {
    castor::tape::SCSI::Structures::locate10CDB_t locate10CDB;
    unsigned char *buff = (unsigned char *)&locate10CDB;
    
    /*
     * Make sure this struct is a POD (plain old data without virtual table)
     * (and has the right size).
     */
    ASSERT_EQ(10U, sizeof(locate10CDB));  // that is why it called locate 10
    
    /* Check proper initialization an location of struct members match
     the bit/byte locations defined in SPC-4 */
    ASSERT_EQ(castor::tape::SCSI::Commands::LOCATE_10, locate10CDB.opCode);
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
    
    ASSERT_EQ(0U, castor::tape::SCSI::Structures::toU32(locate10CDB.logicalObjectID));
    buff[3] |= 0x0A;buff[4] |= 0xBC;buff[5] |= 0xDE;buff[6] |= 0xF0;
    ASSERT_EQ(0x0ABCDEF0U, castor::tape::SCSI::Structures::toU32(locate10CDB.logicalObjectID));
    
    ASSERT_EQ(0, locate10CDB.partition);
    buff[8] = 0xAB;
    ASSERT_EQ(0xAB, locate10CDB.partition);
       
    ASSERT_EQ(0, locate10CDB.control);
    buff[9] |= 0xBC;
    ASSERT_EQ(0xBC, locate10CDB.control);
  }
  
  TEST(castor_tape_SCSI_Structures, readPositionCDB_t) {
    castor::tape::SCSI::Structures::readPositionCDB_t readPositionCDB;
    unsigned char *buff = (unsigned char *)&readPositionCDB;  
    /*
     * Make sure this struct is a POD (plain old data without virtual table)
     * (and has the right size).
     */
    ASSERT_EQ(10U, sizeof(readPositionCDB)); 
    
    /* Check proper initialization an location of struct members match
     the bit/byte locations defined in SPC-4 */
    ASSERT_EQ(castor::tape::SCSI::Commands::READ_POSITION, readPositionCDB.opCode);
    buff[0] = 0xAB;
    ASSERT_EQ(0xAB, readPositionCDB.opCode);
    
    ASSERT_EQ(0, readPositionCDB.serviceAction);
    buff[1] |= (0x15 &   0xFF) << 0;
    ASSERT_EQ(0x15, readPositionCDB.serviceAction);
    
    buff[2] |= 0xFF; buff[3] = 0xFF; buff[4] = 0xFF; buff[5] = 0xFF; buff[6] = 0xFF;
    
    ASSERT_EQ(0, castor::tape::SCSI::Structures::toU16(readPositionCDB.allocationLength));
    buff[7] |= 0x0A;buff[8] |= 0xBC;
    ASSERT_EQ(0x0ABC, castor::tape::SCSI::Structures::toU16(readPositionCDB.allocationLength));
           
    ASSERT_EQ(0, readPositionCDB.control);
    buff[9] |= 0xBC;
    ASSERT_EQ(0xBC, readPositionCDB.control);
  }
  
   TEST(castor_tape_SCSI_Structures, readPositionDataShortForm_t) {
    castor::tape::SCSI::Structures::readPositionDataShortForm_t readPositionData;
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
    
    ASSERT_EQ(0U, castor::tape::SCSI::Structures::toU32(readPositionData.firstBlockLocation));
    buff[4] |= 0x0A;buff[5] |= 0xBC;buff[6] |= 0xDE;buff[7] |= 0xF0;
    ASSERT_EQ(0x0ABCDEF0U, castor::tape::SCSI::Structures::toU32(readPositionData.firstBlockLocation));
    
    ASSERT_EQ(0U, castor::tape::SCSI::Structures::toU32(readPositionData.lastBlockLocation));
    buff[8] |= 0x9A;buff[9] |= 0xBC;buff[10] |= 0xDE;buff[11] |= 0xF9;
    ASSERT_EQ(0x9ABCDEF9U, castor::tape::SCSI::Structures::toU32(readPositionData.lastBlockLocation));
    buff[12] |= 0xFF;
    ASSERT_EQ(0U, castor::tape::SCSI::Structures::toU32(readPositionData.blocksInBuffer));
    buff[13] |= 0x9A;buff[14] |= 0xBC;buff[15] |= 0xDE;
    ASSERT_EQ(0x009ABCDEU, castor::tape::SCSI::Structures::toU32(readPositionData.blocksInBuffer));
           
    ASSERT_EQ(0U, castor::tape::SCSI::Structures::toU32(readPositionData.bytesInBuffer));
    buff[16] |= 0x7A;buff[17] |= 0xBC;buff[18] |= 0xDE;buff[19] |= 0xF7;
    ASSERT_EQ(0x7ABCDEF7U, castor::tape::SCSI::Structures::toU32(readPositionData.bytesInBuffer));
  }
  
  TEST(castor_tape_SCSI_Structures, modeSelect6CDB_t) {
    castor::tape::SCSI::Structures::modeSelect6CDB_t modeSelect6CDB;
    unsigned char *buff = (unsigned char *)&modeSelect6CDB;  
    /*
     * Make sure this struct is a POD (plain old data without virtual table)
     * (and has the right size).
     */
    ASSERT_EQ(6U, sizeof(modeSelect6CDB)); 
    
    /* Check proper initialization an location of struct members match
     the bit/byte locations defined in SPC-4 */
    ASSERT_EQ(castor::tape::SCSI::Commands::MODE_SELECT_6, modeSelect6CDB.opCode);
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
  
  TEST(castor_tape_SCSI_Structures, modeSense6CDB_t) {
    castor::tape::SCSI::Structures::modeSense6CDB_t modeSense6CDB;
    unsigned char *buff = (unsigned char *)&modeSense6CDB;  
    /*
     * Make sure this struct is a POD (plain old data without virtual table)
     * (and has the right size).
     */
    ASSERT_EQ(6U, sizeof(modeSense6CDB)); 
    
    /* Check proper initialization an location of struct members match
     the bit/byte locations defined in SPC-4 */
    ASSERT_EQ(castor::tape::SCSI::Commands::MODE_SENSE_6, modeSense6CDB.opCode);
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
           
    ASSERT_EQ(0U, modeSense6CDB.allocationLength);
    buff[4] |= 0xAB;
    ASSERT_EQ(0xABU, modeSense6CDB.allocationLength);
    
    ASSERT_EQ(0U, modeSense6CDB.control);
    buff[5] |= 0xCD;
    ASSERT_EQ(0xCDU, modeSense6CDB.control);
  }  
  
  TEST(castor_tape_SCSI_Structures, modeSenseDeviceConfiguration_t) {
    castor::tape::SCSI::Structures::modeSenseDeviceConfiguration_t devConfig;
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
  TEST(castor_tape_SCSI_Structures, modeSenseControlDataProtection_t) {
    castor::tape::SCSI::Structures::modeSenseControlDataProtection_t dataProt;
    unsigned char *buff = (unsigned char *)&dataProt;  
    /*
     * Make sure this struct is a POD (plain old data without virtual table)
     * (and has the right size).
     */
    ASSERT_EQ(44U, sizeof(dataProt));
    ASSERT_EQ(4U, sizeof(dataProt.header));
    ASSERT_EQ(8U, sizeof(dataProt.blockDescriptor));
    ASSERT_EQ(32U, sizeof(dataProt.modePage));
    
    /* We will only check used by us parameters */
    ASSERT_EQ(0U, dataProt.header.modeDataLength);
    buff[0] |= 0xAC;
    ASSERT_EQ(0xACU, dataProt.header.modeDataLength);
    
    for(int i=1;i<=15;i++) buff[i] = 0xFF; // fill the space
    ASSERT_EQ(0U, dataProt.modePage.LBPMethod );
    buff[16] |= 0xEF;
    ASSERT_EQ(0xEFU, dataProt.modePage.LBPMethod );
    ASSERT_EQ(0U, dataProt.modePage.LBPInformationLength );
    buff[17] |= 0xDF;
    ASSERT_EQ(0x1FU, dataProt.modePage.LBPInformationLength );
    ASSERT_EQ(0U, dataProt.modePage.LBP_W );
    buff[18] |= 0x8F;
    ASSERT_EQ(0x1U, dataProt.modePage.LBP_W );
    ASSERT_EQ(0U, dataProt.modePage.LBP_R );
    buff[18] |= 0x4F;
    ASSERT_EQ(0x1U, dataProt.modePage.LBP_R );
  }  
    
  TEST(castor_tape_SCSI_Structures, tapeAlertLogPage_t_and_parameters) {
    castor::tape::SCSI::Structures::tapeAlertLogPage_t<12> tal;
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
    ASSERT_EQ(0U, castor::tape::SCSI::Structures::toU16(tal.pageLength));
    buff[2] = 0x2; buff[3]= 0x67;
    ASSERT_EQ(0x267U, castor::tape::SCSI::Structures::toU16(tal.pageLength));
    /* The page length is counted in bytes. We are interested in the number of parameters*/
    ASSERT_EQ(123U, tal.parameterNumber());
    
    ASSERT_EQ(0U, castor::tape::SCSI::Structures::toU16(tal.parameters[0].parameterCode));
    buff [4] = 0xCA; buff[5] = 0xFE;
    ASSERT_EQ(0xCAFEU, castor::tape::SCSI::Structures::toU16(tal.parameters[0].parameterCode));
    
    ASSERT_EQ(0U, tal.parameters[11].flag);
    buff[3+12*5] |= (0x1) << 0;
    ASSERT_EQ(1U, tal.parameters[11].flag);
  }
  
  TEST(castor_tape_SCSI_Structures, senseBuffer_t) {
    castor::tape::SCSI::Structures::senseData_t<255> sense;
    unsigned char * buff = (unsigned char *) & sense;
    
    /* Check the total size of the structure, plus the one of each of the members
     of the union */
    ASSERT_EQ(255U, sizeof(sense));
    ASSERT_EQ(255U - 1U, sizeof(sense.descriptorFormat));
    ASSERT_EQ(255U - 1U, sizeof(sense.fixedFormat));
    
    buff[0] = 0x70;
    buff[2] = 0xFE;
    buff[12] = 0x12;
    buff[13] = 0x34;
    ASSERT_EQ(true, sense.isCurrent());
    ASSERT_FALSE(sense.isDeferred());
    ASSERT_EQ(true, sense.isFixedFormat());
    ASSERT_FALSE(sense.isDescriptorFormat());
    ASSERT_EQ(0x12, sense.getASC());
    ASSERT_EQ(0x34, sense.getASCQ());
    ASSERT_EQ(0xE, sense.getSenseKey());
    ASSERT_EQ("Miscompare", sense.getSenseKeyString());
    
    buff[0] = 0x71;
    buff[2] = 0xFA;
    buff[12] = 0x12;
    buff[13] = 0x34;
    ASSERT_FALSE(sense.isCurrent());
    ASSERT_EQ(true, sense.isDeferred());
    ASSERT_EQ(true, sense.isFixedFormat());
    ASSERT_FALSE(sense.isDescriptorFormat());
    ASSERT_EQ(0x12, sense.getASC());
    ASSERT_EQ(0x34, sense.getASCQ());
    ASSERT_EQ(0xA, sense.getSenseKey());
    ASSERT_EQ("Copy Aborted", sense.getSenseKeyString());
    
    buff[0] = 0x72;
    buff[1] = 0xFB;
    buff[2] = 0x56;
    buff[3] = 0x78;
    ASSERT_EQ(true, sense.isCurrent());
    ASSERT_FALSE(sense.isDeferred());
    ASSERT_FALSE(sense.isFixedFormat());
    ASSERT_EQ(true, sense.isDescriptorFormat());
    ASSERT_EQ(0x56, sense.getASC());
    ASSERT_EQ(0x78, sense.getASCQ());
    ASSERT_EQ(0xB, sense.getSenseKey());
    ASSERT_EQ("Aborted Command", sense.getSenseKeyString());
    
    buff[0] = 0x73;
    buff[1] = 0xFC;
    buff[2] = 0x0b;
    buff[3] = 0x08;
    ASSERT_FALSE(sense.isCurrent());
    ASSERT_EQ(true, sense.isDeferred());
    ASSERT_FALSE(sense.isFixedFormat());
    ASSERT_EQ(true, sense.isDescriptorFormat());
    ASSERT_EQ(0x0b, sense.getASC());
    ASSERT_EQ(0x08, sense.getASCQ());
    ASSERT_EQ(0xC, sense.getSenseKey());
    ASSERT_EQ("Warning - power loss expected", sense.getACSString());
    ASSERT_EQ("Equal", sense.getSenseKeyString());
    
    buff[2] = 0x40;
    buff[3] = 0xab;
    ASSERT_EQ("Diagnostic failure on component (ab)", sense.getACSString());
    
    buff[2] = 0x00;
    buff[3] = 0x1F;
    ASSERT_EQ("Unknown ASC/ASCQ:00/1f", sense.getACSString());
    
    buff[1] = 0xF;
    ASSERT_THROW(sense.getSenseKeyString(), cta::exception::Exception);
    
    buff[0] = 0x74;
    ASSERT_THROW(sense.getASC(), cta::exception::Exception);
    
    ASSERT_THROW(sense.getACSString(), cta::exception::Exception);
    
    try { sense.getACSString(); ASSERT_TRUE(false); }
    catch (cta::exception::Exception & ex) {
      std::string what(ex.getMessageValue());
      ASSERT_NE(std::string::npos, what.find("response code not supported (0x74)"));
    }
    
    buff[1] = 0xA;
    ASSERT_THROW(sense.getSenseKey(), cta::exception::Exception);
    ASSERT_THROW(sense.getSenseKeyString(), cta::exception::Exception);
    try { sense.getSenseKey(); ASSERT_TRUE(false); }
    catch (cta::exception::Exception & ex) {
      std::string what(ex.getMessageValue());
      ASSERT_NE(std::string::npos, what.find("response code not supported (0x74)"));
    }
    try { sense.getSenseKeyString(); ASSERT_TRUE(false); }
    catch (cta::exception::Exception & ex) {
      std::string what(ex.getMessageValue());
      ASSERT_NE(std::string::npos, what.find("response code not supported (0x74)"));
    }
  }
  
  TEST(castor_tape_SCSI_Structures, toU16) {
    unsigned char num[2] = { 0x1, 0x2 };
    ASSERT_EQ( 0x102, castor::tape::SCSI::Structures::toU16(num));
  }
  
  TEST(castor_tape_SCSI_Structures, toU32) {
    unsigned char num[4] = { 0x1, 0x2, 0x3, 0x4 };
    ASSERT_EQ( 0x1020304U, castor::tape::SCSI::Structures::toU32(num));
  }
  
  TEST(castor_tape_SCSI_Structures, toU32_3byte) {
    unsigned char num[3] = { 0xAA, 0xBB, 0xCC };
    ASSERT_EQ( 0x00AABBCCU, castor::tape::SCSI::Structures::toU32(num));
  }
  
  TEST(castor_tape_SCSI_Structures, toS32) {
    unsigned char num[4] = { 0xE6, 0x29, 0x66, 0x5B };
    ASSERT_EQ( -433494437, castor::tape::SCSI::Structures::toS32(num));
  }
  
  TEST(castor_tape_SCSI_Structures, toU64_6byte) {
    unsigned char num[6] = { 0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE };
    ASSERT_EQ ( 0xDEADBEEFCAFEULL, castor::tape::SCSI::Structures::toU64(num));
  }

  TEST(castor_tape_SCSI_Structures, toU64) {
    unsigned char num[8] = { 0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xFA, 0xDE };
    ASSERT_EQ ( 0xDEADBEEFCAFEFADEULL, castor::tape::SCSI::Structures::toU64(num));
  }
  
  TEST(castor_tape_SCSI_Structures, Exception) {
    castor::tape::SCSI::Structures::senseData_t<255> sense;
    castor::tape::SCSI::Structures::LinuxSGIO_t sgio;
    sgio.setSenseBuffer(&sense);
    sgio.status = castor::tape::SCSI::Status::GOOD;
    ASSERT_NO_THROW(castor::tape::SCSI::ExceptionLauncher(sgio));
    sgio.status = castor::tape::SCSI::Status::CHECK_CONDITION;
    /* fill up the ASC part of the */
    sense.responseCode = 0x70;
    sense.fixedFormat.ASC = 0x14;
    sense.fixedFormat.ASCQ = 0x04;
    ASSERT_THROW(castor::tape::SCSI::ExceptionLauncher(sgio), castor::tape::SCSI::Exception);
    try { castor::tape::SCSI::ExceptionLauncher(sgio, "In exception validation:"); ASSERT_TRUE(false); }
    catch (castor::tape::SCSI::Exception & ex) {
      std::string what(ex.getMessageValue());
      ASSERT_NE(std::string::npos, what.find("Block sequence error"));
      /* We check here that the formatting is also done correctly (space added when context
       not empty */
      ASSERT_NE(std::string::npos, what.find("In exception validation: "));
    }
  }
 
  TEST(castor_tape_SCSI_Structures, HostException) {
    castor::tape::SCSI::Structures::LinuxSGIO_t sgio;
    sgio.status = castor::tape::SCSI::Status::GOOD;
    sgio.host_status = castor::tape::SCSI::HostStatus::OK;
    ASSERT_NO_THROW(castor::tape::SCSI::ExceptionLauncher(sgio));

    sgio.host_status = castor::tape::SCSI::HostStatus::NO_CONNECT;
    ASSERT_THROW(castor::tape::SCSI::ExceptionLauncher(sgio),
      castor::tape::SCSI::HostException);
    sgio.host_status = castor::tape::SCSI::HostStatus::BUS_BUSY;
    ASSERT_THROW(castor::tape::SCSI::ExceptionLauncher(sgio),
      castor::tape::SCSI::HostException);
    sgio.host_status = castor::tape::SCSI::HostStatus::TIME_OUT;
    ASSERT_THROW(castor::tape::SCSI::ExceptionLauncher(sgio),
      castor::tape::SCSI::HostException);
    sgio.host_status = castor::tape::SCSI::HostStatus::BAD_TARGET;
    ASSERT_THROW(castor::tape::SCSI::ExceptionLauncher(sgio),
      castor::tape::SCSI::HostException);
    sgio.host_status = castor::tape::SCSI::HostStatus::ABORT;
    ASSERT_THROW(castor::tape::SCSI::ExceptionLauncher(sgio),
      castor::tape::SCSI::HostException);
    sgio.host_status = castor::tape::SCSI::HostStatus::PARITY;
    ASSERT_THROW(castor::tape::SCSI::ExceptionLauncher(sgio),
      castor::tape::SCSI::HostException);
    sgio.host_status = castor::tape::SCSI::HostStatus::ERROR;
    ASSERT_THROW(castor::tape::SCSI::ExceptionLauncher(sgio),
      castor::tape::SCSI::HostException);
    sgio.host_status = castor::tape::SCSI::HostStatus::RESET;
    ASSERT_THROW(castor::tape::SCSI::ExceptionLauncher(sgio),
      castor::tape::SCSI::HostException);
    sgio.host_status = castor::tape::SCSI::HostStatus::BAD_INTR;
    ASSERT_THROW(castor::tape::SCSI::ExceptionLauncher(sgio),
      castor::tape::SCSI::HostException);
    sgio.host_status = castor::tape::SCSI::HostStatus::PASSTHROUGH;
    ASSERT_THROW(castor::tape::SCSI::ExceptionLauncher(sgio),
      castor::tape::SCSI::HostException);
    sgio.host_status = castor::tape::SCSI::HostStatus::SOFT_ERROR;
    ASSERT_THROW(castor::tape::SCSI::ExceptionLauncher(sgio),
      castor::tape::SCSI::HostException);

    try {
      castor::tape::SCSI::ExceptionLauncher(sgio, "In exception validation:");
      ASSERT_TRUE(false);
    }
    catch (castor::tape::SCSI::HostException & ex) {
      std::string what(ex.getMessageValue());
      ASSERT_NE(std::string::npos, what.find("SOFT ERROR"));
      /* We check here that the formatting is also done correctly (space added
       *  when context not empty */
      ASSERT_NE(std::string::npos, what.find("In exception validation: "));
    }
  }

  TEST(castor_tape_SCSI_Structures, DriverException) {
    castor::tape::SCSI::Structures::LinuxSGIO_t sgio;
    sgio.status = castor::tape::SCSI::Status::GOOD;
    sgio.driver_status = castor::tape::SCSI::DriverStatus::OK;
    ASSERT_NO_THROW(castor::tape::SCSI::ExceptionLauncher(sgio));

    sgio.driver_status = castor::tape::SCSI::DriverStatus::BUSY;
    ASSERT_THROW(castor::tape::SCSI::ExceptionLauncher(sgio),
      castor::tape::SCSI::DriverException);
    sgio.driver_status = castor::tape::SCSI::DriverStatus::SOFT;
    ASSERT_THROW(castor::tape::SCSI::ExceptionLauncher(sgio),
      castor::tape::SCSI::DriverException);
    sgio.driver_status = castor::tape::SCSI::DriverStatus::MEDIA;
    ASSERT_THROW(castor::tape::SCSI::ExceptionLauncher(sgio),
      castor::tape::SCSI::DriverException);
    sgio.driver_status = castor::tape::SCSI::DriverStatus::ERROR;
    ASSERT_THROW(castor::tape::SCSI::ExceptionLauncher(sgio),
      castor::tape::SCSI::DriverException);
    sgio.driver_status = castor::tape::SCSI::DriverStatus::INVALID;
    ASSERT_THROW(castor::tape::SCSI::ExceptionLauncher(sgio),
      castor::tape::SCSI::DriverException);
    sgio.driver_status = castor::tape::SCSI::DriverStatus::TIMEOUT;
    ASSERT_THROW(castor::tape::SCSI::ExceptionLauncher(sgio),
      castor::tape::SCSI::DriverException);
    sgio.driver_status = castor::tape::SCSI::DriverStatus::HARD;
    ASSERT_THROW(castor::tape::SCSI::ExceptionLauncher(sgio),
      castor::tape::SCSI::DriverException);

    /* sense is a special case*/
    castor::tape::SCSI::Structures::senseData_t<255> sense;
    sgio.setSenseBuffer(&sense);
    /* fill up the ASC part of the */
    sense.responseCode = 0x70;
    sense.fixedFormat.ASC = 0x14;
    sense.fixedFormat.ASCQ = 0x04;
    sgio.driver_status = castor::tape::SCSI::DriverStatus::SENSE;
    ASSERT_THROW(castor::tape::SCSI::ExceptionLauncher(sgio),
      castor::tape::SCSI::DriverException);
    /* add suggestions*/
    sgio.driver_status |= castor::tape::SCSI::DriverStatusSuggest::RETRY;
    ASSERT_THROW(castor::tape::SCSI::ExceptionLauncher(sgio),
      castor::tape::SCSI::DriverException);
    sgio.driver_status |= castor::tape::SCSI::DriverStatusSuggest::ABORT;
    ASSERT_THROW(castor::tape::SCSI::ExceptionLauncher(sgio),
      castor::tape::SCSI::DriverException);
    sgio.driver_status |= castor::tape::SCSI::DriverStatusSuggest::REMAP;
    ASSERT_THROW(castor::tape::SCSI::ExceptionLauncher(sgio),
      castor::tape::SCSI::DriverException);
    sgio.driver_status |= castor::tape::SCSI::DriverStatusSuggest::DIE;
    ASSERT_THROW(castor::tape::SCSI::ExceptionLauncher(sgio),
      castor::tape::SCSI::DriverException);
    sgio.driver_status |= castor::tape::SCSI::DriverStatusSuggest::SENSE;
    ASSERT_THROW(castor::tape::SCSI::ExceptionLauncher(sgio),
      castor::tape::SCSI::DriverException);

    try {
      castor::tape::SCSI::ExceptionLauncher(sgio, "In exception validation:");
      ASSERT_TRUE(false);
    }
    catch (castor::tape::SCSI::DriverException & ex) {
      std::string what(ex.getMessageValue());
      ASSERT_NE(std::string::npos, what.find(": SENSE"));
      ASSERT_NE(std::string::npos, what.find("Driver suggestions:"));
      ASSERT_NE(std::string::npos, what.find(" RETRY ABORT REMAP DIE SENSE"));
      ASSERT_NE(std::string::npos, what.find("Block sequence error"));
      /* We check here that the formatting is also done correctly (space added
       *  when context not empty */
      ASSERT_NE(std::string::npos, what.find("In exception validation: "));
    }

    try {
      sgio.driver_status = castor::tape::SCSI::DriverStatus::TIMEOUT;
      castor::tape::SCSI::ExceptionLauncher(sgio, "In exception validation:");
      ASSERT_TRUE(false);
    }
    catch (castor::tape::SCSI::DriverException & ex) {
      std::string what(ex.getMessageValue());
      ASSERT_NE(std::string::npos, what.find(": TIMEOUT"));
      ASSERT_EQ(std::string::npos, what.find("Driver suggestions:"));
      ASSERT_EQ(std::string::npos, what.find(" RETRY ABORT REMAP DIE SENSE"));
      ASSERT_EQ(std::string::npos, what.find("Block sequence error"));
      /* We check here that the formatting is also done correctly (space added
       *  when context not empty */
      ASSERT_NE(std::string::npos, what.find("In exception validation: "));
    }
  }
 
  TEST(castor_tape_SCSI_Structures, logSenseParameter_t) {  
    unsigned char dataBuff[128];
    memset(dataBuff, random(), sizeof (dataBuff));
     
    castor::tape::SCSI::Structures::logSenseParameter_t &logParam=
            *(castor::tape::SCSI::Structures::logSenseParameter_t *) dataBuff;
          
    ASSERT_EQ(4U, sizeof(logParam.header));
    ASSERT_LE(sizeof(castor::tape::SCSI::Structures::logSenseParameterHeader_t), sizeof(logParam));
    
    unsigned char test1[] = {0x00, 0x08, 0x43, 0x04, 0x11, 0x22, 0x33, 0x44, 0x55};
    memcpy(dataBuff,test1,sizeof(test1));
    ASSERT_EQ(0x08U,castor::tape::SCSI::Structures::toU16(logParam.header.parameterCode));
    ASSERT_EQ(0x04U,logParam.header.parameterLength);
    ASSERT_EQ(0x11223344ULL,logParam.getU64Value());
    ASSERT_EQ(0x11223344LL,logParam.getS64Value());
    
    unsigned char test2[] = {0x00, 0x00, 0x43, 0x06, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77};
    memcpy(dataBuff,test2,sizeof(test2));
    ASSERT_EQ(0x00U,castor::tape::SCSI::Structures::toU16(logParam.header.parameterCode));
    ASSERT_EQ(0x06U,logParam.header.parameterLength);
    ASSERT_EQ(0x112233445566ULL,logParam.getU64Value());
    ASSERT_EQ(0x112233445566LL,logParam.getS64Value());
    
    unsigned char test3[] = {0x0A, 0x0F, 0x43, 0x08, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99};
    memcpy(dataBuff,test3,sizeof(test3));
    ASSERT_EQ(0xA0FU,castor::tape::SCSI::Structures::toU16(logParam.header.parameterCode));
    ASSERT_EQ(0x08U,logParam.header.parameterLength);
    ASSERT_EQ(0x1122334455667788ULL,logParam.getU64Value());
    ASSERT_EQ(0x1122334455667788LL,logParam.getS64Value());
    
    unsigned char test4[] = {0x0A, 0x0F, 0x43, 0x09, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99};
    memcpy(dataBuff,test4,sizeof(test4));
    ASSERT_EQ(0xA0FU,castor::tape::SCSI::Structures::toU16(logParam.header.parameterCode));
    ASSERT_EQ(0x09U,logParam.header.parameterLength);
    ASSERT_NE(0x1122334455667788ULL,logParam.getU64Value());
    ASSERT_NE(0x1122334455667788LL,logParam.getS64Value());
    
    unsigned char test5[] = {0xBB, 0xEE, 0x43, 0x00, 0x11, 0x22};
    memcpy(dataBuff,test5,sizeof(test5));
    ASSERT_EQ(0xBBEEU,castor::tape::SCSI::Structures::toU16(logParam.header.parameterCode));
    ASSERT_EQ(0x00U,logParam.header.parameterLength);
    ASSERT_EQ(0ULL,logParam.getU64Value());
    ASSERT_EQ(0LL,logParam.getS64Value());
    
    unsigned char test6[] = {0xDD, 0xCC, 0x43, 0x04, 0xFF, 0x22, 0x33, 0x44, 0x55};
    memcpy(dataBuff,test6,sizeof(test6));
    ASSERT_EQ(0xDDCCU,castor::tape::SCSI::Structures::toU16(logParam.header.parameterCode));
    ASSERT_EQ(0x04U,logParam.header.parameterLength);
    ASSERT_EQ(4280431428ULL,logParam.getU64Value());
    ASSERT_EQ(-14535868LL,logParam.getS64Value());
  }
  
  TEST(castor_tape_SCSI_Structures, testUnitReadyCDB_t) {
    castor::tape::SCSI::Structures::testUnitReadyCDB_t testUnitReadyCDB;
    unsigned char *buff = (unsigned char *)&testUnitReadyCDB;  
    /*
     * Make sure this struct is a POD (plain old data without virtual table)
     * (and has the right size).
     */
    ASSERT_EQ(6U, sizeof(testUnitReadyCDB)); 
    
    /* Check proper initialization an location of struct members match
     the bit/byte locations defined in SPC-4 */
    ASSERT_EQ(castor::tape::SCSI::Commands::TEST_UNIT_READY, testUnitReadyCDB.opCode);
    buff[0] = 0xAB;
    ASSERT_EQ(0xABU, testUnitReadyCDB.opCode);
    
    buff[1] = 0xFF;
    
    ASSERT_EQ(0, testUnitReadyCDB.EDCC);
    buff[2] |= (0x01 &   0xFF) << 0;
    ASSERT_EQ(0x1U, testUnitReadyCDB.EDCC);
        
    buff[3] = buff[4] = 0xFF;
    
    ASSERT_EQ(0U, testUnitReadyCDB.control);
    buff[5] |= 0xCD;
    ASSERT_EQ(0xCDU, testUnitReadyCDB.control);
  }  

  TEST(castor_tape_SCSI_Structures, readEndOfWrapPositionCDB_t) {
    castor::tape::SCSI::Structures::readEndOfWrapPositionCDB_t readEndOfWrapPositionCDB;
    unsigned char *buff = reinterpret_cast<unsigned char*>(&readEndOfWrapPositionCDB);

    // Make sure this struct is a POD (plain old data without virtual table) and has the right size
    ASSERT_EQ(12U, sizeof(readEndOfWrapPositionCDB));

    // Check proper initialization and location of struct members match the bit/byte locations defined in LTO-8 reference
    ASSERT_EQ(buff[0], 0xA3);
    ASSERT_EQ(buff[1], 0x1F);
    ASSERT_EQ(buff[2], 0x45);

    ASSERT_EQ(0U, readEndOfWrapPositionCDB.WNV);
    buff[3] = 0x01;
    ASSERT_EQ(1U, readEndOfWrapPositionCDB.WNV);
    ASSERT_EQ(0U, readEndOfWrapPositionCDB.RA);
    buff[3] = 0x02;
    ASSERT_EQ(1U, readEndOfWrapPositionCDB.RA);

    ASSERT_EQ(0U, readEndOfWrapPositionCDB.wrapNumber);
    buff[5] = 0xAB;
    ASSERT_EQ(0xAB, readEndOfWrapPositionCDB.wrapNumber);

    ASSERT_EQ(0U, castor::tape::SCSI::Structures::toU32(readEndOfWrapPositionCDB.allocationLength));
    buff[6] = 0x0A; buff[7] = 0xBC; buff[8] = 0xDE; buff[9] = 0xF0;
    ASSERT_EQ(0xABCDEF0, castor::tape::SCSI::Structures::toU32(readEndOfWrapPositionCDB.allocationLength));

    ASSERT_EQ(0U, readEndOfWrapPositionCDB.control);
    buff[11] = 0xBC;
    ASSERT_EQ(0xBC, readEndOfWrapPositionCDB.control);
  }

  TEST(castor_tape_SCSI_Structures, readEndOfWrapPositionDataShortForm_t) {
    castor::tape::SCSI::Structures::readEndOfWrapPositionDataShortForm_t readEndOfWrapPositionDataShortForm;
    unsigned char *buff = reinterpret_cast<unsigned char*>(&readEndOfWrapPositionDataShortForm);

    // Make sure this struct is a POD (plain old data without virtual table) and has the right size
    ASSERT_EQ(10U, sizeof(readEndOfWrapPositionDataShortForm));

    // Check proper initialization and location of struct members match the bit/byte locations defined in LTO-8 reference
    ASSERT_EQ(0U, castor::tape::SCSI::Structures::toU16(readEndOfWrapPositionDataShortForm.responseDataLength));
    buff[0] = 0x0A; buff[1] = 0xB0;
    ASSERT_EQ(0xAB0, castor::tape::SCSI::Structures::toU16(readEndOfWrapPositionDataShortForm.responseDataLength));

    // Reserved
    buff[2] = 0xFF; buff[3] = 0xFF;

    // In this record, the logical object identifier is 6 bytes (48 bits).
    ASSERT_EQ(0U, castor::tape::SCSI::Structures::toU64(readEndOfWrapPositionDataShortForm.logicalObjectIdentifier));
    buff[4] = 0xAB; buff[5] = 0xCD; buff[6] = 0xEF; buff[7] = 0x12; buff[8] = 0x34; buff[9] = 0x56;
    ASSERT_EQ(0xABCDEF123456, castor::tape::SCSI::Structures::toU64(readEndOfWrapPositionDataShortForm.logicalObjectIdentifier));
  }

  TEST(castor_tape_SCSI_Structures, readEndOfWrapPositionDataLongForm_t) {
    castor::tape::SCSI::Structures::readEndOfWrapPositionDataLongForm_t readEndOfWrapPositionDataLongForm;
    unsigned char *buff = reinterpret_cast<unsigned char*>(&readEndOfWrapPositionDataLongForm);

    // Make sure this struct is a POD (plain old data without virtual table) and has the right size
    ASSERT_EQ(4+(12*castor::tape::SCSI::maxLTOTapeWraps), sizeof(readEndOfWrapPositionDataLongForm));

    // Check proper initialization and location of struct members match the bit/byte locations defined in LTO-8 reference
    ASSERT_EQ(0U, castor::tape::SCSI::Structures::toU16(readEndOfWrapPositionDataLongForm.responseDataLength));
    //Assume we have maxLTOTapeWraps returned * 12 bytes = 3362 = 0x0D22
    buff[0] = 0x0D; buff[1] = 0x22;
    ASSERT_EQ(0x0D22, castor::tape::SCSI::Structures::toU16(readEndOfWrapPositionDataLongForm.responseDataLength));

    for(unsigned int wrap = 0; wrap < castor::tape::SCSI::maxLTOTapeWraps; ++wrap) {
        int offset = 4 + (wrap * 12);

        ASSERT_EQ(0U, castor::tape::SCSI::Structures::toU16(readEndOfWrapPositionDataLongForm.wrapDescriptor[wrap].wrapNumber));
        buff[offset + 0] = 0xAB;
        buff[offset + 1] = 0xCD;
        ASSERT_EQ(0xABCD, castor::tape::SCSI::Structures::toU16(readEndOfWrapPositionDataLongForm.wrapDescriptor[wrap].wrapNumber));

        ASSERT_EQ(0U, castor::tape::SCSI::Structures::toU16(readEndOfWrapPositionDataLongForm.wrapDescriptor[wrap].partition));
        buff[offset + 2] = 0xEF;
        buff[offset + 3] = 0x01;
        ASSERT_EQ(0xEF01, castor::tape::SCSI::Structures::toU16(readEndOfWrapPositionDataLongForm.wrapDescriptor[wrap].partition));

        // Reserved
        buff[offset + 4] = 0xFF; buff[offset + 5] = 0xFF;

        // In this record, the logical object identifier is 6 bytes (48 bits).
        ASSERT_EQ(0U, castor::tape::SCSI::Structures::toU64(readEndOfWrapPositionDataLongForm.wrapDescriptor[wrap].logicalObjectIdentifier));
        buff[offset + 6] = 0xAB; buff[offset + 7] = 0xCD; buff[offset + 8] = 0xEF;
        buff[offset + 9] = 0x12; buff[offset + 10] = 0x34; buff[offset + 11] = 0x56;
        ASSERT_EQ(0xABCDEF123456, castor::tape::SCSI::Structures::toU64(readEndOfWrapPositionDataLongForm.wrapDescriptor[wrap].logicalObjectIdentifier));
    }
    
    ASSERT_EQ(castor::tape::SCSI::maxLTOTapeWraps,readEndOfWrapPositionDataLongForm.getNbWrapsReturned());
  }

  TEST(castor_tape_SCSI_Structures, requestSenseCDB_t) {
    castor::tape::SCSI::Structures::requestSenseCDB_t requestSenseCDB;
    unsigned char *buff = reinterpret_cast<unsigned char*>(&requestSenseCDB);

    // Make sure this struct is a POD (plain old data without virtual table) and has the right size
    ASSERT_EQ(6U, sizeof(requestSenseCDB));

    // Check proper initialization and location of struct members match the bit/byte locations defined in LTO-8 reference
    buff[1] = 0xFF; buff[2] = 0xFF; buff[3] = 0xFF;

    ASSERT_EQ(castor::tape::SCSI::Commands::REQUEST_SENSE, requestSenseCDB.opCode);

    buff[0] = 0xAB;
    ASSERT_EQ(0xAB, requestSenseCDB.opCode);

    ASSERT_EQ(0, requestSenseCDB.allocationLength);
    buff[4] = 0x58;
    ASSERT_EQ(0x58, requestSenseCDB.allocationLength);

    ASSERT_EQ(0, requestSenseCDB.control);
    buff[5] = 0xBC;
    ASSERT_EQ(0xBC, requestSenseCDB.control);
  }

  TEST(castor_tape_SCSI_Structures, requestSenseData_t) {
    castor::tape::SCSI::Structures::requestSenseData_t requestSenseData;
    unsigned char *buff = reinterpret_cast<unsigned char*>(&requestSenseData);

    // Make sure this struct is a POD (plain old data without virtual table) and has the right size
    ASSERT_EQ(96U, sizeof(requestSenseData));

    // Check proper location of struct members match the bit/byte locations defined in LTO-8 reference
    buff[1] = 0xFF;
    buff[20] = 0xFF;

    ASSERT_EQ(0, requestSenseData.RESPONSE_CODE);
    buff[0] = 0x71;
    ASSERT_EQ(0x71, requestSenseData.RESPONSE_CODE);
    ASSERT_EQ(0, requestSenseData.VALID);
    buff[0] |= 1 << 7;
    ASSERT_EQ(0x1, requestSenseData.VALID);

    ASSERT_EQ(0, requestSenseData.SENSE_KEY);
    buff[2] = 0xA;
    ASSERT_EQ(0xA, requestSenseData.SENSE_KEY);
    ASSERT_EQ(0, requestSenseData.ILI);
    buff[2] |= 1 << 5;
    ASSERT_EQ(0x1, requestSenseData.ILI);
    ASSERT_EQ(0, requestSenseData.EOM);
    buff[2] |= 1 << 6;
    ASSERT_EQ(0x1, requestSenseData.EOM);
    ASSERT_EQ(0, requestSenseData.FILEMARK);
    buff[2] |= 1 << 7;
    ASSERT_EQ(0x1, requestSenseData.FILEMARK);

    ASSERT_EQ(0U, castor::tape::SCSI::Structures::toU32(requestSenseData.information));
    buff[3] = 0x0A; buff[4] = 0xBC; buff[5] = 0xDE; buff[6] = 0xF0;
    ASSERT_EQ(0x0ABCDEF0U, castor::tape::SCSI::Structures::toU32(requestSenseData.information));

    ASSERT_EQ(0, requestSenseData.additionalSenseLength);
    buff[7] = 0x58;
    ASSERT_EQ(0x58, requestSenseData.additionalSenseLength);

    ASSERT_EQ(0U, castor::tape::SCSI::Structures::toU32(requestSenseData.commandSpecificInformation));
    buff[8] = 0x12; buff[9] = 0x34; buff[10] = 0x56; buff[11] = 0x78;
    ASSERT_EQ(0x12345678, castor::tape::SCSI::Structures::toU32(requestSenseData.commandSpecificInformation));

    ASSERT_EQ(0, requestSenseData.additionalSenseCode);
    buff[12] = 0x1A;
    ASSERT_EQ(0x1A, requestSenseData.additionalSenseCode);

    ASSERT_EQ(0, requestSenseData.additionalSenseCodeQualifier);
    buff[13] = 0x2A;
    ASSERT_EQ(0x2A, requestSenseData.additionalSenseCodeQualifier);

    ASSERT_EQ(0, requestSenseData.fieldReplacableUnitCode);
    buff[14] = 0x3A;
    ASSERT_EQ(0x3A, requestSenseData.fieldReplacableUnitCode);

    ASSERT_EQ(0, requestSenseData.BIT_POINTER);
    buff[15] = 0x7;
    ASSERT_EQ(0x7, requestSenseData.BIT_POINTER);
    ASSERT_EQ(0, requestSenseData.BPV);
    buff[15] |= 1 << 3;
    ASSERT_EQ(0x1, requestSenseData.BPV);
    ASSERT_EQ(0, requestSenseData.C_D);
    buff[15] |= 1 << 6;
    ASSERT_EQ(0x1, requestSenseData.C_D);
    ASSERT_EQ(0, requestSenseData.SKSV_BIT);
    buff[15] |= 1 << 7;
    ASSERT_EQ(0x1, requestSenseData.SKSV_BIT);

    ASSERT_EQ(0U, castor::tape::SCSI::Structures::toU16(requestSenseData.SKSV));
    buff[16] = 0x45; buff[17] = 0x67;
    ASSERT_EQ(0x4567, castor::tape::SCSI::Structures::toU16(requestSenseData.SKSV));

    ASSERT_EQ(0U, castor::tape::SCSI::Structures::toU16(requestSenseData.reportingErrorFlagData));
    buff[18] = 0x89; buff[19] = 0x0A;
    ASSERT_EQ(0x890A, castor::tape::SCSI::Structures::toU16(requestSenseData.reportingErrorFlagData));

    ASSERT_EQ(0, requestSenseData.VOLVALID);
    buff[21] = 0x1;
    ASSERT_EQ(0x1, requestSenseData.VOLVALID);
    ASSERT_EQ(0, requestSenseData.DUMP);
    buff[21] |= 1 << 1;
    ASSERT_EQ(0x1, requestSenseData.DUMP);
    ASSERT_EQ(0, requestSenseData.CLN);
    buff[21] |= 1 << 3;
    ASSERT_EQ(0x1, requestSenseData.CLN);
    ASSERT_EQ(0, requestSenseData.DRVSRVC);
    buff[21] |= 1 << 4;
    ASSERT_EQ(0x1, requestSenseData.DRVSRVC);

    ASSERT_EQ("", castor::tape::SCSI::Structures::toString(requestSenseData.volumeLabel));
    buff[22] = 'V';
    buff[23] = 'O';
    buff[24] = 'L';
    buff[25] = 'U';
    buff[26] = 'M';
    buff[27] = 'E';
    buff[28] = '1';
    ASSERT_EQ("VOLUME1", castor::tape::SCSI::Structures::toString(requestSenseData.volumeLabel));

    ASSERT_EQ(0, requestSenseData.physicalWrap);
    buff[29] = 0x4A;
    ASSERT_EQ(0x4A, requestSenseData.physicalWrap);

    ASSERT_EQ(0U, castor::tape::SCSI::Structures::toU32(requestSenseData.relativeLPOSValue));
    buff[30] = 0x5A; buff[31] = 0x6B; buff[32] = 0x7C; buff[33] = 0x8D;
    ASSERT_EQ(0x5A6B7C8D, castor::tape::SCSI::Structures::toU32(requestSenseData.relativeLPOSValue));

    ASSERT_EQ(0, requestSenseData.SCSIAddress);
    buff[34] = 0x6A;
    ASSERT_EQ(0x6A, requestSenseData.SCSIAddress);

    ASSERT_EQ(0, requestSenseData.RS422Information);
    buff[35] = 0x7A;
    ASSERT_EQ(0x7A, requestSenseData.RS422Information);

    ASSERT_EQ(0, requestSenseData.activePartition);
    buff[36] = 0x7;
    ASSERT_EQ(0x7, requestSenseData.activePartition);

    ASSERT_EQ(0U, castor::tape::SCSI::Structures::toU32(requestSenseData.portIdentifier));
    buff[37] = 0xF3; buff[38] = 0x2A; buff[39] = 0x94;
    ASSERT_EQ(0xF32A94, castor::tape::SCSI::Structures::toU32(requestSenseData.portIdentifier));

    ASSERT_EQ(0, requestSenseData.relativeTgtPort);
    buff[40] = 0x7;
    ASSERT_EQ(0x7, requestSenseData.relativeTgtPort);
    ASSERT_EQ(0, requestSenseData.tapePartitionsExist);
    buff[40] |= 1 << 6;
    ASSERT_EQ(0x1, requestSenseData.tapePartitionsExist);
    ASSERT_EQ(0, requestSenseData.tapeDirectoryValid);
    buff[40] |= 1 << 7;
    ASSERT_EQ(0x1, requestSenseData.tapeDirectoryValid);

    ASSERT_EQ(0, requestSenseData.hostCommand);
    buff[41] = 0x8A;
    ASSERT_EQ(0x8A, requestSenseData.hostCommand);

    ASSERT_EQ(0, requestSenseData.mediaType);
    buff[42] = 0xF;
    ASSERT_EQ(0xF, requestSenseData.mediaType);
    ASSERT_EQ(0, requestSenseData.cartridgeGenType);
    buff[42] |= 0xC << 4;
    ASSERT_EQ(0xC, requestSenseData.cartridgeGenType);

    ASSERT_EQ(0U, castor::tape::SCSI::Structures::toU16(requestSenseData.volumeLabelCartridgeType));
    buff[43] = 0x8A; buff[44] = 0x9B;
    ASSERT_EQ(0x8A9B, castor::tape::SCSI::Structures::toU16(requestSenseData.volumeLabelCartridgeType));

    ASSERT_EQ(0U, castor::tape::SCSI::Structures::toU32(requestSenseData.logicalBlockNumber));
    buff[45] = 0x1B; buff[46] = 0x2C; buff[47] = 0x3D; buff[48] = 0x4E;
    ASSERT_EQ(0x1B2C3D4E, castor::tape::SCSI::Structures::toU32(requestSenseData.logicalBlockNumber));

    ASSERT_EQ(0U, castor::tape::SCSI::Structures::toU32(requestSenseData.datasetNumber));
    buff[49] = 0x5F; buff[50] = 0x6A; buff[51] = 0x7B; buff[52] = 0x8C;
    ASSERT_EQ(0x5F6A7B8C, castor::tape::SCSI::Structures::toU32(requestSenseData.datasetNumber));

    ASSERT_EQ(0U, castor::tape::SCSI::Structures::toU16(requestSenseData.firstErrorFSC));
    buff[53] = 0x9A; buff[54] = 0x9B;
    ASSERT_EQ(0x9A9B, castor::tape::SCSI::Structures::toU16(requestSenseData.firstErrorFSC));

    ASSERT_EQ(0U, castor::tape::SCSI::Structures::toU16(requestSenseData.firstErrorFlagData));
    buff[55] = 0x9C; buff[56] = 0x9D;
    ASSERT_EQ(0x9C9D, castor::tape::SCSI::Structures::toU16(requestSenseData.firstErrorFlagData));

    ASSERT_EQ(0U, castor::tape::SCSI::Structures::toU16(requestSenseData.secondErrorFSC));
    buff[57] = 0x9E; buff[58] = 0x9F;
    ASSERT_EQ(0x9E9F, castor::tape::SCSI::Structures::toU16(requestSenseData.secondErrorFSC));

    ASSERT_EQ(0U, castor::tape::SCSI::Structures::toU16(requestSenseData.secondErrorFlagData));
    buff[59] = 0x5A; buff[60] = 0x5B;
    ASSERT_EQ(0x5A5B, castor::tape::SCSI::Structures::toU16(requestSenseData.secondErrorFlagData));

    ASSERT_EQ(0U, castor::tape::SCSI::Structures::toU16(requestSenseData.nextToLastErrorFSC));
    buff[61] = 0x5C; buff[62] = 0x5D;
    ASSERT_EQ(0x5C5D, castor::tape::SCSI::Structures::toU16(requestSenseData.nextToLastErrorFSC));

    ASSERT_EQ(0U, castor::tape::SCSI::Structures::toU16(requestSenseData.nextToLastErrorFlagData));
    buff[63] = 0x5E; buff[64] = 0x5F;
    ASSERT_EQ(0x5E5F, castor::tape::SCSI::Structures::toU16(requestSenseData.nextToLastErrorFlagData));

    ASSERT_EQ(0U, castor::tape::SCSI::Structures::toU16(requestSenseData.lastErrorFSC));
    buff[65] = 0x6A; buff[66] = 0x6B;
    ASSERT_EQ(0x6A6B, castor::tape::SCSI::Structures::toU16(requestSenseData.lastErrorFSC));

    ASSERT_EQ(0U, castor::tape::SCSI::Structures::toU16(requestSenseData.lastErrorFlagData));
    buff[67] = 0x6C; buff[68] = 0x6D;
    ASSERT_EQ(0x6C6D, castor::tape::SCSI::Structures::toU16(requestSenseData.lastErrorFlagData));

    ASSERT_EQ(0, requestSenseData.LPOSRegion);
    buff[69] = 0x6E;
    ASSERT_EQ(0x6E, requestSenseData.LPOSRegion);

    ASSERT_EQ("", castor::tape::SCSI::Structures::toString(requestSenseData.ERPSummaryInformation));
    buff[70] = 'E';
    buff[71] = 'R';
    buff[72] = 'P';
    buff[73] = 'S';
    buff[74] = 'U';
    buff[75] = 'M';
    buff[76] = 'M';
    buff[77] = 'A';
    buff[78] = 'R';
    buff[79] = 'Y';
    buff[80] = 'I';
    buff[81] = 'N';
    buff[82] = 'F';
    buff[83] = 'O';
    buff[84] = 'R';
    buff[85] = 'M';
    ASSERT_EQ("ERPSUMMARYINFORM", castor::tape::SCSI::Structures::toString(requestSenseData.ERPSummaryInformation));

    ASSERT_EQ("", castor::tape::SCSI::Structures::toString(requestSenseData.cartridgeSerialNumber));
    buff[86] = 'C';
    buff[87] = 'A';
    buff[88] = 'R';
    buff[89] = 'T';
    buff[90] = 'R';
    buff[91] = 'I';
    buff[92] = 'D';
    buff[93] = 'G';
    buff[94] = 'E';
    buff[95] = '1';
    ASSERT_EQ("CARTRIDGE1", castor::tape::SCSI::Structures::toString(requestSenseData.cartridgeSerialNumber));
  }
}
