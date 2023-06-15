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

#pragma once

#include <sstream>
#include <iomanip>
#include <algorithm>
#include <arpa/inet.h>
#include <endian.h>
#include <string.h>
#include <scsi/sg.h>
#include <climits>
#include <cstdio>

#include "Constants.hpp"
#include "common/exception/Exception.hpp"

namespace castor {
namespace tape {
namespace SCSI {
const unsigned int defaultTimeout = 900000;  //millisecs

/**
   * Maximum number of tape wraps.
   *
   * This is used to determine the maximum size of the response from the REOWP command,
   * which returns 12 bytes per wrap, plus a 4-byte header.
   *
   * LTO-8 has 208 physical wraps. LTO-9 has 280 wraps. This number should be adjusted
   * upwards when the LTO-10 specification is announced.
   */
const unsigned int maxLTOTapeWraps = 280;

/**
   * Structures as defined in the SCSI specifications, and helper functions for them.
   * SPC-4 (SCSI primary commands) can be found at:
   * http://hackipedia.org/Hardware/SCSI/Primary%20Commands/SCSI%20Primary%20Commands%20-%204.pdf
   * 
   * and SSC-3 (SCSI stream commands, i.e. tape drives) at:
   * http://hackipedia.org/Hardware/SCSI/Stream%20Commands/SCSI%20Stream%20Commands%20-%203.pdf
   */
namespace Structures {

/**
     * Helper template to zero a structure. Small boilerplate reduction.
     * Should not be used with classes with virtual tables! With a zeroed
     * out virtual table pointer, this will be detected soon enough.
     * @param s pointer the struct/class.
     */
// Because memset is problematic, since gcc8 it throws an error in compiling time.
// To transforim those errors in warnings
// #pragma GCC diagnostic warning "-Wclass-memaccess"
// To remove the warnings during compilation process
#pragma GCC diagnostic ignored "-Wclass-memaccess"

template<typename C>
void zeroStruct(C* s) {
  memset(s, 0, sizeof(C));
}

/**
     * Class wrapping around Linux' SG_IO struct, providing
     * zeroing and automatic filling up for the mandatory structures
     * (cdb, databuffer, sense buffer, magic 'S', and default timeout).
     * Make it look like a bare sg_io_hdr_t when using & operator.
     * Another little boilerplate killer.
     */
class LinuxSGIO_t : public sg_io_hdr_t {
public:
  LinuxSGIO_t() {
    zeroStruct(this);
    interface_id = 'S';
    timeout = defaultTimeout;
  }

  template<typename T>
  void setCDB(T* cdb) {
    cmdp = (unsigned char*) cdb;
    cmd_len = sizeof(T);
  }

  template<typename T>
  void setSenseBuffer(T* senseBuff) {
    if (sizeof(T) > UCHAR_MAX) {
      throw cta::exception::Exception("sense structure too big in LinuxSGIO_t::setSense");
    }
    mx_sb_len = (unsigned char) sizeof(T);
    sbp = (unsigned char*) senseBuff;
  }

  template<typename T>
  void setDataBuffer(T* dataBuff) {
    dxferp = dataBuff;
    dxfer_len = sizeof(T);
  }

  // If dataBuff is a pointer to a variable length array, sizeof will return
  // the size of one element. This function allows to manually set the buffer size
  template<typename T>
  void setDataBuffer(T* dataBuff, unsigned int dataBuffSize) {
    dxferp = dataBuff;
    dxfer_len = dataBuffSize;
  }

  sg_io_hdr_t* operator&() { return (sg_io_hdr_t*) this; }
};

/**
     * Helper function to deal with endianness.
     * @param t byte array in SCSI order representing a 64 bits number
     * @return 
     */
inline uint64_t toU64(const unsigned char (&t)[8]) {
  /* Like network, SCSI is BigEndian */
  return (uint64_t) ntohl((*(uint64_t*) t << 32) >> 32) << 32 | ntohl(*(uint64_t*) t >> 32);
}

/**
     * Helper function to deal with endianness: 6-byte version
     *
     * The REOWP SCSI command assigns 48 bits to store the LOGICAL OBJECT IDENTIFIER (although other
     * commands use 64 bits or sometimes 32 bits). This function converts the 48-bit byte array
     * into a 64-bit unsigned integer.
     *
     * @param t byte array in SCSI order representing a 48-bit number
     * @return 64-bit unsigned integer
     */
inline uint64_t toU64(const unsigned char (&t)[6]) {
  /* Like network, SCSI is BigEndian */
  return (uint64_t) ntohl((*(uint64_t*) t << 32) >> 16) << 32 | ntohl(*(uint64_t*) t >> 16);
}

/**
     * Helper function to deal with endianness.
     * @param t byte array in SCSI order representing a 32 bits number
     * @return 
     */
inline uint32_t toU32(const unsigned char (&t)[4]) {
  /* Like network, SCSI is BigEndian */
  return ntohl(*((uint32_t*) t));
}

/**
     * Helper function to deal with endianness.
     * for 3 bytes! fields in SCSI replies 
     * @param t byte array in SCSI order representing a 32 bits number
     * @return 
     */
inline uint32_t toU32(const unsigned char (&t)[3]) {
  union {
    unsigned char tmp[4];
    uint32_t val;
  } u;

  u.tmp[0] = 0;
  u.tmp[1] = t[0];
  u.tmp[2] = t[1];
  u.tmp[3] = t[2];

  /* Like network, SCSI is BigEndian */
  return ntohl(u.val);
}

/**
     * Helper function to deal with endianness.
     * for signed values
     * @param t byte array in SCSI order representing a 32 bits number
     * @return 
     */
inline int32_t toS32(const unsigned char (&t)[4]) {
  /* Like network, SCSI is BigEndian */
  return (int32_t) (ntohl(*((uint32_t*) t)));
}

/**
     * Helper function to deal with endianness.
     * @param t byte array in SCSI order representing a 16 bits number
     * @return 
     */
inline uint16_t toU16(const unsigned char (&t)[2]) {
  /* Like network, SCSI is BigEndian */
  return ntohs(*((uint16_t*) t));
}

/**
     * Helper function setting in place a 32 bits SCSI number from a value
     * expressed in the local endianness.
     * @param t pointer to the char array at the 32 bits value position.
     * @param val the value.
     */
inline void setU32(unsigned char (&t)[4], uint32_t val) {
  *((uint32_t*) t) = htonl(val);
}

/**
     * Helper function setting in place a 16 bits SCSI number from a value
     * expressed in the local endianness.
     * @param t pointer to the char array at the 16 bits value position.
     * @param val the value.
     */
inline void setU16(unsigned char (&t)[2], uint16_t val) {
  *((uint16_t*) t) = htons(val);
}

/**
     * Helper function setting in place a 64 bits SCSI number from a value
     * expressed in the local endianness.
     * @param t pointer to the char array at the 64 bits value position.
     * @param val the value.
     */
inline void setU64(unsigned char (&t)[8], uint64_t val) {
  *((uint64_t*) t) = htobe64(val);
}

/**
     * Inquiry CDB as described in SPC-4.
     */
class inquiryCDB_t {
public:
  inquiryCDB_t() {
    zeroStruct(this);
    opCode = SCSI::Commands::INQUIRY;
  }

  unsigned char opCode;

  unsigned char EVPD : 1;
  unsigned char      : 7;

  unsigned char pageCode;

  unsigned char allocationLength[2];

  unsigned char control;
};

/**
     * Inquiry data as described in SPC-4.
     */
class inquiryData_t {
public:
  inquiryData_t() { zeroStruct(this); }

  unsigned char perifDevType   : 5;
  unsigned char perifQualifyer : 3;

  unsigned char     : 7;
  unsigned char RMB : 1;

  unsigned char version : 8;

  unsigned char respDataFmt : 4;
  unsigned char HiSup       : 1;
  unsigned char normACA     : 1;
  unsigned char             : 2;

  unsigned char addLength : 8;

  unsigned char protect : 1;
  unsigned char         : 2;
  unsigned char threePC : 1;
  unsigned char TPGS    : 2;
  unsigned char ACC     : 1;
  unsigned char SCCS    : 1;

  unsigned char addr16  : 1;
  unsigned char         : 3;
  unsigned char multiP  : 1;
  unsigned char VS1     : 1;
  unsigned char encServ : 1;
  unsigned char         : 1;

  unsigned char VS2    : 1;
  unsigned char cmdQue : 1;
  unsigned char        : 2;
  unsigned char sync   : 1;
  unsigned char wbus16 : 1;
  unsigned char        : 2;

  char T10Vendor[8];
  char prodId[16];
  char prodRevLvl[4];
  char vendorSpecific1[20];

  unsigned char IUS      : 1;
  unsigned char QAS      : 1;
  unsigned char clocking : 2;
  unsigned char          : 4;

  unsigned char reserved1;

  unsigned char versionDescriptor[8][2];

  unsigned char reserved2[22];
  unsigned char vendorSpecific2[1];
};

/**
    * Oracle T10K Inquiry data
    */
class inquiryDataT10k_t {
public:
  inquiryDataT10k_t() { zeroStruct(this); }

  unsigned char perifDevType   : 5;
  unsigned char perifQualifyer : 3;

  unsigned char     : 7;
  unsigned char RMB : 1;

  unsigned char version;

  unsigned char respDataFmt : 4;
  unsigned char HiSup       : 1;
  unsigned char normACA     : 1;
  unsigned char RSVD1       : 1;
  unsigned char AERC        : 1;

  unsigned char addLength;

  unsigned char protect : 1;
  unsigned char         : 2;
  unsigned char threePC : 1;
  unsigned char TPGS    : 2;
  unsigned char ACC     : 1;
  unsigned char SCCS    : 1;

  unsigned char         : 3;
  unsigned char mChngr  : 1;
  unsigned char multiP  : 1;
  unsigned char VS1     : 1;
  unsigned char encServ : 1;
  unsigned char bQue    : 1;

  unsigned char VS2    : 1;
  unsigned char cmdQue : 1;
  unsigned char RSVD2  : 1;
  unsigned char linked : 1;
  unsigned char        : 3;
  unsigned char relAdr : 1;

  char vendorId[8];
  char prodId[16];
  char prodRevLvl[8];
  char vendorSpecific1[14];

  unsigned char keyMgmt;

  unsigned char CSL     : 1;
  unsigned char DCMP    : 1;
  unsigned char volSafe : 1;
  unsigned char libAtt  : 1;
  unsigned char encr    : 1;
  unsigned char         : 3;

  unsigned char reserved1[2];

  unsigned char versionDescriptor[8][2];
};

/**
     * Inquiry unit serial number vital product data as described in SPC-4.
     */
class inquiryUnitSerialNumberData_t {
public:
  inquiryUnitSerialNumberData_t() { zeroStruct(this); }

  // byte 0
  unsigned char peripheralDeviceType : 5;  // (000b) connected to this LUN
  unsigned char peripheralQualifier  : 3;  // (01h) tape drive

  // byte 1
  unsigned char pageCode;  // (80h) Vital Product Data page for serial

  // byte 2
  unsigned char : 8;  // Reserved

  // byte 3
  unsigned char pageLength;  // n-3

  // bytes 4-n
  char productSerialNumber[12];  // 12 bytes for T10000&IBM, 10 for LTO
};

/**
     * LOCATE(10) CDB as described in SSC-3.
     */
class locate10CDB_t {
public:
  locate10CDB_t() {
    zeroStruct(this);
    opCode = SCSI::Commands::LOCATE_10;
  }

  // byte 0
  unsigned char opCode;  // OPERATION CODE (2Bh)

  // byte 1
  unsigned char IMMED : 1;  // Immediate
  unsigned char CP    : 1;  // Change Partition
  unsigned char BT    : 1;  // Block address Type
  unsigned char       : 5;  // Reserved

  // byte 2
  unsigned char : 8;  // Reserved

  // bytes 3-6
  unsigned char logicalObjectID[4];  // Logical object identifier or block address

  // byte 7
  unsigned char : 8;  // Reserved

  // byte 8
  unsigned char partition;  // Partition

  // byte 9
  unsigned char control;  // Control byte
};

/**
     * READ POSITION CDB as described in SSC-3.
     */
class readPositionCDB_t {
public:
  readPositionCDB_t() {
    zeroStruct(this);
    opCode = SCSI::Commands::READ_POSITION;
  }

  // byte 0
  unsigned char opCode;  // OPERATION CODE (34h)

  // byte 1
  // *note* for T10000 we have BT:1, LONG:1, TCLP:1, Reserved:5
  unsigned char serviceAction : 5;  // Service action to choice FORM
  unsigned char               : 3;  // Reserved

  // bytes 2-6
  unsigned char reserved[5];  // Reserved

  // bytes 7-8
  unsigned char allocationLength[2];  // used for EXTENDENT FORM

  // byte 9
  unsigned char control;  // Control byte
};

/**
     * READ POSITION  data format, short form as described in SSC-3.
     */
class readPositionDataShortForm_t {
public:
  readPositionDataShortForm_t() { zeroStruct(this); }

  // byte 0
  unsigned char BPEW : 1;  // Beyond Programmable Early Warning
  unsigned char PERR : 1;  // Position ERroR
  unsigned char LOLU : 1;  // Logical Object Location Unknown or Block Position Unknown(BPU) for T10000
  unsigned char      : 1;  // Reserved
  unsigned char BYCU : 1;  // BYte Count Unknown
  unsigned char LOCU : 1;  // Logical Object Count Unknown or Block Count Unknown(BCU) for T10000
  unsigned char EOP  : 1;  // End Of Partition
  unsigned char BOP  : 1;  // Beginning of Partition

  // byte 1
  unsigned char partitionNumber;  // Service action to choice FORM

  // bytes 2-3
  unsigned char reserved[2];  // Reserved

  // bytes 4-7
  unsigned char firstBlockLocation[4];  // First Logical object location in SSC3,IBM,LTO

  // bytes 8-11
  unsigned char lastBlockLocation[4];  // Last Logical object location in SSC3,IBM,LTO

  // byte 12
  unsigned char : 8;  // Reserved

  // bytes 13-15
  unsigned char blocksInBuffer[3];  // Number of logical objects in object buffer

  // bytes 16-19
  unsigned char bytesInBuffer[4];  // Number if bytes in object buffer
};

/**
     * LOG SELECT CDB as described in SPC-4.
     */
class logSelectCDB_t {
public:
  logSelectCDB_t() {
    zeroStruct(this);
    opCode = SCSI::Commands::LOG_SELECT;
  }

  // byte 0
  unsigned char opCode;  // OPERATION CODE (4Ch)

  // byte 1
  unsigned char SP  : 1;  // the Save Parameters
  unsigned char PCR : 1;  // the Parameter Code Reset
  unsigned char     : 6;  // Reserved

  // byte 2
  unsigned char pageCode : 6;  // PAGE CODE
  unsigned char PC       : 2;  // the Page Control

  // byte 3
  unsigned char subPageCode;  // SUBPAGE CODE (Reserved for T10000)

  // bytes 4-6
  unsigned char reserved[3];  // Reserved

  // bytes 7-8
  unsigned char parameterListLength[2];  // PARAMETER LIST LENGTH

  // byte 9
  unsigned char control;  // CONTROL
};

/**
     * Log sense CDB as described in SPC-4, 
     */
class logSenseCDB_t {
public:
  logSenseCDB_t() {
    zeroStruct(this);
    opCode = SCSI::Commands::LOG_SENSE;
  }

  unsigned char opCode;

  unsigned char SP  : 1;
  unsigned char PPC : 1;
  unsigned char     : 6;

  unsigned char pageCode : 6;
  unsigned char PC       : 2;

  unsigned char subPageCode;

  unsigned char reserved;

  unsigned char parameterPointer[2];

  unsigned char allocationLength[2];

  unsigned char control;
};

/**
     * Log sense Log Page Parameter Format as described in SPC-4, 
     */
class logSenseParameterHeader_t {
public:
  // bytes 0-1
  unsigned char parameterCode[2];

  // byte 2
  unsigned char formatAndLinking : 2;  // reserved and List Parameter bits
  unsigned char TMC              : 2;  // Threshold Met Criteria
  unsigned char ETC              : 1;  // Enable Threshold Comparison
  unsigned char TSD              : 1;  // Target Save Disable
  unsigned char                  : 1;  // DS Disable Save for T10000
  unsigned char DU               : 1;  // Disable Update

  // byte 3
  unsigned char parameterLength;  // n-3
};

class logSenseParameter_t {
public:
  // bytes 0-3
  logSenseParameterHeader_t header;

  // bytes 4-n
  unsigned char parameterValue[8];  // parameters have variable length

  /**
       * Gets the parameter value
       * 
       * @return The value  of the log sense parameter as uint64_t.
       *         If we have a parameter length more than 8 bytes the returning
       *         value is not determined. 
       */
  inline uint64_t getU64Value() {
    union {
      unsigned char tmp[8];
      uint64_t val64;
    } u;

    u.tmp[0] = (header.parameterLength > 0) ? parameterValue[0] : 0;
    u.tmp[1] = (header.parameterLength > 1) ? parameterValue[1] : 0;
    u.tmp[2] = (header.parameterLength > 2) ? parameterValue[2] : 0;
    u.tmp[3] = (header.parameterLength > 3) ? parameterValue[3] : 0;
    u.tmp[4] = (header.parameterLength > 4) ? parameterValue[4] : 0;
    u.tmp[5] = (header.parameterLength > 5) ? parameterValue[5] : 0;
    u.tmp[6] = (header.parameterLength > 6) ? parameterValue[6] : 0;
    u.tmp[7] = (header.parameterLength > 7) ? parameterValue[7] : 0;

    u.val64 = be64toh(u.val64);

    return u.val64 >> (64 - (header.parameterLength << 3));
  }

  /**
       * Gets the parameter value.
       * 
       * @return The value  of the log sense parameter as int64_t.
       *         If we have a parameter length more than 8 bytes the returning
       *         value is not determined. 
       */
  inline int64_t getS64Value() {
    union {
      unsigned char tmp[8];
      uint64_t val64U;
      int64_t val64S;
    } u;

    u.tmp[0] = (header.parameterLength > 0) ? parameterValue[0] : 0;
    u.tmp[1] = (header.parameterLength > 1) ? parameterValue[1] : 0;
    u.tmp[2] = (header.parameterLength > 2) ? parameterValue[2] : 0;
    u.tmp[3] = (header.parameterLength > 3) ? parameterValue[3] : 0;
    u.tmp[4] = (header.parameterLength > 4) ? parameterValue[4] : 0;
    u.tmp[5] = (header.parameterLength > 5) ? parameterValue[5] : 0;
    u.tmp[6] = (header.parameterLength > 6) ? parameterValue[6] : 0;
    u.tmp[7] = (header.parameterLength > 7) ? parameterValue[7] : 0;

    u.val64U = be64toh(u.val64U);

    return (u.val64S < 0 ? -(-u.val64S >> (64 - (header.parameterLength << 3))) :
                           (u.val64S >> (64 - (header.parameterLength << 3))));
  }
};

/**
     * Log sense Log Page Format as described in SPC-4, 
     */
class logSenseLogPageHeader_t {
public:
  // byte 0
  unsigned char pageCode : 6;
  unsigned char SPF      : 1;  // the Subpage format
  unsigned char DS       : 1;  // the Disable Slave bit

  // byte 1
  unsigned char subPageCode;

  // bytes 2-3
  unsigned char pageLength[2];  // n-3 number of bytes without header
};

/**
     * Log sense Log Page Format as described in SPC-4, 
     */
class logSenseLogPage_t {
public:
  // bytes 0-3
  logSenseLogPageHeader_t header;

  // bytes 4-n
  logSenseParameter_t parameters[1];  // parameters have variable length
};

/**
     * MODE SENSE(6) CDB as described in SPC-4.
     */
class modeSense6CDB_t {
public:
  modeSense6CDB_t() {
    zeroStruct(this);
    opCode = SCSI::Commands::MODE_SENSE_6;
  }

  // byte 0
  unsigned char opCode;  // OPERATION CODE (1Ah)

  // byte 1
  unsigned char     : 3;  // Reserved
  unsigned char DBD : 1;  // Disable Block Descriptors
  unsigned char     : 4;  // Reserved

  // byte 2
  unsigned char pageCode : 6;  // Page code
  unsigned char PC       : 2;  // Page Control

  // byte3
  unsigned char subPageCode;  // Subpage code

  // byte4
  unsigned char allocationLength;  // The maximum number of bytes to be transferred

  // byte 5
  unsigned char control;  // Control byte
};

/**
     * MODE SENSE(6) MODE SELECT(6) parameter header as described in SPC-4.
     */
class modeParameterHeader6_t {
public:
  // byte 0
  unsigned char modeDataLength;  // The mode data length does not include itself

  // byte 1
  unsigned char mediumType;  // The medium type in the drive

  // byte 2
  /* in SPC-4 we have device-specific parameter byte here
       * but from all drive specifications the fields are the same
       * so we use them here.
       */
  unsigned char speed        : 4;  // Read/write speed
  unsigned char bufferedMode : 3;  // Returns after data is in the buffer or on the medium
  unsigned char WP           : 1;  // Write Protect

  // byte3
  unsigned char blockDescriptorLength;  //  (08h) or (00h)
};

/**
     * MODE SENSE(6,10) and MODE SELECT(6,10) block descriptor as described in SPC-4.
     */
class modeParameterBlockDecriptor_t {
public:
  // byte 0
  unsigned char densityCode;  // Density code

  // bytes 1-3
  unsigned char numberOfBlocks[3];  // Number of block or block count

  // byte 4
  unsigned char : 8;  // Reserved

  // bytes 5-7
  unsigned char blockLength[3];  //  Block length
};

/**
     * MODE SENSE(6) or MODE SENSE(10) mode page 10h: Device Configuration.
     * There is no description in SPC-4 or SSC-3.
     * We use descriptions from: 
     * IBM System Storage Tape Drive 3592 SCSI Reference,
     * Sun StorageTekTM T10000 Tape Drive Fibre Channel Interface Reference Manual,
     * IBM TotalStorage LTO Ultrium Tape Drive SCSI Reference.
    */
class modePageDeviceConfiguration_t {
public:
  // byte 0
  unsigned char pageCode : 6;  // Page code (10h)
  unsigned char SPF      : 1;  // SubPage Format (0b)
  unsigned char PS       : 1;  // Parameters Savable

  // byte 1
  unsigned char pageLength;  // (0Eh)

  // byte 2
  unsigned char activeFormat : 5;  // Active Format
  unsigned char CAF          : 1;  // Change Active Format
  unsigned char CAP          : 1;  // Change Active Partition
  unsigned char              : 1;  // Reserved

  // byte 3
  unsigned char activePartition;  //  Active Partition

  // byte 4
  unsigned char writeBufferFullRatio;  // Write object buffer full ratio

  // byte 5
  unsigned char readBufferEmptyRatio;  // Read object buffer empty ratio

  // bytes 6-7
  unsigned char writeDelayTime[2];  // Write delay time in 100ms for IBM, LTO and in sec for T1000

  // byte 8
  unsigned char REW  : 1;  // Report Early Warning
  unsigned char RBO  : 1;  // Recover Buffer Order
  unsigned char SOCF : 2;  // Stop On Consecutive Filemarks
  unsigned char AVC  : 1;  // Automatic Velocity Control
  unsigned char RSMK : 1;  // Report SetMarKs (obsolete for IBM,LTO)
  unsigned char LOIS : 1;  // Logical Object ID Supported or Block IDs Supported for T10000
  unsigned char OBR  : 1;  // Object Buffer Recovery or Data Buffer Recovery for T10000

  // byte 9
  unsigned char gapSize;  // Obsolete for IBM, LTO

  // byte 10
  unsigned char BAM        : 1;  // Block Address Mode or reserved for T10000
  unsigned char BAML       : 1;  // Block Address Mode Lock or reserved for T10000
  unsigned char SWP        : 1;  // Soft Write Protect
  unsigned char SEW        : 1;  // Synchronize at Early Warning
  unsigned char EEG        : 1;  // EOD Enabled Generation
  unsigned char eodDefined : 3;  // End Of Data

  // bytes 11-13
  unsigned char bufSizeAtEarlyWarning[3];  // Object buffer size at early warning

  // byte 14
  unsigned char selectDataComprAlgorithm;  // Select data compression algorithm

  // byte 15
  unsigned char PRMWP         : 1;  // PeRManent Write Protect
  unsigned char PERSWP        : 1;  // PERSistent Write Protect
  unsigned char ASOCWP        : 1;  // ASsOCiated Write Protect
  unsigned char rewindOnReset : 2;  // Reserved for T10000
  unsigned char OIR           : 1;  // Only If Reserved  or reserved for T10000
  unsigned char WTRE          : 2;  // WORM Tamper Read Enable
};

class modeSenseDeviceConfiguration_t {
public:
  modeSenseDeviceConfiguration_t() { zeroStruct(this); }

  modeParameterHeader6_t header;
  modeParameterBlockDecriptor_t blockDescriptor;
  modePageDeviceConfiguration_t modePage;
};

/**
     * MODE SENSE(6) or MODE SENSE(10) mode page 0Ah: Control Data Protection.
     * as described in SSC-5.
     */
class modePageControlDataProtection_t {
public:
  // byte 0
  unsigned char pageCode : 6;  // Page code (0Ah)
  unsigned char SPF      : 1;  // SubPage Format (1b)
  unsigned char PS       : 1;  // Parameters Savable
                               // 0b required for MODE SELECT IBM,LTO
                               // 1b returned in MODE SENSE IBM, LTO
                               // 0b Not supported for T10000

  // byte 1
  unsigned char subpageCode;  // SubPage code (F0h)

  // bytes 2-3
  unsigned char pageLength[2];  // Page length (n - 3) 1Ch for IBM,LTO

  // byte 4
  unsigned char LBPMethod;  // LBP method

  // byte 5
  unsigned char LBPInformationLength : 6;  // LBP information length
  unsigned char                      : 2;  // Reserved

  // byte 6
  unsigned char       : 5;  // Reserved
  unsigned char RBDP  : 1;  // Recover Buffered Data Protected
  unsigned char LBP_R : 1;  // Logical blocks protected during read
  unsigned char LBP_W : 1;  // Logical blocks protected during write

  // byte 7
  unsigned char               : 4;  // Reserved
  unsigned char T10PIexponent : 4;  // T1000 only for T10 PI mode

  // bytes 8-31
  unsigned char reserved[24];  // Reserved. Added for IBM, LTO and do
                               // not used by T10000
};

/**
     * MODE SENSE(6) structure for mode page 0Ah: Control Data Protection.
     * as described in SSC-5.
     */
class modeSenseControlDataProtection_t {
public:
  modeSenseControlDataProtection_t() { zeroStruct(this); }

  modeParameterHeader6_t header;
  modeParameterBlockDecriptor_t blockDescriptor;
  modePageControlDataProtection_t modePage;
};

/**
     * MODE SELECT(6) CDB as described in SPC-4.
     */
class modeSelect6CDB_t {
public:
  modeSelect6CDB_t() {
    zeroStruct(this);
    opCode = SCSI::Commands::MODE_SELECT_6;
  }

  // byte 0
  unsigned char opCode;  // OPERATION CODE (15h)

  // byte 1
  unsigned char SP : 1;  // Save Parameters
  unsigned char    : 3;  // Reserved
  unsigned char PF : 1;  // Page Format
  unsigned char    : 3;  // Reserved

  // bytes 2-3
  unsigned char reserved[2];  // Reserved

  // byte 4
  unsigned char paramListLength;  // Parameter list length

  // byte 5
  unsigned char control;  // Control byte
};

/**
     * TEST UNIT READY as described in SPC-4.
     */
class testUnitReadyCDB_t {
public:
  testUnitReadyCDB_t() {
    zeroStruct(this);
    opCode = SCSI::Commands::TEST_UNIT_READY;
  }

  // byte 0
  unsigned char opCode;  // OPERATION CODE (00h)

  // byte 1
  unsigned char : 8;  // Reserved

  // byte 2
  unsigned char EDCC : 1;  // Enable Deferred CHECK CONDITION (IBM only)
  unsigned char      : 7;  // Reserved

  // byte 3-4
  unsigned char reserverd[2];  // Reserved

  // byte 5
  unsigned char control;  // Control byte
};

/**
     * Part of a tape alert log page.
     * This structure does not need to be initialized, as the containing structure
     * (tapeAlertLogPage_t) will do it while initializing itself.
     */
class tapeAlertLogParameter_t {
public:
  unsigned char parameterCode[2];

  unsigned char formatAndLinking : 2;
  unsigned char TMC              : 2;
  unsigned char ETC              : 1;
  unsigned char TSD              : 1;
  unsigned char                  : 1;
  unsigned char DU               : 1;

  unsigned char parameterLength;

  unsigned char flag : 1;
  unsigned char      : 7;
};

/**
     * Tape alert log page, returned by LOG SENSE. Defined in SSC-3, section 8.2.3 TapeAler log page.
     */
template<int n>
class tapeAlertLogPage_t {
public:
  tapeAlertLogPage_t() { zeroStruct(this); }

  unsigned char pageCode : 6;
  unsigned char          : 2;

  unsigned char subPageCode;

  unsigned char pageLength[2];

  tapeAlertLogParameter_t parameters[n];

  /**
       * Utility function computing the number of parameters. This converts a
       * length in bytes (as found in the struct) in a parameter count.
       * @return number of parameters.
       */
  unsigned int parameterNumber() {
    unsigned int numFromLength = SCSI::Structures::toU16(pageLength) / sizeof(tapeAlertLogParameter_t);
    return numFromLength;
  }
};

/**
     * Sense buffer as defined in SPC-4, 
     * section 4.5.2 Descriptor format sense data and 
     * section 4.5.3 Fixed format sense data
     * The sense buffer size is stored in the form of
     * a single byte. Therefore, the constructor forbids
     * creation of a senseData_t structure bigger than
     * 255 bytes.
     * As the structure will be different depending on the response code,
     * everything after the first byte is represented by a union, which
     * can be any of the 2 forms (fixedFormat/descriptorFormat).
     * getXXXXX helper member function allow the getting of on of the other
     * version of the common fields.
     */
template<int n>
class senseData_t {
public:
  senseData_t() {
    if (sizeof(*this) > 255) {
      throw cta::exception::Exception("In SCSI::Structures::senseData_t::senseData_t(): size too big (> 255>");
    }
    zeroStruct(this);
  }

  // byte 0
  unsigned char responseCode : 7;
  unsigned char              : 1;

  // Following bytes can take 2 versions:
  union {
    struct {
      // byte 1
      unsigned char senseKey : 4;
      unsigned char          : 4;
      // Additional sense code (byte 2))
      unsigned char ASC;
      // Additional sense code qualifier (byte 3)
      unsigned char ASCQ;
      // byte 4
      unsigned char           : 7;
      unsigned char SDAT_OVFL : 1;
      // byte 5-6
      unsigned char reserved[2];
      // byte 7
      unsigned char additionalSenseLength;
      // byte 8 onwards
      unsigned char additionalSenseBuffer[n - 8];
    } descriptorFormat;

    struct {
      // byte 1
      unsigned char obsolete;
      // byte 2
      unsigned char senseKey  : 4;
      unsigned char SDAT_OVFL : 1;
      unsigned char ILI       : 1;
      unsigned char EOM       : 1;
      unsigned char filemark  : 1;
      // bytes 3-6
      unsigned char information[4];
      // byte 7
      unsigned char additionalSenseLength;
      // bytes 8 - 11
      unsigned char commandSpecificInformation[4];
      // Additional sense code (byte 12))
      unsigned char ASC;
      // Additional sense code qualifier (byte 13)
      unsigned char ASCQ;
      // bytes 14
      unsigned char fieldReplaceableUnitCode;
      // bytes 15-17
      unsigned char senseSpecificInformation[3];
      // bytes 18 onwards
      unsigned char aditionalSenseBuffer[n - 18];
    } fixedFormat;

    // Helper functions for common fields
    // First make the difference between the fixed/descriptor
    // and current/deffered
  };

  bool isFixedFormat() { return responseCode == 0x70 || responseCode == 0x71; }

  bool isDescriptorFormat() { return responseCode == 0x72 || responseCode == 0x73; }

  bool isCurrent() { return responseCode == 0x70 || responseCode == 0x72; }

  bool isDeferred() { return responseCode == 0x71 || responseCode == 0x73; }

  uint8_t getASC() {
    if (isFixedFormat()) {
      return fixedFormat.ASC;
    }
    else if (isDescriptorFormat()) {
      return descriptorFormat.ASC;
    }
    else {
      std::stringstream err;
      err << "In senseData_t::getASC: no ACS with this response code or response code not supported (" << std::hex
          << std::showbase << (int) responseCode << ")";
      throw cta::exception::Exception(err.str());
    }
  }

  uint8_t getASCQ() {
    if (isFixedFormat()) {
      return fixedFormat.ASCQ;
    }
    else if (isDescriptorFormat()) {
      return descriptorFormat.ASCQ;
    }
    else {
      std::stringstream err;
      err << "In senseData_t::getASCQ: no ACSQ with this response code or response code not supported (" << std::hex
          << std::showbase << (int) responseCode << ")";
      throw cta::exception::Exception(err.str());
    }
  }

  /**
       * Returns the Sense Key value.
       */
  unsigned char getSenseKey() {
    if (isFixedFormat()) {
      return fixedFormat.senseKey;
    }
    else if (isDescriptorFormat()) {
      return descriptorFormat.senseKey;
    }
    else {
      std::stringstream err;
      err << "In senseData_t::getSenseKey: no Sense Key with this response "
             "code or response code not supported ("
          << std::hex << std::showbase << (int) responseCode << ")";
      throw cta::exception::Exception(err.str());
    }
  }

  /**
       * Returns the Sense Key value as string.
       */
  std::string getSenseKeyString() {
    if (castor::tape::SCSI::senseKeys::lastWithText >= getSenseKey()) {
      return castor::tape::SCSI::senseKeys::senseKeysText[getSenseKey()];
    }
    else {
      std::stringstream err;
      err << "In senseData_t::getSenseKeyString: no Sense Key with this "
             "value ("
          << std::hex << std::showbase << (int) getSenseKey() << ")";
      throw cta::exception::Exception(err.str());
    }
  }

  /**
       * Function turning the ACS/ACSQ contents into a string.
       * This function is taken from the Linux kernel sources.
       * see scsi_extd_sense_format.
       *
       * TODO: add support for other bits. See section 4.5.6
       * of SPC-4 for sense key = NO SENSE.
       *
       * @return the error string as defined by SCSI specifications.
       */
  std::string getACSString() {
    uint8_t asc = getASC();
    uint8_t ascq = getASCQ();
    return SCSI::senseConstants::getASCString(asc, ascq);
  }
};

/**
     * READ END OF WRAP POSITION CDB as described in LTO-8 SCSI Reference, p.119
     */
class readEndOfWrapPositionCDB_t {
public:
  readEndOfWrapPositionCDB_t() {
    zeroStruct(this);
    // REOWP is a service action of the MAINTENANCE_IN command: A3 1F 45
    opCode = SCSI::Commands::MAINTENANCE_IN;
    SERVICE_ACTION = 0x1F;
    serviceActionQualifier = 0x45;
  }

  // byte 0
  unsigned char opCode;  // OPERATION CODE (A3h)

  // byte 1
  unsigned char SERVICE_ACTION : 5;  // 1Fh
  unsigned char                : 3;

  // byte 2
  unsigned char serviceActionQualifier;  // 45h

  // byte 3
  unsigned char WNV : 1;  // Wrap Number Valid: 0 = request data for first wrap on the tape
                          //                    1 = return data for wrap in wrapNumber field
  unsigned char RA : 1;   // Report All: 0 = short form reply (return data for a single wrap)
                          //             1 = long form reply  (return data for all wraps)
  unsigned char : 6;

  // byte 4
  unsigned char reserved1;  // Reserved

  // byte 5
  unsigned char wrapNumber;  // Wrap for which the end of wrap position is requested.
                             // If set, WNV must be 1 and RA must be 0.
  // bytes 6-9
  unsigned char allocationLength[4];  // Maximum number of bytes to be transferred
                                      // In the case of Report All, each wrap descriptor is 12
                                      // bytes. LTO-9 tapes have 280 wraps, so reply can be up
                                      // to 3084 bytes. And longer in case of LTO-10 (details
                                      // not available at time of writing).
  // byte 10
  unsigned char reserved2;  // Reserved

  // byte 11
  unsigned char control;  // Control byte
};

/**
     * REOWP Short form parameter data, as described in LTO-8 SCSI Reference, p.120
     */
class readEndOfWrapPositionDataShortForm_t {
public:
  readEndOfWrapPositionDataShortForm_t() { zeroStruct(this); }

  // bytes 0-1
  unsigned char responseDataLength[2];  // 08h, the number of bytes to follow

  // bytes 2-3
  unsigned char reserved[2];  // Reserved

  // bytes 4-9
  unsigned char logicalObjectIdentifier[6];  // The logical object identifier of the object at the
                                             // end of the wrap requested by the WNV bit and the
                                             // WRAP_NUMBER field
};

/**
     * REOWP Long form parameter data, as described in LTO-8 SCSI Reference, p.120-121
     */
class readEndOfWrapPositionDataLongForm_t {
public:
  readEndOfWrapPositionDataLongForm_t() { zeroStruct(this); }

  // bytes 0-1
  unsigned char responseDataLength[2];  // n-1, the number of bytes to follow

  // bytes 2-3
  unsigned char reserved[2];  // Reserved

  // bytes 4-n
  struct WrapDescriptor {
    unsigned char wrapNumber[2];  // Wrap number
    unsigned char partition[2];   // The partition number of the above wrap
    unsigned char reserved[2];    // Reserved
    unsigned char
      logicalObjectIdentifier[6];     // The logical object identifier of the object at the end of the above wrap
  } wrapDescriptor[maxLTOTapeWraps];  // Array of wrap descriptiors

  uint16_t getNbWrapsReturned() {
    return ((SCSI::Structures::toU16(responseDataLength) - sizeof(reserved)) / sizeof(WrapDescriptor));
  }
};

/**
     * REQUEST SENSE CDB as described in LTO-8 SCSI Reference, p.157
     */
class requestSenseCDB_t {
public:
  requestSenseCDB_t() {
    zeroStruct(this);
    opCode = SCSI::Commands::REQUEST_SENSE;
  }

  // byte 0
  unsigned char opCode;  // OPERATION CODE (03h)

  // bytes 1-3
  unsigned char reserved[3];  // Reserved

  // byte 4
  unsigned char allocationLength;  // Maximum number of bytes to be transferred (up to 96), see 5.2.29

  // byte 5
  unsigned char control;  // Control byte
};

/**
     * REQUEST SENSE data format, as described in LTO-8 SCSI Reference, p.158
     */
class requestSenseData_t {
public:
  requestSenseData_t() { zeroStruct(this); }

  // byte 0
  unsigned char RESPONSE_CODE : 7;  // 70h current, 71h deferred
  unsigned char VALID         : 1;  // Information bytes 3-6 are valid

  // byte 1
  unsigned char : 8;  // Obsolete

  // byte 2
  unsigned char SENSE_KEY : 4;  // See Annex B
  unsigned char           : 1;  // Reserved
  unsigned char ILI       : 1;  // Incorrect Length Indicator
  unsigned char EOM       : 1;  // Device is at end of medium
  unsigned char FILEMARK  : 1;  // The current command has encountered a filemark

  // bytes 3-6
  unsigned char information[4];  // Valid if VALID bit is set. Generally only used for non-deferred errors.

  // byte 7
  unsigned char
    additionalSenseLength;  // 0Ah only 18 bytes of sense data returned, 58h full 96 bytes of sense data returned

  // bytes 8-11
  unsigned char commandSpecificInformation[4];  // Not supported by LTO-8 drives

  // byte 12
  unsigned char additionalSenseCode;  // See Annex B

  // byte 13
  unsigned char additionalSenseCodeQualifier;  // See Annex B

  // byte 14
  unsigned char fieldReplacableUnitCode;  // Used for extended fault isolation information

  // byte 15
  unsigned char BIT_POINTER : 3;  // Points to bit in error of the field specified by the FIELD_POINTER
  unsigned char BPV         : 1;  // Bit Pointer Valid, indicates whether BIT_POINTER contains information
  unsigned char             : 2;  // Reserved
  unsigned char C_D         : 1;  // Control/Data, indicates if error is in a data field or CDB field
  unsigned char SKSV_BIT    : 1;  // Sense Key Specific Valid

  // bytes 16-17
  unsigned char SKSV[2];  // Field Pointer, points to the CDB byte or parameter byte in error

  // bytes 18-19
  unsigned char reportingErrorFlagData[2];  // Reporting Error Flag Data

  // byte 20
  unsigned char : 8;  // Reserved

  // byte 21
  unsigned char VOLVALID : 1;  // Indicates if Volume Label and Volume Label Cartridge Type contain valid information
  unsigned char DUMP     : 1;  // Indicates if a debug dump is present in the drive
  unsigned char          : 1;  // Reserved
  unsigned char CLN      : 1;  // Is the device requesting a clean?
  unsigned char DRVSRVC  : 1;  // Does the drive have a hardware fault causing it to be inoperative?
  unsigned char          : 3;  // Reserved

  // bytes 22-28
  char volumeLabel[7];  // Seven characters from left of Volume Label

  // byte 29
  unsigned char physicalWrap;  // Physical wrap of the current location. LSB reflects current physical direction:
                               //    0b - current direction is away from the physical beginning of tape
                               //    1b - current direction is towards the physical beginning of tape
                               //   FFh - logical wrap number exceeds 254, physical direction is not reflected
  // bytes 30-33
  unsigned char relativeLPOSValue[4];  // The current physical position on tape

  // byte 34
  unsigned char SCSIAddress;  // Obsolete, use portIdentifier instead

  // byte 35
  unsigned char RS422Information;  // May contain a value passed across the RS-422 serial interface by a tape library

  // byte 36
  unsigned char activePartition : 3;  // Partition number of the current logical position of the volume
  unsigned char                 : 5;  // Reserved

  // bytes 37-39
  unsigned char portIdentifier[3];  // Address of the port through which the sense is reported, fibre channel or SAS

  // byte 40
  unsigned char relativeTgtPort     : 3;  // Relative target port through which sense data is reported
  unsigned char                     : 3;  // Reserved
  unsigned char tapePartitionsExist : 1;  // Does the mounted volume contain more than one partition?
  unsigned char tapeDirectoryValid  : 1;  // Is the tape directory valid?

  // byte 41
  unsigned char hostCommand;  // SCSI Opcode of the command to which sense data is being returned

  // byte 42
  unsigned char mediaType        : 4;  // Vendor reserved
  unsigned char cartridgeGenType : 4;  // Cartridge generation type, 000b = Gen1, 111b = Gen 8

  // bytes 43-44
  unsigned char volumeLabelCartridgeType[2];  // Valid if VOLVALID bit is set to 1b. Can be 'L7', 'M8', 'L8', etc.

  // bytes 45-48
  unsigned char logicalBlockNumber[4];  // Current LBA that would be reported in Read Position command

  // bytes 49-52
  unsigned char datasetNumber[4];

  // bytes 53-54
  unsigned char firstErrorFSC[2];

  // bytes 55-56
  unsigned char firstErrorFlagData[2];

  // bytes 57-58
  unsigned char secondErrorFSC[2];

  // bytes 59-60
  unsigned char secondErrorFlagData[2];

  // bytes 61-62
  unsigned char nextToLastErrorFSC[2];

  // bytes 63-64
  unsigned char nextToLastErrorFlagData[2];

  // bytes 65-66
  unsigned char lastErrorFSC[2];

  // bytes 67-68
  unsigned char lastErrorFlagData[2];

  // byte 69
  unsigned char LPOSRegion;

  // bytes 70-85
  char ERPSummaryInformation[16];

  // bytes 86-95
  char cartridgeSerialNumber[10];  // This is the value from the CRM right-justified, not the Barcode
};

namespace encryption {

class spinCDB_t {
public:
  spinCDB_t() {
    zeroStruct(this);
    opCode = SCSI::Commands::SECURITY_PROTOCOL_IN;
  }

  unsigned char opCode;
  unsigned char securityProtocol;
  unsigned char securityProtocolSpecific[2];
  unsigned char reserved[2];
  unsigned char allocationLength[4];
  unsigned char reserved2;
  unsigned char controlByte;
};

class spoutCDB_t {
public:
  spoutCDB_t() {
    zeroStruct(this);
    opCode = SCSI::Commands::SECURITY_PROTOCOL_OUT;
  }

  unsigned char opCode;
  unsigned char securityProtocol;
  unsigned char securityProtocolSpecific[2];
  unsigned char reserved[2];
  unsigned char allocationLength[4];
  unsigned char reserved2;
  unsigned char controlByte;
};

template<int n>
class spinPageList_t {
public:
  spinPageList_t() { zeroStruct(this); }

  unsigned char reserved[6];
  unsigned char supportedProtocolListLength[2];
  unsigned char list[n];
};

/**
       * Security Protocol OUT-Set Data Encryption Page as described in SSC-4.
       */
class spoutSDEParam_t {
public:
  spoutSDEParam_t() {
    zeroStruct(this);
    setU16(pageCode, SCSI::encryption::spoutSecurityProtocolSpecificPages::setDataEncryptionPage);
    setU16(keyLength, SCSI::encryption::ENC_KEY_LENGTH);
  }

  unsigned char pageCode[2];
  unsigned char length[2];

  unsigned char lock       : 1;
  unsigned char            : 4;
  unsigned char nexusScope : 3;  // Specifies the scope of the data encryption parameters

  unsigned char CKORL : 1;  // Clear key on reservation loss
  unsigned char CKORP : 1;  // Clear key on reservation preempt
  unsigned char CKOD  : 1;  // Clear key on reservation demount
  unsigned char SDK   : 1;  // Supplemental decryption key
  unsigned char RDMC  : 2;  // Raw decryption mode control
  unsigned char CEEM  : 2;  // Check external encryption mode

  unsigned char encryptionMode;
  unsigned char decryptionMode;
  unsigned char algorithmIndex;
  unsigned char keyFormat;
  unsigned char kadFormat;
  unsigned char reserved[7];
  unsigned char keyLength[2];
  unsigned char keyData[SCSI::encryption::ENC_KEY_LENGTH];
};
}  // namespace encryption

namespace RAO {

/**
        * Receive RAO Command Descriptor Block (CDB)
        */
class recieveRAO_t {
public:
  recieveRAO_t() {
    zeroStruct(this);
    opcode = SCSI::Commands::MAINTENANCE_IN;
  }

  unsigned char opcode;

  unsigned char serviceAction : 5;
  unsigned char               : 2;
  unsigned char udsLimits     : 1;

  unsigned char raoListOffset[4];

  unsigned char allocationLength[4];

  unsigned char udsType : 3;
  unsigned char         : 5;

  unsigned char control;
};

/**
        * UDS (User Data Segments) limits page
        */
class udsLimitsPage_t {
public:
  udsLimitsPage_t() { zeroStruct(this); }

  unsigned char maxSupported[2];
  unsigned char maxSize[2];
};

class udsLimits {
public:
  uint16_t maxSupported;
  uint16_t maxSize;
};

/**
        * Generate RAO CDB
        */
class generateRAO_t {
public:
  generateRAO_t() {
    zeroStruct(this);
    opcode = SCSI::Commands::MAINTENANCE_OUT;
    raoProcess = 2;
  }

  unsigned char opcode;

  unsigned char serviceAction : 5;
  unsigned char               : 3;

  unsigned char raoProcess : 3;
  unsigned char            : 5;

  unsigned char udsType : 3;
  unsigned char         : 5;

  unsigned char reserved[2];

  unsigned char paramsListLength[4];

  unsigned char reserved2;

  unsigned char control;
};

class udsDescriptor_t {
public:
  udsDescriptor_t() {
    zeroStruct(this);
    setU16(descriptorLength, 0x1e);
  }

  unsigned char descriptorLength[2];
  unsigned char reserved[3];
  unsigned char udsName[10];
  unsigned char partitionNumber;
  unsigned char beginLogicalObjID[8];
  unsigned char endLogicalObjID[8];
};

/**
        * RAO list struct
        */
class raoList_t {
public:
  raoList_t() { zeroStruct(this); }

  unsigned char raoProcess : 3;
  unsigned char            : 5;

  unsigned char status : 3;
  unsigned char        : 5;

  unsigned char res[2];

  unsigned char raoDescriptorListLength[4];

  udsDescriptor_t udsDescriptors[1];
};

/**
        * Generate RAO parameters
        */
class generateRAOParams_t {
public:
  generateRAOParams_t() { zeroStruct(this); }

  unsigned char res[4];
  unsigned char udsListLength[4];
  udsDescriptor_t udsDescriptors[1];
};

/**
        * Block Limits
        */
class blockLims {
public:
  blockLims() { zeroStruct(this); }

  unsigned char fseq[10];
  uint64_t begin;
  uint64_t end;
};
}  // namespace RAO

template<size_t n>
/**
     * Extract a string from a fixed size array. This function
     * gets rid of zeros in array and stops the extracted string
     * there. In SCSI, the arrays are space padded, so the string
     * should have a size equal to n usually. This function is templated
     * to manage the fixed-size array in the SCSI structures conveniently.
     * @param t array pointer to the char array.
     * @return the extracted string.
     */
std::string toString(const char (&t)[n]) {
  std::stringstream r;
  r.write(t, std::find(t, t + n, '\0') - t);
  return r.str();
}

std::string toString(const inquiryData_t&);

template<size_t n>
std::string hexDump(const unsigned char (&d)[n]) {
  std::stringstream hex;
  hex << std::hex << std::setfill('0');
  size_t pos = 0;
  while (pos < (8 * (n / 8))) {
    hex << std::setw(4) << pos << " | ";
    for (int i = 0; i < 8; i++) {
      hex << std::setw(2) << ((int) d[pos + i]) << " ";
    }
    hex << "| ";
    for (int i = 0; i < 8; i++) {
      hex << std::setw(0) << d[pos + i];
    }
    hex << std::endl;
    pos += 8;
  }
  if (n % 8) {
    hex << std::setw(4) << pos << " | ";
    for (size_t i = 0; i < (n % 8); i++) {
      hex << std::setw(2) << ((int) d[pos + i]) << " ";
    }
    for (size_t i = (n % 8); i < 8; i++) {
      hex << "   ";
    }
    hex << "| ";
    for (size_t i = 0; i < (n % 8); i++) {
      hex << std::setw(0) << d[pos + i];
    }
    hex << std::endl;
  }
  return hex.str();
}
}  // namespace Structures
}  // namespace SCSI
}  // namespace tape
}  // namespace castor
