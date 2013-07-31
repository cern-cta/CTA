// ----------------------------------------------------------------------
// File: Drive/Structures.hh
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

#pragma once

#include <sstream>
#include <iomanip>
#include <algorithm>
#include <arpa/inet.h>
#include <string.h>
#include <scsi/sg.h>
#include <climits>

#include "Constants.hh"
#include "../Exception/Exception.hh"

namespace SCSI {
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
    template <typename C>
    void zeroStruct(C * s) {
      memset (s, 0, sizeof(C));
    }
    
    /**
     * Class wrapping around Linux' SG_IO struct, providing
     * zeroing and automatic filling up for the mandatory structures
     * (cdb, databuffer, sense buffer, magic 'S', and default timeout).
     * Make it look like a bare sg_io_hdr_t when using & operator.
     * Another little boilerplate killer.
     */
    class LinuxSGIO_t: public sg_io_hdr_t {
    public:
      LinuxSGIO_t() { zeroStruct(this); interface_id = 'S'; timeout = 30000; }
      template <typename T>
      void setCDB(T * cdb) { cmdp = (unsigned char *)cdb; cmd_len = sizeof(T); }
      
      template <typename T>
      void setSenseBuffer(T * senseBuff) throw (Tape::Exception) 
      { 
        if (sizeof(T) > UCHAR_MAX)
          throw Tape::Exception("sense structure too big in LinuxSGIO_t::setSense");
        sb_len_wr = (unsigned char) sizeof(T);
        sbp = (unsigned char *)senseBuff;
      }
      
      template <typename T>
      void setDataBuffer(T * dataBuff) { dxferp = dataBuff; dxfer_len = sizeof (T); }
      
      sg_io_hdr_t * operator & () { return (sg_io_hdr_t *) this; }
    };
    
    /**
     * Helper function to deal with endianness.
     * @param t byte array in SCSI order representing a 64 bits number
     * @return 
     */
    inline uint64_t toU64(const unsigned char(& t)[8])
    {
      /* Like network, SCSI is BigEndian */
      return (uint64_t) ntohl ( (*(uint64_t *) t << 32) >> 32)  << 32 | ntohl(*(uint64_t *) t >>32);
    }  
           
    /**
     * Helper function to deal with endianness.
     * @param t byte array in SCSI order representing a 32 bits number
     * @return 
     */
    inline uint32_t toU32(const unsigned char(& t)[4])
    {
      /* Like network, SCSI is BigEndian */
      return ntohl (*((uint32_t *) t));
    }

    /**
     * Helper function to deal with endianness.
     * @param t byte array in SCSI order representing a 16 bits number
     * @return 
     */
    inline uint16_t toU16(const unsigned char(& t)[2])
    {
      /* Like network, SCSI is BigEndian */
      return ntohs (*((uint16_t *) t));
    }
    
    /**
     * Inquiry CDB as described in SPC-4.
     */
    class inquiryCDB_t {
    public:
      unsigned char opCode;
      
      unsigned char EVPD : 1;
      unsigned char : 7;
      
      unsigned char pageCode;
      
      char allocationLength[2];
      
      unsigned char control;
      inquiryCDB_t() { zeroStruct(this); opCode = SCSI::Commands::INQUIRY; }
    };
    
    /**
     * Inquiry data as described in SPC-4.
     */
    typedef struct {
      unsigned char perifDevType : 5;
      unsigned char perifQualifyer : 3;

      unsigned char : 7;
      unsigned char RMB : 1;

      unsigned char version : 8;

      unsigned char respDataFmt : 4;
      unsigned char HiSup : 1;
      unsigned char normACA : 1;
      unsigned char : 2;

      unsigned char addLength : 8;

      unsigned char protect : 1;
      unsigned char : 2;
      unsigned char threePC : 1;
      unsigned char TPGS : 2;
      unsigned char ACC : 1;
      unsigned char SCCS : 1;

      unsigned char addr16 : 1;
      unsigned char : 3;
      unsigned char multiP : 1;
      unsigned char VS1 : 1;
      unsigned char encServ : 1;
      unsigned char : 1;

      unsigned char VS2 : 1;
      unsigned char cmdQue : 1;
      unsigned char : 2;
      unsigned char sync : 1;
      unsigned char wbus16 : 1;
      unsigned char : 2;

      char T10Vendor[8];
      char prodId[16];
      char prodRevLvl[4];
      char vendorSpecific1[20];

      unsigned char IUS : 1;
      unsigned char QAS : 1;
      unsigned char clocking : 2;
      unsigned char : 4;

      unsigned char reserved1;

      unsigned char versionDescriptor[8][2];

      unsigned char reserved2[22];
      unsigned char vendorSpecific2[1];
    } inquiryData_t;
    
    /*
     * LOG SELECT CDB as described in SPC-4.
     */
    class logSelectCDB_t {
    public:
      // byte 0
      unsigned char opCode;                // OPERATION CODE (4Ch)
      
      // byte 1
      unsigned char SP : 1;                // the Save Parameters
      unsigned char PCR: 1;                // the Parameter Code Reset
      unsigned char    : 6;                // Reserved 
      
      // byte 2
      unsigned char pageCode: 6;           // PAGE CODE
      unsigned char PC: 2;                 // the Page Control
      
      // byte 3
      unsigned char subPageCode;           // SUBPAGE CODE (Reserved for T10000)
      
      // bytes 4-6
      unsigned char reserved[3];           // Reserved
      
      // bytes 7-8
      unsigned char parameterListLength[2];// PARAMETER LIST LENGTH
      
      // byte 9
      unsigned char control;               // CONTROL   
      
      logSelectCDB_t() {
        zeroStruct(this);
        opCode = SCSI::Commands::LOG_SELECT; 
      }
    };

    /**
     * Log sense CDB as described in SPC-4, 
     */
    class logSenseCDB_t {
    public:
      unsigned char opCode;
      
      unsigned char SP : 1;
      unsigned char PPC: 1;
      unsigned char :6;
      
      unsigned char pageCode : 6;
      unsigned char PC : 2;
      
      unsigned char subPageCode;
      
      unsigned char reserved;
      
      unsigned char parameterPointer[2];
      
      unsigned char allocationLength[2];
      
      unsigned char control;
      
      logSenseCDB_t() { zeroStruct(this); opCode = SCSI::Commands::LOG_SENSE; }
    };
    
    /**
    * Log sense Log Page Parameter Format as described in SPC-4, 
    */
    class logSenseParameterHeader_t  {
    public:
      // bytes 0-1
      unsigned char parameterCode [2];
      
      // byte 2
      unsigned char formatAndLinking : 2; // reserved and List Parameter bits
      unsigned char TMC : 2;              // Threshold Met Criteria
      unsigned char ETC : 1;              // Enable Threshold Comparison
      unsigned char TSD : 1;              // Target Save Disable
      unsigned char : 1;                  // DS Disable Save for T10000
      unsigned char DU : 1;               // Disable Update 
      
      // byte 3
      unsigned char parameterLength;      // n-3          
    };
    
    class logSenseParameter_t {
    public:
      // bytes 0-3
      logSenseParameterHeader_t header;
           
      // bytes 4-n
      unsigned char parameterValue[1];     // parameters have variable length 
    };
    
    /**
    * Log sense Log Page Format as described in SPC-4, 
    */
    class logSenseLogPageHeader_t {
    public:
      // byte 0
      unsigned char pageCode : 6;
      unsigned char SPF: 1;          // the Subpage format
      unsigned char DS: 1;           // the Disable Slave bit
      
      // byte 1
      unsigned char subPageCode;
      
      // bytes 2-3
      unsigned char pageLength[2];   // n-3 number of bytes without header   
    };
    /**
    * Log sense Log Page Format as described in SPC-4, 
    */
    class logSenseLogPage_t {
    public:
      // bytes 0-3
      logSenseLogPageHeader_t header;
      
      // bytes 4-n      
      logSenseParameter_t parameters [1]; // parameters have variable length
    };
    

    
    class tapeAlertLogParameter_t {
    public:
      unsigned char parameterCode [2];
      
      unsigned char formatAndLinking : 2;
      unsigned char TMC : 2;
      unsigned char ETC : 1;
      unsigned char TSD : 1;
      unsigned char : 1;
      unsigned char DU : 1;
      
      unsigned char parameterLength;
      
      unsigned char flag : 1;
      unsigned char : 7;
    };
    
    /**
     * Tape alert log page, returned by LOG SENSE. Defined in SSC-3, section 8.2.3 TapeAler log page.
     */
    template <int n>
    class tapeAlertLogPage_t {
    public:
      unsigned char pageCode : 6;
      unsigned char : 2;
      
      unsigned char subPageCode;
      
      unsigned char pageLength[2];
      
      tapeAlertLogParameter_t parameters [n];
      
      /**
       * Utility function computing the number of parameters. This converts a
       * length in bytes (as found in the struct) in a parameter count.
       * @return number of parameters.
       */
      int parameterNumber() throw (Tape::Exception) {
        int numFromLength = SCSI::Structures::toU16(pageLength) / sizeof (tapeAlertLogParameter_t);
        return numFromLength;
      }
      tapeAlertLogPage_t() { zeroStruct(this); }
    };
    
    template <size_t n>
    std::string toString(const char(& t)[n]) {
      std::stringstream r;
      r.write(t, std::find(t, t + n, '\0') - t);
      return r.str();
    }
    
    std::string toString(const inquiryData_t &);
    
    template <size_t n>
    std::string hexDump(const unsigned char(& d)[n]) {
      std::stringstream hex;
      hex << std::hex << std::setfill('0');
      int pos = 0;
      while (pos < (8* (n / 8))) {
        hex << std::setw(4) << pos << " | ";
        for (int i=0; i<8; i++)
          hex << std::setw(2) << ((int) d[pos + i]) << " ";
        hex << "| ";
        for (int i=0; i<8; i++)
          hex << std::setw(0) << d[pos + i];
        hex << std::endl;
        pos += 8;
      }
      if (n % 8) {
        hex << std::setw(4) << pos << " | ";
        for (int i=0; i<(n % 8); i++)
          hex << std::setw(2) << ((int) d[pos + i]) << " ";
        for (int i=(n % 8); i<8; i++)
          hex << "   ";
        hex << "| ";
        for (int i=0; i<(n % 8); i++)
          hex << std::setw(0) << d[pos + i];
        hex << std::endl;
      }
      return hex.str();
    }
  };


};
