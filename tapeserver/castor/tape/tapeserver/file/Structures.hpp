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

#include "tapeserver/castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "tapeserver/castor/tape/tapeserver/SCSI/Structures.hpp"
#include "common/exception/InvalidArgument.hpp"
#include <string>
#include <stdexcept>

namespace castor {
namespace tape {
  namespace tapeFile {

    /**
     * Helper template to fill with space a structure. 
     * @param s pointer the struct/class.
     */
    template <typename C>
    void spaceStruct(C * s) {
      memset(s, 0x20, sizeof (C));
    }

    template <size_t n>
    /**
     * Templated helper function to get std::string from the char array 
     * not terminated by '\0'.
     * 
     * @param t   array pointer to the char array.
     * @return    std::string for the char array
     */
    std::string toString(const char(& t)[n]) {
      std::string r;
      r.assign(t, n);
      return r;
    }

    class VOL1 {
    public:

      VOL1() {
        spaceStruct(this);
      }
    protected:
      char m_label[4];         // The characters VOL1. 
      char m_VSN[6];           // The Volume Serial Number. 
      char m_accessibility[1]; // A space indicates that the volume is authorized.
      char m_reserved1[13];    // Reserved.
      char m_implID[13];       // The Implementation Identifier - spaces.
      char m_ownerID[14];      // CASTOR or stagesuperuser name padded with spaces.
      char m_reserved2[26];    // Reserved
      char m_LBPMethod[2];     // Logic block protection checksum type.
                               // This field is a CASTOR variation from the ECMA 013/ISO1001
                               // standard. It contains 2 spaces or '00'. Otherwise, contains
                               // the ASCII representation of the hexadecimal value of the
                               // Logical block protection method, as defined in the SSC-5 (latest drafts)
                               // In practice we intend to use "  " (double space) or "00" 
                               // for no LBP, "02" for CRC32C where possible (enterprise drives)
                               // and "01" for ECMA-319 Reed-Solomon where not (LTO drives).
      char m_lblStandard[1];   // The label standard level - ASCII 3 for the CASTOR
    public:
      /**
       * Fills up all fields of the VOL1 structure with proper values and data provided.
       * @param VSN the tape serial number
       * @param LBPMethod The logical block protection method.
       */
      void fill(std::string VSN, unsigned char LBPMethod);

      /**
       * @return VSN the tape serial number
       */
      inline std::string getVSN() const {
        return toString(m_VSN);
      }
      
      /**
       * @return the logic block protection method as parsed from the header
       */
      inline unsigned char getLBPMethod() const {
        if (!::strncmp(m_LBPMethod, "  ", sizeof(m_LBPMethod)))
          return SCSI::logicBlockProtectionMethod::DoNotUse;
        // Generate a proper string for the next steps, as otherwise functions
        // get confused by the lack of zero-termination.
        std::string LBPMethod;
        LBPMethod.append(m_LBPMethod, sizeof (m_LBPMethod));
        unsigned char hexValue;
        try {
          hexValue = std::stoi(LBPMethod, 0, 16);
        } catch (std::invalid_argument &) {
          throw cta::exception::InvalidArgument(
            std::string("In VOL1::getLBPMethod(): syntax error for numeric value: ") + LBPMethod);
        } catch (std::out_of_range &) {
          throw cta::exception::InvalidArgument(
            std::string("In VOL1::getLBPMethod(): out of range value: ") + LBPMethod);
        }
        switch (hexValue) {
          case SCSI::logicBlockProtectionMethod::DoNotUse:
          case SCSI::logicBlockProtectionMethod::CRC32C:
          case SCSI::logicBlockProtectionMethod::ReedSolomon:
            return hexValue;
          default:
            throw cta::exception::InvalidArgument(
              std::string("In VOL1::getLBPMethod(): unexpected value: ") + LBPMethod);
        }
      }

      /**
       * Throws an exception if the structure does not match expectations.
       */
      void verify();
    };

    class VOL1withCrc : public VOL1 {
    public:
      VOL1withCrc(): VOL1() { m_crc = 0; }
    protected:
      uint32_t m_crc;          // 32bit crc addition for VOL1
    };

    // The common part of the HDR1, EOF1 and PRELEBEL HDR1 labels

    class HDR1EOF1 {
    public:

      HDR1EOF1() {
        spaceStruct(this);
      }
    protected:
      char m_label[4];         // The characters HDR1. 
      char m_fileId[17];       // The CASTOR NS file Id in ACSII hex or PRELABEL
      char m_VSN[6];           // The Volume Serial Number.
      char m_fSec[4];          // The file section number - '0001'.
      char m_fSeq[4];          // The file sequence number is modulus by 10000.
      char m_genNum[4];        // The generation number - '0001'.
      char m_verNumOfGen[2];   // The version number of generation - '00'.
      char m_creationDate[6];  // The creation date in cyyddd form.
      char m_expirationDate[6];// The expiration date in cyyddd form.
      char m_accessibility[1]; // The security status of the data set - space.
      char m_blockCount[6];    // In the HDR1 is always '000000'.
      char m_sysCode[13];      // The system ID code - 'CASTOR '+CASTORBASEVERSION
      char m_reserved[7];      // Reserved
      /**
       * Fills up all common fields of the HDR1, EOF1 and PRELABEL HDR1 
       * structures with proper values and data provided.
       * 
       * @param fileId       The CASTOR NS file Id in ACSII hex.
       * @param VSN          The tape serial number.
       * @param fSeq         The file sequence number on the tape.
       */
      void fillCommon(std::string fileId, std::string VSN, int fSeq);

      /**
       * Throws an exception if the common field of the structures does
       * not match expectations.
       */
      void verifyCommon() const ;
    public:

      /**
       * @return VSN the tape serial number
       */
      inline std::string getVSN() const {
        return toString(m_VSN);
      }

      /**
       * @return The CASTOR NS file Id
       */
      inline std::string getFileId() const {
        return toString(m_fileId);
      }

      /**
       * @return The file sequence number (modulo 10000: useless)
       */
      inline std::string getfSeq() const {
        return toString(m_fSeq);
      }
      
      /**
       * @return The number of block written on tape per file (only valid in trailer, fairly useless)
       */
      inline std::string getBlockCount() const {
        return toString(m_blockCount);
      }
    };

    class HDR1 : public HDR1EOF1 {
    public:
      /**
       * Fills up only a few fields of the HDR1 structure with proper values 
       * and data provided.
       * 
       * @param fileId       The CASTOR NS file Id in ACSII hex.
       * @param VSN          The tape serial number.
       * @param fSeq         The file sequence number on the tape.
       */
      void fill(std::string fileId, std::string VSN, int fSeq);

      /**
       * Throws an exception if the structure does not match expectations.
       */
      void verify() const ;
    };

    class EOF1 : public HDR1EOF1 {
    public:
      /**
       * Fills up only a few fields of the EOF1 structure with proper values 
       * and data provided.
       * 
       * @param fileId       The CASTOR NS file Id in ACSII hex.
       * @param VSN          The tape serial number.
       * @param fSeq         The file sequence number on the tape.
       * @param blockCount   The number of written data blocks. It is
       *                     modulus by 1000000.
       */
      void fill(std::string fileId,
        std::string VSN, int fSeq, int blockCount);

      /**
       * Throws an exception if the structure does not match expectations.
       */
      void verify() const ;
    };
    
    class HDR1PRELABEL : public HDR1EOF1 {
    public:
      /**
       * Fills up only a few fields of the HDR1 structure with proper values 
       * and data provided.
       * 
       * @param VSN          The tape serial number.
       */
      void fill(std::string VSN);

      /**
       * Throws an exception if the structure does not match expectations.
       */
      void verify() const ;
    };

    // The common part of the HDR2 and EOF2 labels

    class HDR2EOF2 {
    public:

      HDR2EOF2() {
        spaceStruct(this);
      }
    protected:
      char m_label[4];        // The characters HDR2. 
      char m_recordFormat[1]; // The record format is 'F' for the CASTOR AUL. 
      char m_blockLength[5];  // If it is greater than 100000 it set as '00000'.
      char m_recordLength[5]; // If it is greater than 100000 it set as '00000'.
      char m_tapeDensity[1];  // The tape density code. Not used or verified.
      char m_reserved1[18];   // Reserved
      char m_recTechnique[2]; // The tape recording technique. 'P ' for xxxGC 
                              // drives (only on the real tapes on we do not see
                              // it with MHVTL setup).
      char m_reserved2[14];   // Reserved
      char m_aulId[2];        // CASTOR specific for the AUL format - '00'.
      char m_reserved3[28];   // Reserved  
      /**
       * Fills up all common fields of the HDR2 and EOF2 structures
       * with proper values and data provided.
       * 
       * @param blockLength          The CASTOR block size.
       * @param driveHasCompression  The boolean to set If the drive is 
       *                             configured to use compression or not.            
       */
      void fillCommon(int blockLength, bool driveHasCompression);

      /**
       * Throws an exception if the structure does not match expectations.
       */
      void verifyCommon() const ;
    public:
      /**
       * @return    The block length 
       */
      inline std::string getBlockLength() const {
        return toString(m_blockLength);
      }
    };

    class HDR2 : public HDR2EOF2 {
    public:
      /**
       * Fills up a few specific fields fields of the HDR2 structure
       * with proper values and data provided.
       * 
       * @param blockLength  The CASTOR block size.
       * @param driveHasCompression  The boolean to set If the drive is 
       *                             configured to use compression or not.
       *                             By default it is true.
       */
      void fill(int blockLength, bool compression = true);

      /**
       * Throws an exception if the structure does not match expectations.
       */
      void verify() const ;
    };

    class EOF2 : public HDR2EOF2 {
    public:
      /**
       * Fills up only a few specific fields of the HDR2 structure
       * with proper values and data provided.
       * 
       * @param blockLength          The CASTOR block size.
       * @param driveHasCompression  The boolean to set If the drive is 
       *                             configured to use compression or not. 
       *                             By default it si true.
       */
      void fill(int blockLength,  bool compression = true);

      /**
       * Throws an exception if the structure does not match expectations.
       */
      void verify() const ;
    };

    // The common part of the UHL1 and UTL1 labels

    class UHL1UTL1 {
    public:

      UHL1UTL1() {
        spaceStruct(this);
      }
    protected:
      char m_label[4];              // The characters UHL1. 
      char m_actualfSeq[10];        // The actual file sequence number. 
      char m_actualBlockSize[10];   // The actual block size.
      char m_actualRecordLength[10];// The actual record length.
      char m_site[8];               // The domain name uppercase without level 1.
      char m_moverHost[10];         // The tape mover host name uppercase.
      char m_driveVendor[8];        // The drive manufacturer.
      char m_driveModel[8];         // The first 8 bytes from drive identification
      char m_serialNumber[12];      // The drive serial number.
      /**
       * Fills up all common fields of the UHL1 and UTL1 structure with proper
       * values and data provided.
       * 
       * @param fSeq           The file sequence number.
       * @param blockSize      The block size.
       * @param siteName       The domain name uppercase without level 1 (.ch)
       * @param hostName       The tape mover host name uppercase
       * @param deviceInfo     The structure with the tape drive information.
       */
      void fillCommon(int fSeq,
        int blockSize,
        std::string siteName,
        std::string hostName,
        castor::tape::tapeserver::drive::deviceInfo deviceInfo);
      /**
       * Throws an exception if the common part structure does
       * not match expectations.
       */
      void verifyCommon() const ;
    public:
      /**
       * @return    The block size
       */
      inline std::string getBlockSize() const {
        return toString(m_actualBlockSize);
      }

      /**
       * @return The file sequence number
       */
      inline std::string getfSeq() const {
        return toString(m_actualfSeq);
      }
    };

    class UHL1 : public UHL1UTL1 {
    public:
      /**
       * Fills up only specific fields of the UHL1 structure with proper values 
       * and data provided.
       * 
       * @param fSeq           The file sequence number.
       * @param blockSize      The block size.
       * @param siteName       The domain name uppercase without level 1 (.ch)
       * @param hostName       The tape mover host name uppercase
       * @param deviceInfo     The structure with the tape drive information.
       */
      void fill(int fSeq,
        int blockSize,
        std::string siteName,
        std::string hostName,
        castor::tape::tapeserver::drive::deviceInfo deviceInfo);
      /**
       * Throws an exception if the structure does not match expectations.
       */
      void verify() const ;
    };

    class UTL1 : public UHL1UTL1 {
    public:
      /**
       * Fills up only specific fields of the UTL1 structure with proper values 
       * and data provided.
       * 
       * @param fSeq           The file sequence number.
       * @param blockSize      The block size.
       * @param siteName       The domain name uppercase without level 1 (.ch)
       * @param hostName       The tape mover host name uppercase
       * @param deviceInfo     The structure with the tape drive information.
       */
      void fill(int fSeq,
        int blockSize,
        std::string siteName,
        std::string hostName,
        castor::tape::tapeserver::drive::deviceInfo deviceInfo);
      /**
       * Throws an exception if the structure does not match expectations.
       */
      void verify() const ;
    };

    template <size_t n>
    /**
     * Templated helper function to set a space padded string into a
     * fixed sized field of a header structure.
     */
    void setString(char(& t)[n], const std::string & s) {
      size_t written = s.copy(t, n);
      if (written < n) {
        memset(&t[written], ' ', n - written);
      }
    }

    template <size_t n>
    /**
     * Templated helper function to set a '0' padded from left integer into a
     * fixed sized field of a header structure. If the integer is bigger than
     * char array modulus will be used.
     */
    void setInt(char(& t)[n], const int i) {
      char format[6]; // buffer big enough to handle any size
      sprintf(format, "%%.%dd", (int) n);
      char buf[n + 1];
      long mod = 1;
      for (unsigned int j = 0; j < n; j++) mod *= 10;
      sprintf(buf, format, i % mod);

      memcpy(t, buf, n);
    }
    
    template <size_t n>
    /**
     * Templated helper function to compare a string with space padded string.
     *  
     * @return   Returns an integer equal to zero if strings match
     */
    int cmpString(const char(& t)[n], const std::string & s) {
      char forCmp[n];
      setString(forCmp, s);
      return strncmp(forCmp, t, n);
    }
    /**
     * Helper function to set a current date  into a fixed sized field of a
     * header structure. The date format is cyyddd, where:
     * c = century (blank=19; 0=20; 1=21; etc.) 
     * yy= year (00-99) ddd = day (001-366)
     */
    #pragma GCC diagnostic ignored "-Wformat-overflow"
    inline void setDate(char(& t)[6]) {
      time_t current_time;
      struct tm localTime;

      time(&current_time);
      localtime_r(&current_time, &localTime);
      char bufDate[7];
      sprintf(bufDate, "%c%.2d%.3d", localTime.tm_year / 100 ? '0' : ' ',
              localTime.tm_year % 100, localTime.tm_yday + 1);

      memcpy(t, bufDate, 6);      
    }
  } //namespace AULFile
} // namespace tape
} // namespace castor
