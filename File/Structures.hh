// ----------------------------------------------------------------------
// File: File/Structures.hh
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
#include <string>
#include "string.h"
#include "../Drive/Drive.hh"

namespace Tape {
  namespace AULFile {

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
    private:
      char label[4];         // The characters VOL1. 
      char VSN[6];           // The Volume Serial Number. 
      char accessibility[1]; // A space indicates that the volume is authorized.
      char reserved1[13];    // Reserved.
      char implID[13];       // The Implementation Identifier - spaces.
      char ownerID[14];      // CASTOR or stagesuperuser name padded with spaces.
      char reserved2[28];    // Reserved
      char lblStandard[1];   // The label standard level - ASCII 3 for the CASTOR
    public:
      /**
       * Fills up all fields of the VOL1 structure with proper values and data provided.
       * @param VSN the tape serial number
       */
      void fill(std::string _VSN);

      /**
       * @return VSN the tape serial number
       */
      inline std::string getVSN() {
        return toString(VSN);
      }

      /**
       * Throws an exception if the structure does not match expectations.
       */
      void verify();
    };

    // The common part of the HDR1, EOF1 and PRELEBEL HDR1 labels

    class HDR1EOF1 {
    public:

      HDR1EOF1() {
        spaceStruct(this);
      }
    protected:
      char label[4];         // The characters HDR1. 
      char fileId[17];       // The CASTOR NS file Id in ACSII hex or PRELABEL
      char VSN[6];           // The Volume Serial Number.
      char fSec[4];          // The file section number - '0001'.
      char fSeq[4];          // The file sequence number is modulus by 10000.
      char genNum[4];        // The generation number - '0001'.
      char verNumOfGen[2];   // The version number of generation - '00'.
      char creationDate[6];  // The creation date in cyyddd form.
      char expirationDate[6];// The expiration date in cyyddd form.
      char accessibility[1]; // The security status of the data set - space.
      char blockCount[6];    // In the HDR1 is always '000000'.
      char sysCode[13];      // The system ID code - 'CASTOR '+CASTORBASEVERSION
      char reserved[7];      // Reserved
      /**
       * Fills up all common fields of the HDR1, EOF1 and PRELABEL HDR1 
       * structures with proper values and data provided.
       * 
       * @param fileId       The CASTOR NS file Id in ACSII hex.
       * @param VSN          The tape serial number.
       * @param fSeq         The file sequence number on the tape.
       */
      void fillCommon(std::string _fileId, std::string _VSN, int _fSeq);

      /**
       * Throws an exception if the common field of the structures does
       * not match expectations.
       */
      void verifyCommon() const throw (Tape::Exceptions::Errnum);
    public:

      /**
       * @return VSN the tape serial number
       */
      inline std::string getVSN() {
        return toString(VSN);
      }

      /**
       * @return The CASTOR NS file Id
       */
      inline std::string getFileId() {
        return toString(fileId);
      }

      /**
       * @return The file sequence number
       */
      inline std::string getfSeq() {
        return toString(fSeq);
      }
      
      /**
       * @return The number of block written on tape per file
       */
      inline std::string getBlockCount() {
        return toString(blockCount);
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
      void fill(std::string _fileId, std::string _VSN, int _fSeq);

      /**
       * Throws an exception if the structure does not match expectations.
       */
      void verify() const throw (Tape::Exceptions::Errnum);
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
      void fill(std::string _fileId,
        std::string _VSN, int _fSeq, int _blockCount);

      /**
       * Throws an exception if the structure does not match expectations.
       */
      void verify() const throw (Tape::Exceptions::Errnum);
    };
    
    class HDR1PRELABEL : public HDR1EOF1 {
    public:
      /**
       * Fills up only a few fields of the HDR1 structure with proper values 
       * and data provided.
       * 
       * @param VSN          The tape serial number.
       */
      void fill(std::string _VSN);

      /**
       * Throws an exception if the structure does not match expectations.
       */
      void verify() const throw (Tape::Exceptions::Errnum);
    };

    // The common part of the HDR2 and EOF2 labels

    class HDR2EOF2 {
    public:

      HDR2EOF2() {
        spaceStruct(this);
      }
    protected:
      char label[4];        // The characters HDR2. 
      char recordFormat[1]; // The record format is 'F' for the CASTOR AUL. 
      char blockLength[5];  // If it is greater than 100000 it set as '00000'.
      char recordLength[5]; // If it is greater than 100000 it set as '00000'.
      char tapeDensity[1];  // The tape density code. Not used or verified.
      char reserved1[18];   // Reserved
      char recTechnique[2]; // The tape recording technique. 'P ' for xxxGC 
                            // drives (only on the real tapes on we do not see
                            // it with MHVTL setup).
      char reserved2[14];   // Reserved
      char aulId[2];        // CASTOR specific for the AUL format - '00'.
      char reserved3[28];   // Reserved  
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
      void verifyCommon() const throw (Tape::Exceptions::Errnum);
    public:
      /**
       * @return    The block length 
       */
      inline std::string getBlockLength() {
        return toString(blockLength);
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
       *                             By default it si true.
       */
      void fill(int blockLength, bool compression = true);

      /**
       * Throws an exception if the structure does not match expectations.
       */
      void verify() const throw (Tape::Exceptions::Errnum);
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
      void verify() const throw (Tape::Exceptions::Errnum);
    };

    // The common part of the UHL1 and UTL1 labels

    class UHL1UTL1 {
    public:

      UHL1UTL1() {
        spaceStruct(this);
      }
    protected:
      char label[4];              // The characters UHL1. 
      char actualfSeq[10];        // The actual file sequence number. 
      char actualBlockSize[10];   // The actual block size.
      char actualRecordLength[10];// The actual record length.
      char site[8];               // The domain name uppercase without level 1.
      char moverHost[10];         // The tape mover host name uppercase.
      char driveVendor[8];        // The drive manufacturer.
      char driveModel[8];         // The first 8 bytes from drive identification
      char serialNumber[12];      // The drive serial number.
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
        Tape::deviceInfo deviceInfo);
      /**
       * Throws an exception if the common part structure does
       * not match expectations.
       */
      void verifyCommon() const throw (Tape::Exceptions::Errnum);
    public:
      /**
       * @return    The block size
       */
      inline std::string getBlockSize() {
        return toString(actualBlockSize);
      }

      /**
       * @return The file sequence number
       */
      inline std::string getfSeq() {
        return toString(actualfSeq);
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
        Tape::deviceInfo deviceInfo);
      /**
       * Throws an exception if the structure does not match expectations.
       */
      void verify() const throw (Tape::Exceptions::Errnum);
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
        Tape::deviceInfo deviceInfo);
      /**
       * Throws an exception if the structure does not match expectations.
       */
      void verify() const throw (Tape::Exceptions::Errnum);
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
  }
}
