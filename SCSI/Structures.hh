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
#include <arpa/inet.h>

namespace SCSI {
  namespace Structures {

    /*
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

      char versionDescriptor[8][2];

      unsigned char reserved2[22];
      unsigned char vendorSpecific2[1];
    } inquiryData_t;

    template <size_t n>
    std::string toString(const char(& t)[n]) {
      std::stringstream r;
      r.write(t, std::find(t, t + n, '\0') - t);
      return r.str();
    }

    inline uint32_t toU32(const char(& t)[4])
    {
      /* Like network, SCSI is BigEndian */
      return ntohl (*((uint32_t *) t));
    }
    
    inline uint16_t toU16(const char(& t)[2])
    {
      /* Like network, SCSI is BigEndian */
      return ntohs (*((uint16_t *) t));
    }
  };


};
