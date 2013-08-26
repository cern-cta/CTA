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

namespace Tape {
  namespace AULFile {
    /**
     * Helper template to fill with space a structure. 
     * @param s pointer the struct/class.
     */
    template <typename C>
    void spaceStruct(C * s) {
      memset (s, 0x20, sizeof(C));
    }
    template <size_t n>
    /**
     * Templated helper function to get std::string from the char array
     * padded with spaces.
     * @param t   array pointer to the char array.
     * @return    std::string for the char array
     */
    std::string toString(char(& t)[n]) {
      std::string r;
      r.assign(t,n);
      return r;
    }
    class VOL1 {
    public:
      VOL1() { spaceStruct(this);}
    private:      
      char label[4];        // The characters VOL1. 
      char VSN[6];          // The Volume Serial Number. 
      char accessibility[1];// A space indicates that the volume is authorized.
      char reserved1[13];   // Reserved.
      char implID[13];      // The Implementation Identifier - spaces.
      char ownerID[14];     // CASTOR or stagesuperuser name padded with spaces.
      char reserved2[28];   // Reserved */
      char lblStandart[1];  // The label standart level - ASCII 3 for the CASTOR
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
    template <size_t n>
    /**
     * Templated helper function to set a space padded string into a
     * fixed sized field of a header structure.
     */
    void setString(char(& t)[n], const std::string & s) {
      size_t written = s.copy(t, n);
      if (written < n) {
        memset(&t[written],' ', n - written);
      }
    }
    template <size_t n>
    /**
     * Templated helper function to compare a string with space padded string.
     *  
     * @return   Returns an integer equal to zero if strings match
     */
    int cmpString(char(& t)[n], const std::string & s) {
      char forCmp[n];
      setString(forCmp,s);
      return strncmp(forCmp,t,n);
    }
  }
}
