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

namespace Tape {
  namespace AULFile {
    class VOL1 {
    public:
      char label[4];
      char VSN[6];
/* TODO: finish */
      /**
       * Fills up all fields of the VOL1 structure with proper values and data provided.
       * @param VSN the tape serial number
       */
      void fill(std::string _VSN);
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
        memset(t[written],' ', n - written);
      }
    }
  }
}