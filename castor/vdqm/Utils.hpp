/******************************************************************************
 *                      Utils.hpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 *
 *
 *
 * @author Castor dev team
 *****************************************************************************/

#ifndef CASTOR_VDQM_UTILS_HPP
#define CASTOR_VDQM_UTILS_HPP 1


namespace castor {

  namespace vdqm {
  	
    /**
     * Collection of general-purpose static methods.
     */
    class Utils {
    public:

      /**
       * Checks if the specified string is a valid unsigned integer.
       *
       * @param str The string to be checked.
       * @returns true if the string is a valid unsigned integer, else false.
       */
      static bool isAValidUInt(char *str);

      /**
       * Marshalls the specified string into the specified message header/body
       * buffer.
       *
       * @param ptr pointer to where the string should be marshalled
       * @param str the string to be marshalled
       * @return pointer to the first byte after the marshalled string
       */
      static char* marshallString(char *ptr, char *str);

    }; // class Utils

  } // namespace vdqm

} // namespace castor


#endif // CASTOR_VDQM_UTILS_HPP
