/******************************************************************************
 *                      Marshaller.hpp
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

#ifndef CASTOR_TAPE_AGGREGATOR_MARSHALLER_HPP
#define CASTOR_TAPE_AGGREGATOR_MARSHALLER_HPP 1

#include <stdint.h>


namespace castor {
namespace tape {
namespace aggregator {
  	
  /**
   * Collection of static methods to marshall / unmarshall network messages.
   */
  class Marshaller {
  public:

    /**
     * Marshalls the specified string into the specified message buffer.
     *
     * @param ptr pointer to where the value should be marshalled
     * @param value the value to be marshalled
     * @return pointer to the first byte after the marshalled value
     */
    static char* marshallString(char *const ptr, const char *const value)
      throw();

    /**
     * Unmarshalls an unsigned 32 bit integer from the specified buffer.
     *
     * @param ptr pointer to where the integer should be unmarshalled from
     * @param value the unmarshalled value
     * @return pointer to the first byte after the unmarshalled value
     */
    static char* unmarshallUint32(char *const ptr, uint32_t &value) throw();

  }; // class Utils

} // namespace aggregator
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_AGGREGATOR_MARSHALLER_HPP
