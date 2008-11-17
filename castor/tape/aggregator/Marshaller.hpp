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

#include "castor/exception/Exception.hpp"

#include <stdint.h>
#include <string>


namespace castor {
namespace tape {
namespace aggregator {
    
  /**
   * Collection of static methods to marshall / unmarshall network messages.
   */
  class Marshaller {
  public:

    /**
     * Marshalls the specified unsigned 32-bit integer into the specified
     * destination buffer.
     *
     * @param src the unsigned 32-bit integer to be marshalled
     * @param dst in/out parameter, before invocation points to the destination
     * buffer where the unsigned 32-bit integer should be marshalled to and on
     * return points to the byte in the destination buffer immediately after
     * the marshalled unsigned 32-bit integer
     */
    static void marshallUint32(uint32_t src, char * &dst)
      throw(castor::exception::Exception);

    /**
     * Unmarshalls an unsigned 32-bit integer from the specified source buffer
     * into the specified* destination unsigned 32-bit integer.
     *
     * @param src in/out parameter, before invocation points to the source
     * buffer where the unsigned 32-bit integer should be unmarshalled from and
     * on return points to the byte in the source buffer immediately after the
     * unmarshalled unsigned 32-bit integer
     * @param srcLen in/our parameter, before invocation is the length of the
     * source buffer from where the unsigned 32-bit integer should unmarshalled
     * and on return is the number of bytes remaining in the source buffer
     * @param dst the destination unsigned 32-bit integer
     */
    static void unmarshallUint32(char * &src, size_t &srcLen, uint32_t &dst)
      throw(castor::exception::Exception);

    /**
     * Marshalls the specified string into the specified destination buffer.
     *
     * @param src the string to be marshalled
     * @param dst in/out parameter, before invocation points to the destination
     * buffer where the string should be marshalled to and on return points to
     * the byte in the destination buffer immediately after the marshalled
     * string
     */
    static void marshallString(const char *src, char * &dst)
      throw(castor::exception::Exception);

    /**
     * Marshalls the specified string into the specified destination buffer.
     *
     * @param src the string to be marshalled
     * @param dst in/out parameter, before invocation points to the destination
     * buffer where the string should be marshalled to and on return points to
     * the byte in the destination buffer immediately after the marshalled
     * string
     */
    static void marshallString(const std::string &src, char * &dst)
      throw(castor::exception::Exception);

    /**
     * Unmarshalls a string from the specified source buffer into the specified
     * destination buffer.
     *
     * @param src in/out parameter, before invocation points to the source
     * buffer where the string should be unmarshalled from and on return points
     * to the byte in the source buffer immediately after the unmarshalled
     * string
     * @param srcLen in/our parameter, before invocation is the length of the
     * source buffer from where the string should unmarshalled and on return
     * is the number of bytes remaining in the source buffer
     * @param dst in/out parameter, before invocation points to the destination
     * buffer where the string should be unmarshalled to and on return points
     * to the byte in the destination buffer immediately after the unmarshalled
     * string 
     * @param dstLen the length of the destination buffer where the string
     * should unmarshalled to
     */
    static void unmarshallString(char * &src, size_t &srcLen, char *dst,
      const size_t dstLen) throw(castor::exception::Exception);

  }; // class Utils

} // namespace aggregator
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_AGGREGATOR_MARSHALLER_HPP
