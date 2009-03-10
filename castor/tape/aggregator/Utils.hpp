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
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef CASTOR_TAPE_AGGREGATOR_UTILS_HPP
#define CASTOR_TAPE_AGGREGATOR_UTILS_HPP 1

#include "castor/exception/Exception.hpp"

#include <string>

#include <errno.h>
#include <stdint.h>
#include <string.h>


/**
 * The TAPE_THROW_EX macro throws an exception and automatically adds file,
 * line and function strings to the message of the exception.  Example usage:
 *
 * TAPE_THROW_EX(castor::exception::Internal,
 *   ": Failed to down cast reply object to tapegateway::FileToRecall");
 */
#define TAPE_THROW_EX(EXCEPTION, MSG) { \
  EXCEPTION _ex_;                       \
                                        \
  _ex_.getMessage()                     \
    <<  "File="     << __FILE__         \
    << " Line="     << __LINE__         \
    << " Function=" << __FUNCTION__     \
    << MSG;                             \
  throw _ex_;                           \
}


/**
 * The TAPE_THROW_CODE macro throws an exception and automatically adds file,
 * line and function strings to the message of the exception.  Example usage:
 *
 * TAPE_THROW_CODE(EBADMSG,
 *   ": Unknown reply type "
 *   ": Reply type = " << reply->type());
 */
#define TAPE_THROW_CODE(CODE, MSG) {       \
  castor::exception::Exception _ex_(CODE); \
                                           \
  _ex_.getMessage()                        \
    <<  "File="     << __FILE__            \
    << " Line="     << __LINE__            \
    << " Function=" << __FUNCTION__        \
    << MSG       ;                         \
  throw _ex_;                              \
}


namespace castor {
namespace tape {
namespace aggregator {
  	
/**
 * Class of static utility functions.
 */
class Utils {
public:

  /**
   * Writes the specified unsigned 64-bit integer into the specified
   * destination character array as a hexadecimal string.
   *
   * This function allows user to allocate a character array on the stack and
   * fill it with 64-bit hexadecimal string.
   *
   * @param i The unsigned 64-bit integer.
   * @param dst The destination character array.
   * @param dstLen The length of the destination character string.
   */
  static void toHex(const uint64_t i, char *dst, size_t dstLen)
    throw(castor::exception::Exception);

  /**
   * Writes the specified unsigned 64-bit integer into the specified
   * destination character array as a hexadecimal string.
   *
   * This function allows user to allocate a character array on the stack and
   * fill it with 64-bit hexadecimal string.
   *
   * @param i The unsigned 64-bit integer.
   * @param dst The destination character array.
   */
  template<int n> static void toHex(const uint64_t i, char (&dst)[n])
    throw(castor::exception::Exception) {
    toHex(i, dst, n);
  }

  /**
   * Sets all the bytes of the specified object to the value of c.
   *
   * @param object The object whose bytes are to be set.
   * @param c The value to set each byte of object.
   */
  template<typename T> static void setBytes(T &object, const int c) {
    memset(&object, c, sizeof(object));
  }

  /**
   * Safely copies source string into destination string.  The destination
   * will always be null terminated if this function is successful.
   *
   * @param dst Destination string.
   * @param src Source string.
   * @param n   The maximum number of characters to be copied from source to
   * destination.
   */
  static void copyString(char *const dst, const char *src, const size_t n)
    throw(castor::exception::Exception);

  /**
   * Safely copies source string into destination string.  The destination
   * will always be null terminated if this function is successful.
   *
   * @param dst Destination string.
   * @param src Source string.
   */
  template<int n> static void copyString(char (&dst)[n], const char *src)
    throw(castor::exception::Exception) {

    copyString(dst, src, n);
  }

  /**
   * Returns the string representation of the specified magic number.
   *
   * @param magic The magic number.
   */
  static const char *magicToStr(const uint32_t magic);

  /**
   * Returns the string representation of the specified RTCOPY_MAGIC request
   * type.
   *
   * @param reqType The RTCP request type.
   */
  static const char *rtcopyReqTypeToStr(const uint32_t reqType);

  /**
   * Returns the string representation of the specified file request status
   * code.
   *
   * @param reqType The file request status code.
   */
  static const char *procStatusToStr(const uint32_t procStatus);

private:

  /**
   * Private constructor to inhibit instances of this class from being
   * instantiated.
   */
  Utils() {}

}; // class Utils

} // namespace aggregator
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_AGGREGATOR_UTILS_HPP
