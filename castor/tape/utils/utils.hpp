/******************************************************************************
 *                      castor/tape/aggregator/utils.hpp
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

#ifndef CASTOR_TAPE_AGGREGATOR_UTILS_UTILS_HPP
#define CASTOR_TAPE_AGGREGATOR_UTILS_UTILS_HPP 1

#include "castor/exception/Exception.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/io/ServerSocket.hpp"
#include "castor/tape/Constants.hpp"

#include <errno.h>
#include <stdint.h>
#include <string>
#include <string.h>
#include <unistd.h>
#include <vector>


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
namespace tape   {
namespace utils  {

  /**
   * Returns the size of the specified array.
   *
   * @param array The array whose size is to be determined.
   */
  template<class T, int n> static int arraySize(T (&array)[n]) {
    return n;
  }

  /**
   * Returns the string representation of the specified boolean value.
   *
   * @param value The boolean value.
   */
  const char *boolToString(const bool value);

  /**
   * Splits the specified string into a vector of strings using the specified
   * separator.
   *
   * Please note that the string to be split is NOT modified.
   *
   * @param str The string to be split.
   * @param separator The separator to be used to split the specified string.
   * @param result The vector when the result of spliting the string will be
   * stored.
   */
  void splitString(const std::string &str, const char separator,
    std::vector<std::string> &result) throw();

  /**
   * Returns the number of occurences the specified character appears in the
   * specified string.
   *
   * @param ch The character to be searched for and counted.
   * @param str The string to be seacrhed.
   */
  int countOccurrences(const char ch, const char *str);
  	
  /**
   * Writes the specified unsigned 64-bit integer into the specified
   * destination character array as a hexadecimal string.
   *
   * This function allows user to allocate a character array on the stack and
   * fill it with 64-bit hexadecimal string.
   *
   * @param i The unsigned 64-bit integer.
   * @param dst The destination character array which should have a minimum
   * length of 17 characters.  The largest 64-bit hexadecimal string
   * "FFFFFFFFFFFFFFFF" would ocuppy 17 characters (17 * characters =
   * 16 x 'F' + 1 x '\0').
   * @param dstLen The length of the destination character string.
   */
  void toHex(const uint64_t i, char *dst, size_t dstLen)
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
  template<int n> void toHex(const uint64_t i, char (&dst)[n])
    throw(castor::exception::Exception) {
    toHex(i, dst, n);
  }

  /**
   * Sets all the bytes of the specified object to the value of c.
   *
   * @param object The object whose bytes are to be set.
   * @param c The value to set each byte of object.
   */
  template<typename T> void setBytes(T &object, const int c) throw() {
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
  void copyString(char *const dst, const char *src, const size_t n)
    throw(castor::exception::Exception);

  /**
   * Safely copies source string into destination string.  The destination
   * will always be null terminated if this function is successful.
   *
   * @param dst Destination string.
   * @param src Source string.
   */
  template<int n> void copyString(char (&dst)[n], const char *src)
    throw(castor::exception::Exception) {

    copyString(dst, src, n);
  }

  /**
   * Writes the specified array of strings to the specified output stream as a
   * list of strings separated by the specified separator.
   *
   * @param os The output stream to be written to.
   * @param strings The array of strings to be written to the output stream.
   * @param stringsLen The length of the array of strings, in other words the
   * number of strings.
   * @param separator The separator to be written between the strings.
   */
  void writeStrings(std::ostream &os, const char **strings,
    const int stringsLen, const char *const separator);

  /**
   * Writes the specified array of strings to the specified output stream as a
   * list of strings separated by the specified separator.
   *
   * @param os The output stream to be written to.
   * @param strings The array of strings to be written to the output stream.
   * @param separator The separator to be written between the strings.
   */
  template<int n> void writeStrings(std::ostream &os,
    const char *(&strings)[n], const char *const separator) {

    writeStrings(os, strings, n, separator);
  }

  /**
   * Returns the string representation of the specified magic number.
   *
   * @param magic The magic number.
   */
  const char *magicToString(const uint32_t magic) throw();

  /**
   * Returns the string representation of the specified RTCOPY_MAGIC request
   * type.
   *
   * @param reqType The RTCP request type.
   */
  const char *rtcopyReqTypeToString(const uint32_t reqType) throw();

  /**
   * Returns the string representation of the specified file request status
   * code.
   *
   * @param reqType The file request status code.
   */
  const char *procStatusToString(const uint32_t procStatus) throw();

  /**
   * Checks if the specified string is a valid unsigned integer.
   *
   * @param str The string to be checked.
   * @returns true if the string is a valid unsigned integer, else false.
   */
  bool isValidUInt(const char *str) throw();

  /**
   * Converts the specified string to uppercase.
   */
  void toUpper(char *str);

  /**
   * Drains (reads and discards) all data from the specified file until end of
   * file is reached.
   *
   * @param fd The file descriptor of the file to be drained.
   */
  ssize_t drainFile(const int fd) throw(castor::exception::Exception);

  /**
   * Throws an InvalidArgument exception if the specified identifier string is
   * syntactically incorrect.
   *
   * The indentifier string is valid if each character is either a number (0-9),   * a letter (a-z, A-Z) or an underscore.
   *
   * @param idString The  to be checked.
   */
  void checkIdSyntax(const char *idString)
    throw(castor::exception::InvalidArgument);

  /**
   * Throws an InvalidArgument exception if the specified VID is syntactically
   * invalid.
   *
   * @param vid The VID to be checked.
   */
  void checkVidSyntax(const char *vid)
    throw(castor::exception::InvalidArgument);

  /**
   * Throws an InvalidArgument exception if the specified DGN is syntactically
   * invalid.
   *
   * @param vid The DGN to be checked.
   */
  void checkDgnSyntax(const char *dgn)
    throw(castor::exception::InvalidArgument);

  /**
   * Returns the string representation of the specified CASTOR object type.
   * In the case of the type being unknoen, the returned string is "UNKNOWN".
   */
  const char *objectTypeToString(const unsigned int type);

} // namespace utils
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_AGGREGATOR_UTILS_UTILS_HPP
