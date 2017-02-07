/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <list>
#include <sstream>
#include <stdint.h>
#include <string>
#include <unistd.h>
#include <vector>

namespace cta {

namespace utils {

  /**
   * Throws an exception if the specified absolute path constains a
   * syntax error.
   *
   * @param path The Absolute path.
   */
  void assertAbsolutePathSyntax(const std::string &path);

  /**
   * Returns the path of the enclosing directory of the specified path.
   *
   * For example:
   *
   * * path="/grandparent/parent/child" would return "/grandparent/parent/"
   * * path="/grandparent/parent" would return "/grandparent/"
   * * path="/grandparent" would return "/"
   * * path="/" would throw an exception
   *
   * @param path The path.
   * @return The path of the enclosing directory.
   */
  std::string getEnclosingPath(const std::string &path);

  /**
   * Returns the name of the enclosed file or directory of the specified path.
   *
   * @param path The path.
   * @return The name of the enclosed file or directory.
   */
  std::string getEnclosedName(const std::string &path);

  /**
   * Returns the names of the enclosed file or directory of each of the
   * specified paths.
   *
   * @param paths The path
   * @return The names of the enclosed file or directory of each of the
   * specified paths.
   */
  std::list<std::string> getEnclosedNames(const std::list<std::string> &paths);

  /**
   * Returns the result of trimming both left and right slashes from the
   * specified string.
   *
   * @param s The string to be trimmed.
   * @return The result of trimming the string.
   */
  std::string trimSlashes(const std::string &s);
  
  /**
   * Returns the result of trimming right slashes from the
   * specified string.
   *
   * @param s The string to be trimmed.
   * @return The result of trimming the string.
   */
  std::string trimFinalSlashes(const std::string &s);

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
  void splitString(const std::string &str, const char separator, std::vector<std::string> &result);
  

  /**
   * Returns the result of trimming both left and right white-space from the
   * specified string.
   *
   * @param s The string to be trimmed.
   * @return The result of trimming the string.
   */
  std::string trimString(const std::string &s) noexcept;
  
  /**
   * Returns a string with an ellipsis in the end if necessary so that the 
   * string plus ellipsis does not exceed the maxSize. The returned string is 
   * identical to s if it fits the maximum size.
   * @param s the string
   * @param maxSize
   * @return the ellipsed string
   */
  std::string postEllipsis(const std::string &s, size_t maxSize);

  /**
   * Returns a string with an ellipsis in the middle if necessary so that the 
   * string plus ellipsis does not exceed the maxSize. The returned string is 
   * identical to s if it fits the maximum size. The parts before and after the
   * ellipsis are evenly distributed, unless beginingSize is set to a non zero 
   * value. 
   * @param s the string
   * @param maxSize
   * @param beginingSize
   * @return the ellipsed string
   */
  
  std::string midEllipsis(const std::string &s, size_t maxSize, size_t beginingSize = 0);
  /**
   * Returns uuid in the form of a string.
   *
   * @return uuid in the form of a string.
   */
  std::string generateUuid();

  /**
   * Returns true if the specified string ends with the specifie character.
   *
   * @param str The string to be tested.
   * @param c The character to be looked for at the end of the string.
   * @return True if the specified string ends with the specified character.
   */
  bool endsWith(const std::string &str, const char c);

  /**
   * Returns the string reprsentation of the specified value.
   *
   * @param value The value whose string representation is to be returned.
   * @return The string reprsentation of the specified value.
   */
  template <typename T> std::string toString(const T& value) {
    std::ostringstream oss;
    oss << value;
    return oss.str();
  }

  /**
   * Creates and returns an std::string which is the result of replacing each
   * occurance of whitespace (a collection of on or more space and tab
   * characters) with a single space character.
   *
   * @param str The original string.
   * @return    The newly created string with single spaces.
   */
  std::string singleSpaceString(const std::string &str) throw();
  
  /**
   * C++ wrapper around the setxtarr() function.
   *
   * @param path The path to file to which the extended attribute belongs.
   * @param name The name of the extended attribute.
   * @param value The value of the extended attribute.
   */
  void setXattr(const std::string &path, const std::string &name, const std::string &value);

  /**
   * C++ wrapper around the getxattr() function.
   *
   * @param path The path to file to which the extended attribute belongs.
   * @param name The name of the extended attribute.
   * @return The value of the extended attribute.
   */
  std::string getXattr(const std::string &path, const std::string &name);

  /**
   * Determines the string representation of the specified error number.
   *
   * Please note this method is thread safe.
   *
   * @param errnoValue The errno value.
   * @return The string representation.
   */
  std::string errnoToString(const int errnoValue) throw();

  /**
   * Converts the specified string to an unsigned integer.
   *
   * @param str The string.
   * @return The unisgned integer.
   */
  uint16_t toUint16(const std::string &str);

  /**
   * Converts the specified string to a uid.
   *
   * @param str The string.
   * @return The uid.
   */
  uid_t toUid(const std::string &str);

  /**
   * Converts the specified string to a gid.
   *
   * @param str The string.
   * @return The gid.
   */
  gid_t toGid(const std::string &str);

  /**
   * Checks if the specified string is a valid unsigned integer.
   *
   * @param str The string to be checked.
   * @returns true if the string is a valid unsigned integer, else false.
   */
  bool isValidUInt(const std::string &str) throw();

  /**
   * Parses the specified string representation of an unsigned 64-bit integer.
   *
   * Please note that "-1" is a valid string and will parse successfully.
   *
   * @return The parsed unsigned 64-bit integer.
   */
  uint64_t toUint64(const std::string &str);

  /**
   * Converts the specified string to uppercase.
   *
   * @param In/out parameter: The string to be converted.
   */
  void toUpper(std::string &str);

  /**
   * Gets the short host name from the system
   * 
   * @return the short host name
   */
  std::string getShortHostname();

  /**
   * Returns the alder32 checksum of the specified buffer.
   *
   * @param buf The buffer.
   * @param len The length of the buffer in bytes.
   * @return the alder32 checksum of the specified buffer.
   */
  uint32_t getAdler32(const uint8_t *buf, const uint32_t len);
  
  /**
   * Returns the alder32 checksum of the specified buffer in string format,
   * CTA style that is with leading 0x, 8 digits with leading 0 and uppercase.
   *
   * @param buf The buffer.
   * @param len The length of the buffer in bytes.
   * @return the alder32 checksum of the specified buffer.
   */
  std::string getAdler32String(const uint8_t *buf, const uint32_t len);

  /**
   * Returns true if the attributes of the current process indicate that it will
   * produce a core dump if it receives a signal whose behaviour is to produce a
   * core dump.
   *
   * This method is implemented using prctl().
   *
   * @return true if the current program is dumpable.
   */
  bool getDumpableProcessAttribute();

  /**
   * Sets the attributes of the current process to indicate hat it will produce a
   * core dump if it receives a signal whose behaviour is to produce a core dump.
   *
   * @param dumpable true if the current program should be dumpable.
   */
  void setDumpableProcessAttribute(const bool dumpable);
  
  /**
   * Returns the hexadecimal dump of the specified memory.
   *
   * @param mem Pointer to the memory to be dumped.
   * @param n The length of the memory to be dumped.
   * @return The hexadecimal dump.
   */
  std::string hexDump(const void *mem, unsigned int n);

  /**
   * Safely copies source string into destination string.  The destination
   * will always be null terminated if this function is successful.
   *
   * @param dst     Destination string.
   * @param dstSize The size of the destination string including the terminating
   *                null character.
   * @param src     Source string.
   * destination.
   */
  void copyString(char *const dst, const size_t dstSize, const std::string &src);

  /**
   * Safely copies source string into destination string.  The destination
   * will always be null terminated if this function is successful.
   *
   * @param dst Destination string.
   * @param src Source string.
   */
  template<size_t dstSize> void copyString(char (&dst)[dstSize],
    const std::string &src) {
    copyString(dst, dstSize, src);
  }

} // namespace utils

} // namespace cta
