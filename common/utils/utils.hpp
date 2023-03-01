/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include <list>
#include <sstream>
#include <stdint.h>
#include <string>
#include <unistd.h>
#include <vector>

#include "Regex.hpp"

namespace cta {
namespace utils {

  /**
   * Returns true if the hostname is a valid IPv4 or IPv6 address.
   *
   * The address is checked against two regular expressions adapted from:
   * https://stackoverflow.com/questions/53497/regular-expression-that-matches-valid-ipv6-addresses#17871737
   */
  bool isValidIPAddress(const std::string &address);

  /**
   * Throws an exception if the hostname is not a Fully-Qualified Domain Name (FQDN).
   *
   * This is a simple regex check, we don't check that the name is a valid DNS name.
   *
   * The regex is based on the hostname rules from this post on StackOverflow:
   * https://stackoverflow.com/questions/11809631/fully-qualified-domain-name-validation#20204811
   *
   * Summary:
   * - Hostnames are composed of a series of labels concatenated with dots.
   * - Each label is 1 to 63 characters long, and may contain:
   *      a-z | 0-9 | -
   * - Labels cannot start or end with hyphens (RFC 952)
   * - Labels can start with numbers (RFC 1123)
   * - Max length of ASCII hostname including dots is 253 characters (not counting trailing dot)
   *   (RFC 1035 section 2.3.4., see also https://blogs.msdn.microsoft.com/oldnewthing/20120412-00/?p=7873/)
   * - Underscores are not allowed in hostnames (but are allowed in other DNS types)
   * - We assume that TLD is at least 2 alphabetic characters
   * - We want at least 1 level above TLD
   */
  void assertIsFQDN(const std::string &hostname);

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
  std::string trimString(const std::string &s);

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
   * Returns a string with an ellipsis in the beginning if necessary so that the
   * string plus ellipsis does not exceed the maxSize. The returned string is
   * identical to s if it fits the maximum size.
   * @param s the string
   * @param maxSize
   * @return the ellipsed string
   */
  std::string preEllipsis(const std::string &s, size_t maxSize);

  /**
   * Returns uuid in the form of a string.
   *
   * @return uuid in the form of a string.
   */
  std::string generateUuid();

  /**
   * Returns true if the specified string is a valid uuid.
   *
   * @param str The string to be tested.
   * @return True if the specified string is a valid uuid.
   */
  bool isUuidFormat(const std::string& str);

  /**
   * Returns true if the specified string is a valid hexadecimal number.
   *
   * @param str The string to be tested.
   * @return True if the specified string is a valid hexadecimal number.
   */
  bool isHexadecimalFormat(const std::string& str);

  /**
   * Returns true if the specified string is a valid decimal number.
   *
   * @param str The string to be tested.
   * @return True if the specified string is a valid decimal number.
   */
  bool isDecimalFormat(const std::string& str);

  /**
    * Converts the specified string to a decimal file id or uuid.
    *
    * @param str The string to be tested.
    * @return True if the specified string is a valid disk file id.
    */
  void checkDiskFileID(std::string *diskFileId);

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
  std::string singleSpaceString(const std::string &str);

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
  std::string errnoToString(const int errnoValue);

  /**
   * Converts the specified string to an unsigned integer.
   *
   * @param str The string.
   * @return The unisgned integer.
   */
  uint8_t toUint8(const std::string &str);

  /**
   * Converts the specified string to an unsigned integer.
   *
   * @param str The string.
   * @return The unisgned integer.
   */
  uint16_t toUint16(const std::string &str);

  /**
   * Converts the specified string to an unsigned integer.
   *
   * @param str The string.
   * @return The unsigned integer.
   */
  uint32_t toUint32(const std::string &str);

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
  bool isValidUInt(const std::string &str);

  /**
   * Parses the specified string representation of an unsigned 64-bit integer.
   *
   * Please note that "-1" is a valid string and will parse successfully.
   *
   * @return The parsed unsigned 64-bit integer.
   */
  uint64_t toUint64(const std::string &str);

  /**
   * Checks if the specified string is a valid decimal.
   *
   * @param str The string to be checked.
   * @returns true if the string is a valid decimal, else false.
   */
  bool isValidDecimal(const std::string &str);

  /**
   * Parses the specified string representation of a double.
   *
   * @return The parsed double.
   */
  double toDouble(const std::string &str);

  /**
   * Converts the specified string to uppercase.
   *
   * @param In/out parameter: The string to be converted.
   */
  void toUpper(std::string &str);

  /**
  * Checks if the specified string is uppercase.
  *
  * @param In/out parameter: The string to be checked.
  * @return true if the string is uppercase.
  */
  bool isUpper(const std::string &str);

  /**
   * Converts the specified string to lowercase.
   *
   * @param In/out parameter: The string to be converted.
   */
  void toLower(std::string &str);

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

  /**
   * Returns the current time, in the local time zone, formated for logging.
   * @return the foramtted time.
   */
  std::string getCurrentLocalTime();

  /**
   * Returns the current time formatted according to the string passed in parameter
   * @param format the format of the time the user wants to have as an output
   * The format should be compliant with http://www.cplusplus.com/reference/ctime/strftime/
   * @return the current time formatted according to the string passed in parameter
   */
  std::string getCurrentLocalTime(const std::string & format);

  /**
   * Remove the root:// part of the path passed in parameter
   * and return the corresponding string
   * @param path the xroot path to truncate
   * @return the xroot path truncated
   */
  std::string extractPathFromXrootdPath(const std::string &path);

  /**
   * Performs a search and replace operation on the specified string.
   *
   * @param str In/out parameter which is the string to be modified.
   * @param search The search string.
   * @param replacement The replacement string.
   * @return Number of replacements performed.
   */
  int searchAndReplace(std::string &str, const std::string &search, const std::string& replacement);

  /**
   * Willful segmentation violation (accessing nullptr) ensuring the process will stop hard.
   * This is different from self-killing the process as the signal delivery is not immediate.
   * Here, the CPU will interrupt the process with a memory protection error.
   */
  void segfault(void);

  /**
   * Idempotently append the XRootD parameter to the fileURL
   * If the fileURL is not an XRootD path (does not start with root://),
   * the fileURL will not be changed.
   * If the fileURL already has this parameter, it will not be changed.
   * @param fileURL [input, output]
   * @param parameterName [input]
   * @param value [input]
   */
  void appendParameterXRootFileURL(std::string &fileURL, const std::string &parameterName, const std::string &value);

  /**
   * Applies a reduce function to a list : transform a list of element to one element according
   * to the ReduceOp passed as a parameter
   * @param begin the beginning of the range to reduce
   * @param end the end of the range to reduce
   * @param init the first element that will be used by reduce
   * @param op the operator to use (lambda or functor)
   * @return the element after the reducing of the list is done.
   */
  template<typename InputIt, typename T, typename ReduceOp>
  T reduce(InputIt begin, InputIt end, const T & init, ReduceOp op){
    T ret = init;
    for(InputIt elt = begin; elt != end; elt++){
      ret = op(ret,*elt);
    }
    return ret;
  }

   /**
   * Allows to remove the first c chars from the beginning of a string
   * Example : removePrefix(aaaaaabcdef,'a') -> bcdef
   * @param input the string from which we will remove the chars c from it
   * @param prefixChar the character that we want to remove from the beginning
   * @return a new string with the prefix.
   */
  std::string removePrefix(const std::string & input, char prefixChar);

   /**
   * Returns the value of the environment variable passed in parameter
   * @param variableName the name of the environment variable to be passed
   * @return the value of the environment variable passed in parameter
   * If the environment variable is not set, it will return an empty string
   */
  std::string getEnv(const std::string & variableName);

   /**
   * Transorms a comma separated list to a vector
   * @param commaSeparated the string hat will be transformed to a vector
   * @return the transformed vecor
   */
  std::vector<std::string> commaSeparatedStringToVector(const std::string &commaSeparated);

   /**
   * Converts a number from a decimal number to a hexidecimal number
   * @param decimalNumber The number that will be transformed
   * @return the hexadecimal version of the number
   */
  std::string decimalToHexadecimal(const std::string &decimalNumber);

} // namespace utils

} // namespace cta
