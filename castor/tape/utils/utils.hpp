/******************************************************************************
 *                      castor/tape/utils.hpp
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

#ifndef CASTOR_TAPE_TAPEBRIDGE_UTILS_UTILS_HPP
#define CASTOR_TAPE_TAPEBRIDGE_UTILS_UTILS_HPP 1

#include "castor/exception/Exception.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/InvalidConfigEntry.hpp"
#include "castor/exception/InvalidConfiguration.hpp"
#include "castor/io/ServerSocket.hpp"
#include "castor/tape/Constants.hpp"
#include "castor/tape/tapegateway/ClientType.hpp"
#include "castor/tape/tapegateway/VolumeMode.hpp"

#include <errno.h>
#include <list>
#include <pthread.h>
#include <stdint.h>
#include <string>
#include <string.h>
#include <unistd.h>
#include <vector>
#include <sys/types.h>
#include <sys/stat.h>


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
template<class T, int n> static int arraySize(T (&)[n]) {
  return n;
}

/**
 * Writes the specified time to the specified stream using the specified
 * format.
 *
 * @param os     The stream to be written to.
 * @param time   The time as the number of seconds since the Epoch
 *               (00:00:00 UTC, January 1, 1970).
 * @param format The time format specified using the 
 * recognized formatting characters of 'std::strftime'.
 */
void writeTime(std::ostream &os, const time_t time, const char* const format);

/**
 * Returns the string representation of the specified boolean value.
 *
 * @param value The boolean value.
 */
const char *boolToString(const bool value);


/**
 * Writes the hex form of the specified unsigned 32-bit integer into the
 * first 8 bytes of the specified character buffer.  The string termination
 * character '\0' is written to the ninth byte of the output buffer.
 *
 * @param number The 32-bit integer.
 * @param buf    The character buffer that must have a minimum of 9 bytes.
 * @param len    The length of the character buffer.
 */
void toHex(uint32_t number, char *buf, size_t len)
  throw(castor::exception::InvalidArgument);

/**
 * Writes the hex form of the specified unsigned 32-bit integer into the
 * first 8 bytes of the specified character buffer.  The string termination
 * character '\0' is written to the ninth byte of the output buffer.
 *
 * @param number The 32-bit integer.
 * @param buf    The character buffer that must have a minimum of 9 bytes.
 */
template<size_t n> void toHex(uint32_t number, char (&buf)[n])
  throw(castor::exception::InvalidArgument) {
  toHex(number, buf, n);
}

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
 * @param i      The unsigned 64-bit integer.
 * @param dst    The destination character array which should have a minimum
 *               length of 17 characters.  The largest 64-bit hexadecimal
 *               string "FFFFFFFFFFFFFFFF" would ocuppy 17 characters
 *               (17 characters = 16 x 'F' + 1 x '\0').
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
 * code from an RTCP_FILE[ERR]_REQ message.
 *
 * @param reqType The file request status code.
 */
const char *procStatusToString(const uint32_t procStatus) throw();

/**
 * Returns the string representation of the specified tapebridge client type
 * from a tapegateay::Volume message (READ, WRITE or DUMP).
 */
const char *volumeClientTypeToString(const tapegateway::ClientType mode)
  throw();

/**
 * Returns the string representation of the specified volume mode from a
 * tapegateay::Volume message (READ, WRITE or DUMP).
 */
const char *volumeModeToString(const tapegateway::VolumeMode mode) throw();

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
 * Converts the specified string to uppercase.
 */
void toUpper(std::string &str);

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
 * In the case of the type being unknown, the returned string is "UNKNOWN".
 *
 * @param type The type of the CASTOR object.
 */
const char *objectTypeToString(const unsigned int type);

/**
 * Appends each line of the specified file to the specified list of lines.
 * The new-line characters are extracted from the file, but they are not
 * stored in the lines appended to the list.
 *
 * @param filename The filename of the file to be read.
 * @param lines The list to which each line of the file will be appended.
 */
void readFileIntoList(const char *filename, std::list<std::string> &lines)
  throw(castor::exception::Exception);

/**
 * Appends to the specified list the filenames from the "filelist" file with
 * the specified filename.
 *
 * This method:
 * <ul>
 * <li>Trims leading and trailing white space from each line
 * <li>Ignores blank lines with or without white space.
 * <li>Ignores comment lines, i.e. those starting with a '#' after their
 * leading and trailing white space has been trimmed.
 * </ul>
 *
 * @param filename The filename of the "filelist" file.
 * @param list The to which the filenames will be appended.
 */
void parseFileList(const char *filename, std::list<std::string> &list)
  throw (castor::exception::Exception);

/**
 * Creates and returns an std::string which is the result of the stripping
 * the leading and trailing white space from the specified string.  White
 * space is a contiguous sequence of space ' ' and/or a tab '\t' characters.
 *
 * @param str The string to be trimmed.
 * @return    The newly created trimmed string.
 */
std::string trimString(const std::string &str) throw();

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
 * Writes a banner with the specified title text to the specified stream.
 *
 * @param os The stream to be written to.
 * @param title The title text to be written.
 */
void writeBanner(std::ostream &os, const char *const title) throw();

/**
 * Gets and returns the specified port number using getconfent or returns the
 * specified default if getconfent cannot find it.
 *
 * @param catagory    The category of the configuration entry.
 * @param name        The name of the configuration entry.
 * @param defaultPort The default port number.
 */
unsigned short getPortFromConfig(const char *const category,
  const char *const name, const unsigned short defaultPort)
  throw(exception::InvalidConfigEntry, castor::exception::Exception);

/**
 * The data stored in a data-line (as opposed to a comment-line) from a
 * TPCONFIG file (/etc/castor/TPCONFIG).
 */
struct TpconfigLine {
  const std::string mUnitName;
  const std::string mDeviceGroup;
  const std::string mSystemDevice;
  const std::string mDensity;
  const std::string mInitialStatus;
  const std::string mControlMethod;
  const std::string mDevType;

  /**
   * Constructor.
   */
  TpconfigLine(
    std::string &unitName,
    std::string &deviceGroup,
    std::string &systemDevice,
    std::string &density,
    std::string &initialStatus,
    std::string &controlMethod,
    std::string &devType) :
    mUnitName(unitName),
    mDeviceGroup(deviceGroup),
    mSystemDevice(systemDevice),
    mDensity(density),
    mInitialStatus(initialStatus),
    mControlMethod(controlMethod),
    mDevType(devType) {
  }
}; // struct TpconfigLine

/**
 * A list of TPCONFIG data-lines.
 */
typedef std::list<TpconfigLine> TpconfigLines;

/**
 * Parses the specified TPCONFIG file.
 *
 * @param filename The filename of the TPCONFIG file.
 * @param lines    Output parameter: The list of data-lines parsed from the
 *                 TPCONFIG file.
 */
void parseTpconfigFile(const char *const filename, TpconfigLines &lines)
  throw(castor::exception::Exception);

/**
 * Extracts the drive-unit names from the specified list of parsed TPCONFIG
 * data-lines.
 *
 * This method clears the list of drive-unit names before starting extracting
 * the names from the list of parsed TPCONFIG data-lines.
 *
 * @param tpconfigLines The list of parsed TCONFIG data-lines.
 * @param driveNames    Output parameter: The list of extracted drive-unit
 *                      names.
 */
void extractTpconfigDriveNames(const TpconfigLines &tpconfigLines,
  std::list<std::string> &driveNames) throw();

/**
 * C++ wrapper for the standard C function stat().
 *
 * This wrapper throws an exception if the underlying C function encounters
 * an error.
 *
 * @param filename The name of the file for which information should be
 *                 returned.
 * @param buf      Output parameter: The infromation returned about the
 *                 specified file.
 */
void statFile(const char *const filename, struct stat &buf)
  throw(castor::exception::Exception);

/**
 * C++ wrapper function of the C pthread_create() function.  This wrapper
 * converts the return value indicating an error into the throw of a
 * castor::exception::Exception.
 *
 * @param thread       The location where the ID of the created thread will be
 *                     stored.
 * @param attr         The attribute with which the new thread will be created.
 *                     If this parameter is NULL, then the default attributes
 *                     will be used.
 * @param startRoutine The start routine of the thread to be created.
 * @param arg          The argument to be passed to the startRoutine of the
 *                     created thread.
 */
void pthreadCreate(
  pthread_t *const thread,
  const pthread_attr_t *const attr,
  void *(*const startRoutine)(void*),
  void *const arg)
  throw(castor::exception::Exception);

/**
 * C++ wrapper function of the C pthread_join() function.  This wrapper
 * converts the return value indicating an error into the throw of a
 * castor::exception::Exception.
 *
 * @param thread   The thread which the calling thread will wait for.
 * @param valuePtr If not NULL, will be made to point to the location of the
 *                 value set by the target thread calling pthread_exit().
 */
void pthreadJoin(pthread_t thread, void **const valuePtr)
  throw(castor::exception::Exception);


/**
 * C++ wrapper function of the C getconfent() function.  This wrapper
 * converts a return value of null or empty string into an
 * InvalidConfiguration exception.
 *
 * @param category The category of the configuration value to be retrieved.
 * @param name     The name of the configuration value to be retrieved.
 * @return         The value returned bu getconfent().
 */
const char *getMandatoryValueFromConfiguration(const char *const category,
  const char *const name) throw(castor::exception::InvalidConfiguration);


/**
 * Returns true if the specified C string is empty else returns false.
 */
bool isAnEmptyString(const char *const str) throw();

} // namespace utils
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_TAPEBRIDGE_UTILS_UTILS_HPP
