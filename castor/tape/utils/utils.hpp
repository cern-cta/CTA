/******************************************************************************
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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/exception/Exception.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/exception/InvalidConfigEntry.hpp"
#include "castor/exception/InvalidConfiguration.hpp"
#include "castor/io/ServerSocket.hpp"
#include "castor/tape/Constants.hpp"
#include "castor/tape/tapegateway/ClientType.hpp"
#include "castor/tape/tapegateway/VolumeMode.hpp"
#include "castor/tape/utils/DriveConfig.hpp"
#include "castor/tape/utils/DriveConfigMap.hpp"
#include "castor/tape/utils/TpconfigLines.hpp"

#include <errno.h>
#include <list>
#include <pthread.h>
#include <stdint.h>
#include <string>
#include <string.h>
#include <unistd.h>
#include <vector>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>

/**
 * The TAPE_EX_LOCALE macro streams the current locale (file, line and
 * function) into the message stream of the specified
 * castor::exception::Exception.
 */
#define TAPE_EX_LOCALE(EX) {         \
  EX.getMessage()                  \
    <<  "File="     << __FILE__      \
    << " Line="     << __LINE__      \
    << " Function=" << __FUNCTION__; \
}

/**
 * The TAPE_THROW_EX macro throws an exception and automatically adds file,
 * line and function strings to the message of the exception.  Example usage:
 *
 * TAPE_THROW_EX(castor::exception::Exception,
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
 * Appends each line of the specified file to the specified list of lines.
 * The new-line characters are extracted from the file, but they are not
 * stored in the lines appended to the list.
 *
 * An empty line, with or without a delimiting '\n' character will be appended
 * to the list od lines as an empty string.
 *
 * @param filename The filename of the file to be read.
 * @param lines The list to which each line of the file will be appended.
 */
void readFileIntoList(const char *const filename,
  std::list<std::string> &lines) ;

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
 * @param list     The list to which the filenames will be appended.
 */
void parseFileList(const char *filename, std::list<std::string> &list);

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
 * Parses the specified TPCONFIG file.
 *
 * @param filename The filename of the TPCONFIG file.
 * @param lines    Output parameter: Map from the unit name of a tape drive to
 *                 its configuration.
 */
void parseTpconfigFile(const std::string &filename, DriveConfigMap &drives)
  throw(castor::exception::Exception);

/**
 * Parses the specified TPCONFIG file.
 *
 * @param filename The filename of the TPCONFIG file.
 * @param lines    Output parameter: The list of data-lines parsed from the
 *                 TPCONFIG file.
 */
void parseTpconfigFile(const std::string &filename, TpconfigLines &lines);

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
 * C++ wrapper function of the C getconfent() function.  This wrapper
 * converts a return value of null or empty string into an
 * InvalidConfiguration exception.
 *
 * @param category The category of the configuration value to be retrieved.
 * @param name     The name of the configuration value to be retrieved.
 * @return         The value returned bu getconfent().
 */
const char *getMandatoryValueFromConfiguration(const char *const category,
  const char *const name) ;

/**
 * Returns true if the specified C string is empty else returns false.
 */
bool isAnEmptyString(const char *const str) throw();


/**
 * Returns the string representation of the specified tape block-id.
 *
 * @param  blockId0 Block-id part 0.
 * @param  blockId1 Block-id part 1.
 * @param  blockId2 Block-id part 2.
 * @param  blockId3 Block-id part 3.
 * @return The string representation of the block-id.
 */
std::string tapeBlockIdToString(
  const unsigned char blockId0,
  const unsigned char blockId1,
  const unsigned char blockId2,
  const unsigned char blockId3) throw();

/**
 * Appends the specified path to the value of the specified enviornment
 * variable.  If the environment variable does not exist or is set with an
 * empty string, then the specified path simply becomes the value of the
 * environment variable.
 *
 * @param envVarName       The name of the environment variable.
 * @param pathToBeAppended The path to be appended to the value of the
 *                         environment variable.
 */
void appendPathToEnvVar(const std::string &envVarName,
  const std::string &pathToBeAppended);

} // namespace utils
} // namespace tape
} // namespace castor


