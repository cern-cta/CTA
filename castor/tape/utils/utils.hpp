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

namespace castor {
namespace tape   {
namespace utils  {
/**
 * Will  give an hexadecimal dump of the data between mem and mem+n
 * @param mem THe pointer to memory do dump
 * @param n The length of the memory
  * @return The hex dump of the memory
 */
std::string toHexString( const void * mem, unsigned int n );

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
 * @param lines    Output parameter: The list of data-lines parsed from the
 *                 TPCONFIG file.
 */
void parseTpconfigFile(const std::string &filename, TpconfigLines &lines);

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

} // namespace utils
} // namespace tape
} // namespace castor
