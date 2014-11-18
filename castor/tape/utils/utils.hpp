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

} // namespace utils
} // namespace tape
} // namespace castor
