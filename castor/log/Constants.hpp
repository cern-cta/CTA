/******************************************************************************
 *                      castor/log/Constants.hpp
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
 * Interface to the CASTOR logging system
 *
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef CASTOR_LOG_CONSTANTS_HPP
#define CASTOR_LOG_CONSTANTS_HPP 1

#include <stdlib.h>

namespace castor {
namespace log {

/**
 * Maximum length of the program name to be prepended to every log message.
 */
const size_t LOG_MAX_PROGNAMELEN = 20;

/**
 * Parameter type: Double precision floating point value.
 */
const int LOG_MSG_PARAM_DOUBLE = 1;

/**
 * Parameter type: 64 bit integer.
 */
const int LOG_MSG_PARAM_INT64 = 2;

/**
 * Parameter type: General purpose string.
 */
const int LOG_MSG_PARAM_STR = 3;

/**
 * Parameter type: Tape volume identifier string.
 */
const int LOG_MSG_PARAM_TPVID = 4;

/**
 * Parameter type: Subrequest identifier.
 */
const int LOG_MSG_PARAM_UUID = 5;

/**
 * Parameter type: Single precision floating point value.
 */
const int LOG_MSG_PARAM_FLOAT = 6;

/**
 * Parameter type: Integer.
 */
const int LOG_MSG_PARAM_INT = 7;

/**
 * Parameter type: User identifier as in getuid().
 */
const int LOG_MSG_PARAM_UID = 8;

/**
 * Parameter type: Group identifier as in getgid().
 */
const int LOG_MSG_PARAM_GID = 9;

/**
 * Parameter type: STYPE?
 */
const int LOG_MSG_PARAM_STYPE = 10;

/**
 * Parameter type: SNAME?
 */
const int LOG_MSG_PARAM_SNAME = 11;

/**
 * Parameter type: Raw (set of) parameters, in key=value format.
 */
const int LOG_MSG_PARAM_RAW = 12;

} // namespace log
} // namespace castor

#endif // CASTOR_LOG_CONSTANTS_HPP
