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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

namespace castor     {
namespace acs        {

/**
 * The default TCP/IP port on which the CASTOR ACS daemon listens for incoming Zmq
 * connections from the tape server.
 */
const unsigned short DEFAULT_ACS_SERVER_INTERNAL_LISTENING_PORT = 54521; 
      
/**
 * Default time to wait in seconds between queries to ACS Library for responses.
 */
const int DEFAULT_ACS_QUERY_LIBRARY_INTERVAL = 10;

/**
 * Default time to wait in seconds for the mount or dismount to conclude.
 */
const int DEFAULT_ACS_COMMAND_TIMEOUT = 610;

/**
 * The maximum ACS sequence number value to be used.
 */
const unsigned short ACS_MAXIMUM_SEQUENCE_NUMBER = 65535;

/**
 * Default timeout for the response command to the ACS library.
 */
const int DEFAULT_ACS_RESPONSE_TIMEOUT = 5;

/**
 * Enumeration of the states of an ACS request.
 */
enum RequestState {
  ACS_REQUEST_TO_EXECUTE,
  ACS_REQUEST_IS_RUNNING,
  ACS_REQUEST_COMPLETED,
  ACS_REQUEST_FAILED,
  ACS_REQUEST_TO_DELETE};

} // namespace acs
} // namespace castor

