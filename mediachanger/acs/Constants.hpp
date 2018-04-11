/*
 * The CERN Tape Archive(CTA) project
 * Copyright(C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *(at your option) any later version.
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

namespace cta     {
//namespace acs        {

/**
 * The default TCP/IP port on which the CASTOR ACS daemon listens for incoming Zmq
 * connections from the tape server.
 */
const unsigned short ACS_PORT = 54521; 
      
/**
 * Default time to wait in seconds between queries to ACS Library for responses.
 */
const int ACS_QUERY_INTERVAL = 10;

/**
 * Default time to wait in seconds for the tape-library command to conclude.
 */
const int ACS_CMD_TIMEOUT = 610;

/**
 * The maximum ACS sequence number value to be used.
 */
const unsigned short ACS_MAX_SEQ = 65535;

/**
 * Default timeout for the response command to the ACS library.
 */
const int ACS_RESPONSE_TIMEOUT = 5;

/**
 * Enumeration of the states of an ACS request.
 */
enum RequestState {
  ACS_REQUEST_TO_EXECUTE,
  ACS_REQUEST_IS_RUNNING,
  ACS_REQUEST_COMPLETED,
  ACS_REQUEST_FAILED,
  ACS_REQUEST_TO_DELETE};

//} // namespace acs
} // namespace cta

