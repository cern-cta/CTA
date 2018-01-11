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

//#include "castor/log/Logger.hpp"
#include "common/log/Logger.hpp"

#include <time.h>

namespace castor {
namespace acs    {

/**
 * Structure used to store the CASTOR configuration parameters used by the
 * CASTOR ACS daemon.
 */
struct AcsDaemonConfig {

  /**
   * The TCP/IP port on which the CASTOR ACS daemon listens for incoming Zmq
   * connections from the tape server.
   */
  unsigned short port;

  /**
   * Time to wait in seconds between queries to the tape Library.
   */
  time_t queryInterval; 

  /**
   * The maximum time to wait in seconds for a tape-library command to
   * conclude.
   */
  time_t cmdTimeout;

  /**
   * Constructor that sets all integer member-variables to 0 and all string
   * member-variables to the empty string.
   */
  AcsDaemonConfig() throw();

  /**
   * Returns a configuration structure based on the contents of
   * /etc/castor/castor.conf and compile-time constants.
   *
   * @param log pointer to NULL or an optional logger object.
   * @return The configuration structure.
   */
  static AcsDaemonConfig createFromCastorConf(
    log::Logger *const log = NULL);

}; // struct AcsDaemonConfig

} // namespace acs
} // namespace castor

