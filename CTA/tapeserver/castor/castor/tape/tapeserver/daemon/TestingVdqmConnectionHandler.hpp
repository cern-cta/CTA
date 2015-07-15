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
 * Interface to the CASTOR logging system
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/tape/tapeserver/daemon/VdqmConnectionHandler.hpp"

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {

/**
 * Class used to facilitate unit testing by making public one or more of the
 * protected members of its super class.
 */
class TestingVdqmConnectionHandler: public VdqmConnectionHandler {
public:

  /**
   * Constructor.
   *
   * @param fd The file descriptor of the connection with the vdqmd
   * daemon.
   * @param reactor The reactor with which this event handler is registered.
   * @param log The object representing the API of the CASTOR logging system.
   * @param driveCatalogue The catalogue of tape drives controlled by the tape
   * server daemon.
   * @param tapeDaemonConfig The CASTOR configuration parameters to be used by
   * the tape daemon.
   */
  TestingVdqmConnectionHandler(
    const int fd,
    reactor::ZMQReactor &reactor,
    log::Logger &log,
    Catalogue &driveCatalogue,
    const TapeDaemonConfig &tapeDaemonConfig) throw():
    VdqmConnectionHandler(
      fd,
      reactor,
      log,
      driveCatalogue,
      tapeDaemonConfig) {
  }

  using VdqmConnectionHandler::connectionIsFromTrustedVdqmHost;

  using VdqmConnectionHandler::getPeerHostName;

}; // class TestingVdqmConnectionHandler

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor
