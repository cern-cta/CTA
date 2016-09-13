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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/common/CastorConfiguration.hpp"
#include "common/log/Logger.hpp"
#include "castor/tape/tapeserver/daemon/CatalogueConfig.hpp"
#include "castor/tape/tapeserver/daemon/DataTransferConfig.hpp"
#include "castor/tape/tapeserver/daemon/LabelSessionConfig.hpp"

#include <stdint.h>
#include <string>
#include <vector>

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {

/**
 * The contents of the castor.conf file to be used by the tape-server daemon.
 */
struct TapeDaemonConfig {

  /**
   * The CASTOR configuration parameters used by the Catalogue.
   */
  CatalogueConfig catalogueConfig;
  
  /**
   * This is the path to the objectstore backend
   */
  std::string objectStoreBackendPath;

  /**
   * The TCP/IP port on which the rmcd daemon is listening.
   */
  unsigned short rmcPort;

  /**
   * The maximum number of attempts a retriable RMC request should be issued.
   */
  unsigned int rmcMaxRqstAttempts;

  /**
   * The TCP/IP port on which the tape server daemon listens for incoming
   * connections from the VDQM server.
   */
  unsigned short jobPort;

  /**
   * The TCP/IP port on which the tape server daemon listens for incoming
   * connections from the tpconfig admin command.
   */
  unsigned short adminPort;

  /**
   * The TCP/IP port on which the tape server daemon listens for incoming
   * connections from the label command.
   */
  unsigned short labelPort;

  /**
   * The port on which ZMQ sockets will bind for internal communication between
   * forked sessions and the parent tapeserverd process.
   */
  unsigned short internalPort;

  /**
   * The configuration parameters required by a data-transfer session.
   */
  DataTransferConfig dataTransfer;

  /**
   * The configuration parameters required by a label session.
   */
  LabelSessionConfig labelSession;
  
  /**
   * Constructor.
   *
   * Initializes all integer values to 0, all string values to the empty
   * string and all collections to empty.
   */
  TapeDaemonConfig();

  /**
   * Returns a configuration structure based on the contents of
   * /etc/castor/castor.conf and compile-time constants.
   *
   * @param log Pointer to NULL or an optional logger object.
   * @return The configuration structure.
   */
  static TapeDaemonConfig createFromCastorConf(
    cta::log::Logger *const log = NULL);

}; // TapeDaemonConfig

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor
