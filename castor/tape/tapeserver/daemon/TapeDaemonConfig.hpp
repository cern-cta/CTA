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

#include "castor/log/Logger.hpp"
#include "castor/tape/tapeserver/daemon/DataTransferConfig.hpp"

#include <stdint.h>
#include <string>

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {

/**
 * The contents of the castor.conf file to be used by the tape-server daemon.
 *
 * The pre-extraction of the contents by the caller instead of direct
 * getconfent() calls facilitates unit-testing.
 */
struct TapeDaemonConfig {

  /**
   * The contents of the castor.conf file to be used by the
   * DataTransferSession.
   *
   * The pre-extraction of the contents by the caller instead of direct
   * getconfent() calls facilitates unit-testing.
   */
  DataTransferConfig dataTransferConfig;

  /**
   * The maximum time in seconds that the data-transfer session can take to get
   * the transfer job from the client.
   */
  time_t waitJobTimeoutInSecs;

  /**
   * The maximum time in seconds that the data-transfer session can take to
   * mount a tape.
   */
  time_t mountTimeoutInSecs;

  /**
   * The maximum time in seconds the data-transfer session of tapeserverd can
   * cease to move data blocks
   */
  time_t blockMoveTimeoutInSec;

  /**
   * Constructor that sets all integer member-variables to 0 and all string
   * member-variables to the empty string.
   */
  TapeDaemonConfig() throw();

  /**
   * Returns a configuration structure based on the contents of
   * /etc/castor/castor.conf and compile-time constants.
   *
   * @param log pointer to NULL or an optional logger object.
   * @return The configuration structure.
   */
  static TapeDaemonConfig createFromCastorConf(
    log::Logger *const log = NULL);

}; // TapeDaemonConfig

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor
