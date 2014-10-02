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

#include "castor/log/Logger.hpp"
#include "castor/messages/AcsProxy.hpp"
#include "castor/messages/Frame.hpp"
#include "castor/messages/ZmqSocketMT.hpp"

namespace castor {
namespace messages {

/**
 * Concrete class providing a ZMQ implementation of an AcsProxy.
 */
class AcsProxyZmq: public AcsProxy {
public:

  /**
   * Constructor.
   *
   * @param log The object representing the API of the CASTOR logging system.
   * @param serverPort The TCP/IP port on which the CASTOR ACS daemon is listening
   * for ZMQ messages.
   * @param zmqContext The ZMQ context.
   */
  AcsProxyZmq(log::Logger &log, const unsigned short serverPort,
    void *const zmqContext) throw();

  /**
   * Request the CASTOR ACS daemon to mount the specifed tape for recall.
   *
   * @param vid The tape to be mounted.
   * @param acs The ACS identifier.
   * @param lsm The LSM identifier.
   * @param panel The panel identifier.
   * @param drive The drive identifier.
   */
  void mountTapeForRecall(const std::string &vid, const uint32_t acs,
    const uint32_t lsm, const uint32_t panel, const uint32_t drive);
  
  /**
   * Request the CASTOR ACS daemon to mount the specifed tape for migration.
   *
   * @param vid The tape to be mounted.
   * @param acs The ACS identifier.
   * @param lsm The LSM identifier.
   * @param panel The panel identifier.
   * @param drive The drive identifier.
   */
  void mountTapeForMigration(const std::string &vid, const uint32_t acs,
    const uint32_t lsm, const uint32_t panel, const uint32_t drive);
  
  /**
   * Request the CASTOR ACS daemon to dismount the specifed tape.
   *
   * @param vid The tape to be mounted.
   * @param acs The ACS identifier.
   * @param lsm The LSM identifier.
   * @param panel The panel identifier.
   * @param drive The drive identifier.
   */
  void dismountTape(const std::string &vid, const uint32_t acs,
    const uint32_t lsm, const uint32_t panel, const uint32_t drive);

private:

  /**
   * The object representing the API of the CASTOR logging system.
   */
  log::Logger &m_log;

  /**
   * The TCP/IP port on which the CASTOR ACS daemon is listening for ZMQ
   * messages.
   */
  const unsigned short m_serverPort;

  /**
   * Socket connecting this proxy the daemon it represents.
   */
  ZmqSocketMT m_serverSocket;

  /**
   * Creates a frame containing a AcsMountTapeForRecall message.
   *
   * @param vid The tape to be mounted.
   * @param acs The ACS identifier.
   * @param lsm The LSM identifier.
   * @param panel The panel identifier.
   * @param drive The drive identifier.
   * @return The frame.
   */
  Frame createAcsMountTapeForRecallFrame(const std::string &vid,
    const uint32_t acs, const uint32_t lsm, const uint32_t panel,
    const uint32_t drive);
  
  /**
   * Creates a frame containing a AcsMountTapeForMigration message.
   *
   * @param vid The tape to be mounted.
   * @param acs The ACS identifier.
   * @param lsm The LSM identifier.
   * @param panel The panel identifier.
   * @param drive The drive identifier.
   * @return The frame.
   */
  Frame createAcsMountTapeForMigrationFrame(const std::string &vid,
    const uint32_t acs, const uint32_t lsm, const uint32_t panel,
    const uint32_t drive);
  
  /**
   * Creates a frame containing a AcsDismountTape message.
   *
   * @param vid The tape to be dismounted.
   * @param acs The ACS identifier.
   * @param lsm The LSM identifier.
   * @param panel The panel identifier.
   * @param drive The drive identifier.
   * @return The frame.
   */
  Frame createAcsDismountTapeFrame(const std::string &vid,
    const uint32_t acs, const uint32_t lsm, const uint32_t panel,
    const uint32_t drive);

}; // class AcsProxyZmq

} // namespace messages
} // namespace castor

