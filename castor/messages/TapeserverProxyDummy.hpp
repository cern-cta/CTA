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

#include "castor/messages/TapeserverProxy.hpp"

namespace castor {
namespace messages {

/**
 * A dummy tapeserverd-proxy.
 */
class TapeserverProxyDummy: public TapeserverProxy {
public:

  /**
   * Notifies the tapeserverd daemon that the mount-session child-process got
   * a recall job from the tapegatewayd daemon.
   *
   * @param vid The tape to be mounted for recall.
   * @param unitName The unit name of the tape drive.
   */
  void gotRecallJobFromTapeGateway(const std::string &vid,
    const std::string &unitName);

  /**
   * Notifies the tapeserverd daemon that the mount-session child-process got
   * a recall job from the readtp command-line tool.
   *
   * @param vid The tape to be mounted for recall.
   * @param unitName The unit name of the tape drive.
   */
  void gotRecallJobFromReadTp(const std::string &vid,
    const std::string &unitName);

  /**
   * Notifies the tapeserverd daemon that the mount-session child-process got
   * a migration job from the tapegatewayd daemon.
   *
   * @param vid The tape to be mounted for recall.
   * @param unitName The unit name of the tape drive.
   * @return The number of files currently stored on the tape as given by the
   * vmgrd daemon.
   */
  uint32_t gotMigrationJobFromTapeGateway(const std::string &vid,
    const std::string &unitName);

  /**
   * Notifies the tapeserverd daemon that the mount-session child-process got
   * a migration job from the writetp command-line tool.
   *
   * @param vid The tape to be mounted for recall.
   * @param unitName The unit name of the tape drive.
   * @return The number of files currently stored on the tape as given by the
   * vmgrd daemon.
   */
  uint32_t gotMigrationJobFromWriteTp(const std::string &vid,
    const std::string &unitName);

  /**
   * Notifies the tapeserverd daemon that the specified tape has been mounted.
   *
   * @param vid The tape to be mounted for recall.
   * @param unitName The unit name of the tape drive.
   */
  void tapeMountedForRecall(const std::string &vid,
    const std::string &unitName);

  /**
   * Notifies the tapeserverd daemon that the specified tape has been mounted.
   *
   * @param vid The tape to be mounted for recall.
   * @param unitName The unit name of the tape drive.
   */
  void tapeMountedForMigration(const std::string &vid,
    const std::string &unitName);

  /**
   * Notifies the tapeserverd daemon that the specified tape is unmounting.
   *
   * @param vid The tape to be mounted for recall.
   * @param unitName The unit name of the tape drive.
   */
  void tapeUnmountStarted(const std::string &vid,
    const std::string &unitName);


  /**
   * Notifies the tapeserverd daemon that the specified tape has been unmounted.
   *
   * @param vid The tape to be mounted for recall.
   * @param unitName The unit name of the tape drive.
   */
  void tapeUnmounted(const std::string &vid,
    const std::string &unitName);

  /**
   * Notifies the tapeserverd daemon that the data-transfer session is still
   * alive and gives an indication of how much data has been moved.
   *
   * @param unitName The unit name of the tape drive.
   * @param nbBlocksMoved Delta value giving the number of blocks moved
   * since the last heartbeat message.
   */
  void notifyHeartbeat(const std::string &unitName,
    const uint64_t nbBlocksMoved);

  /**
   * Notifies the tapeserverd daemon that a label session has encountered the
   * specified error.
   *
   * @param unitName The unit name of the tape drive.
   * @param labelEx The error encountered by the label session.
   */
  void labelError(const std::string &unitName,
    const castor::exception::Exception &labelEx);

}; // class TapeserverProxyDummy

} // namespace messages
} // namespace castor

