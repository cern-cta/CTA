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

#include "castor/exception/Exception.hpp"
#include "castor/log/Param.hpp"

#include <stdint.h>
#include <string>
#include <list>

namespace castor {
namespace messages {

/**
 * Abstract class defining the interface to a proxy object representing the
 * internal network interface of the tapeserverd daemon.
 */
class TapeserverProxy {
public:

  /**
   * Destructor.
   */
  virtual ~TapeserverProxy()  = 0;

  /**
   * Notifies the tapeserverd daemon that the mount-session child-process got
   * a recall job from the tapegatewayd daemon.
   *
   * @param vid The tape to be mounted for recall.
   * @param unitName The unit name of the tape drive.
   */
  virtual void gotRecallJobFromTapeGateway(const std::string &vid,
    const std::string &unitName) = 0;

  /**
   * Notifies the tapeserverd daemon that the mount-session child-process got
   * a recall job from the readtp command-line tool.
   *
   * @param vid The tape to be mounted for recall.
   * @param unitName The unit name of the tape drive.
   */
  virtual void gotRecallJobFromReadTp(const std::string &vid,
    const std::string &unitName) = 0;

  /**
   * Notifies the tapeserverd daemon that the mount-session child-process got
   * a migration job from the tapegatewayd daemon.
   *
   * @param vid The tape to be mounted for recall.
   * @param unitName The unit name of the tape drive.
   * @return The number of files currently stored on the tape as given by the
   * vmgrd daemon.
   */
  virtual uint32_t gotMigrationJobFromTapeGateway(const std::string &vid,
    const std::string &unitName) = 0;
  
  /**
   * Notifies the tapeserverd daemon that the mount-session child-process got
   * an archive job from CTA
   *
   * @param vid The tape to be mounted for recall.
   * @param unitName The unit name of the tape drive.
   * @return The number of files currently stored on the tape
   */
  virtual uint32_t gotArchiveJobFromCTA(const std::string &vid,
    const std::string &unitName, const uint32_t nbFiles) = 0;
  
  /**
   * Notifies the tapeserverd daemon that the mount-session child-process got
   * a retrieve job from CTA
   *
   * @param vid The tape to be mounted for recall.
   * @param unitName The unit name of the tape drive.
   */
  virtual void gotRetrieveJobFromCTA(const std::string &vid,
    const std::string &unitName) = 0;

  /**
   * Notifies the tapeserverd daemon that the mount-session child-process got
   * a migration job from the writetp command-line tool.
   *
   * @param vid The tape to be mounted for recall.
   * @param unitName The unit name of the tape drive.
   * @return The number of files currently stored on the tape as given by the
   * vmgrd daemon.
   */
  virtual uint32_t gotMigrationJobFromWriteTp(const std::string &vid,
    const std::string &unitName) = 0;

  /**
   * Notifies the tapeserverd daemon that the specified tape has been mounted.
   *
   * @param vid The tape to be mounted for recall.
   * @param unitName The unit name of the tape drive.
   */
  virtual void tapeMountedForRecall(const std::string &vid,
    const std::string &unitName) = 0;

  /**
   * Notifies the tapeserverd daemon that the specified tape has been mounted.
   *
   * @param vid The tape to be mounted for recall.
   * @param unitName The unit name of the tape drive.
   */
  virtual void tapeMountedForMigration(const std::string &vid,
    const std::string &unitName) = 0;

  /**
   * Notifies the tapeserverd daemon that the specified tape is unmounting.
   *
   * @param vid The tape to be mounted for recall.
   * @param unitName The unit name of the tape drive.
   */
  virtual void tapeUnmountStarted(const std::string &vid,
    const std::string &unitName) = 0;

  /**
   * Notifies the tapeserverd daemon that the specified tape has been unmounted.
   *
   * @param vid The tape to be mounted for recall.
   * @param unitName The unit name of the tape drive.
   */
  virtual void tapeUnmounted(const std::string &vid,
    const std::string &unitName) = 0;

  /**
   * Notifies the tapeserverd daemon that the data-transfer session is still
   * alive and gives an indication of how much data has been moved.
   *
   * @param unitName The unit name of the tape drive.
   * @param nbBytesMoved Delta value giving the number of bytes moved
   * since the last heartbeat message.
   */
  virtual void notifyHeartbeat(const std::string &unitName,
    const uint64_t nbBytesMoved) = 0;
  
  /**
   * Sends a new set of parameters, to be logged by the mother process when the
   * transfer session is over.
   * @param params: a vector of log parameters
   */
  virtual void addLogParams(const std::string &unitName,
    const std::list<castor::log::Param> & params) = 0;
  
  /**
   * Sends a list of parameters to remove from the end of session logging.
   */
  virtual void deleteLogParams(const std::string &unitName,
    const std::list<std::string> & paramNames) = 0;

  /**
   * Notifies the tapeserverd daemon that a label session has encountered the
   * specified error.
   *
   * @param unitName The unit name of the tape drive.
   * @param message The error message.
   */
  virtual void labelError(const std::string &unitName,
    const std::string &message) = 0;

}; // class TapeserverProxy

} // namespace messages
} // namespace castor

