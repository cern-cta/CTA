/******************************************************************************
 *                castor/tape/tapeserver/daemon/Vdqm.hpp
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/exception/Exception.hpp"
#include "castor/tape/legacymsg/RtcpJobRqstMsgBody.hpp"

#include <sys/types.h>

namespace castor     {
namespace tape       {
namespace tapeserver {
namespace daemon     {

/**
 * Proxy class representing the vdqm daemon.
 */
class Vdqm {
public:

  /**
   * Destructor.
   */
  virtual ~Vdqm() throw() = 0;

  /**
   * Sets the status of the specified tape drive to down.
   *
   * @param server The host name of the server to which the tape drive is
   * attached.
   * @param unitName The unit name of the tape drive. 
   * @param dgn The device group name of the tape drive.
   */
  virtual void setDriveStatusDown(const std::string &server, const std::string &unitName, const std::string &dgn) throw(castor::exception::Exception) = 0;

  /**
   * Sets the status of the specified tape drive to up.
   *
   * @param server The host name of the server to which the tape drive is
   * attached.
   * @param unitName The unit name of the tape drive.
   * @param dgn The device group name of the tape drive.
   */
  virtual void setDriveStatusUp(const std::string &server, const std::string &unitName, const std::string &dgn) throw(castor::exception::Exception) = 0;

  /**
   * Assigns the specified mount session process to the specified tape
   * drive and mount transaction.
   *
   * @param server The host name of the server to which the tape drive is
   * attached.
   * @param unitName The unit name of the tape drive.
   * @param dgn The device group name of the tape drive.
   * @param mountTransactionId The mount transaction ID.
   * @param sessionPid The process ID of the tape-server daemon's mount-session
   * process.
   */
  virtual void assignDrive(const std::string &server, const std::string &unitName, const std::string &dgn, const uint32_t mountTransactionId, const pid_t sessionPid) throw(castor::exception::Exception) = 0;

  /**
   * Notifies the vdqmd daemon of the specified tape mount.
   *
   * @param server The host name of the server to which the tape drive is
   * attached.
   * @param unitName The unit name of the tape drive.
   * @param dgn The device group name of the tape drive.
   * @param vid The volume identifier of the mounted tape.
   * @param sessionPid The process ID of the tape-server daemon's mount-session
   * process.
   */
  virtual void tapeMounted(const std::string &server, const std::string &unitName, const std::string &dgn, const std::string &vid, const pid_t sessionPid) throw(castor::exception::Exception) = 0;

  /**
   * Releases the specified tape drive.
   *
   * @param server The host name of the server to which the tape drive is
   * attached.
   * @param unitName The unit name of the tape drive.
   * @param dgn The device group name of the tape drive.
   * @param forceUnmount Set to true if the current tape mount should not bei
   * reused.
   * @param sessionPid The process ID of the tape-server daemon's mount-session
   * process.
   */
  virtual void releaseDrive(const std::string &server, const std::string &unitName, const std::string &dgn, const bool forceUnmount, const pid_t sessionPid) throw(castor::exception::Exception) = 0;

}; // class Vdqm

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor

