/******************************************************************************
 *         castor/tape/tapeserver/daemon/DummyVdqm.hpp
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

#include "castor/tape/legacymsg/MessageHeader.hpp"
#include "castor/tape/tapeserver/daemon/Vdqm.hpp"

namespace castor     {
namespace tape       {
namespace tapeserver {
namespace daemon     {

/**
 * A dummy vdqm.
 *
 * The main goal of this class is to facilitate the development of unit tests.
 */
class DummyVdqm: public Vdqm {
public:

  /**
   * Constructor.
   *
   * @param job The vdqm job to be returned by the receiveJob() method.
   */
  DummyVdqm(const legacymsg::RtcpJobRqstMsgBody &job) throw();

  /**
   * Destructor.
   *
   * Closes the listening socket created in the constructor to listen for
   * connections from the vdqmd daemon.
   */
  ~DummyVdqm() throw();

  /**
   * Receives a job from the specified connection with the vdqm daemon,
   * sends back a positive acknowledgement and closes the connection.
   *
   * @param connection The file descriptor of the connection with the vdqm
   * daemon.
   * @return The job request from the vdqm.
   */
  legacymsg::RtcpJobRqstMsgBody receiveJob(const int connection) throw(castor::exception::Exception);

  /**
   * Sets the status of the specified tape drive to down.
   *
   * @param server The host name of the server to which the tape drive is
   * attached.
   * @param unitName The unit name of the tape drive. 
   * @param dgn The device group name of the tape drive.
   */
  void setDriveStatusDown(const std::string &server, const std::string &unitName, const std::string &dgn) throw(castor::exception::Exception);

  /**
   * Sets the status of the specified tape drive to up.
   *
   * @param server The host name of the server to which the tape drive is
   * attached.
   * @param unitName The unit name of the tape drive.
   * @param dgn The device group name of the tape drive.
   */
  void setDriveStatusUp(const std::string &server, const std::string &unitName, const std::string &dgn) throw(castor::exception::Exception);

  /**
   * Sets the status of the specified tape drive to assign.
   *
   * @param server The host name of the server to which the tape drive is
   * attached.
   * @param unitName The unit name of the tape drive.
   * @param dgn The device group name of the tape drive.
   * @param mountTransactionId The mount transaction ID.
   * @param childPid The process ID of the tape-server daemon's mount-session
   * process.
   */
  void assignDrive(const std::string &server, const std::string &unitName, const std::string &dgn, const uint32_t mountTransactionId, const pid_t childPid) throw(castor::exception::Exception);

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
  void tapeMounted(const std::string &server, const std::string &unitName, const std::string &dgn, const std::string &vid, const pid_t sessionPid) throw(castor::exception::Exception);

  /**
   * Sets the status of the specified tape drive to release.
   *
   * @param server The host name of the server to which the tape drive is
   * attached.
   * @param unitName The unit name of the tape drive.
   * @param dgn The device group name of the tape drive.
   * @param forceUnmount Set to true if the current tape mount should not be
   * reused.
   * @param childPid The process ID of the tape-server daemon's mount-session
   * process.
   */
  void releaseDrive(const std::string &server, const std::string &unitName, const std::string &dgn, const bool forceUnmount, const pid_t childPid) throw(castor::exception::Exception);

private:

  /**
   * The vdqm job to be returned by the receiveJob() method.
   */
  legacymsg::RtcpJobRqstMsgBody m_job;

}; // class DummyVdqm

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor

