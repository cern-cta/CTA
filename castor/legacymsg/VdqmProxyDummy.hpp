/******************************************************************************
 *         castor/legacymsg/VdqmProxyDummy.hpp
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

#include "castor/legacymsg/MessageHeader.hpp"
#include "castor/legacymsg/VdqmProxy.hpp"

namespace castor {
namespace legacymsg {

/**
 * A dummy vdqm proxy.
 *
 * The main goal of this class is to facilitate the development of unit tests.
 */
class VdqmProxyDummy: public VdqmProxy {
public:

  /**
   * Empty constructor. receiveJob will fail in this case (this is for tests).
   */
  VdqmProxyDummy() throw();
  
  /**
   * Constructor.
   *
   * @param job The vdqm job to be returned by the receiveJob() method.
   */
  VdqmProxyDummy(const RtcpJobRqstMsgBody &job) throw();

  /**
   * Destructor.
   *
   * Closes the listening socket created in the constructor to listen for
   * connections from the vdqmd daemon.
   */
  ~VdqmProxyDummy() throw();

  /**
   * Receives a job from the specified connection with the vdqm daemon,
   * sends back a positive acknowledgement and closes the connection.
   *
   * @param connection The file descriptor of the connection with the vdqm
   * daemon.
   * @return The job request from the vdqm.
   */
  RtcpJobRqstMsgBody receiveJob(const int connection) throw(castor::exception::Exception);

  /**
   * Sets the status of the specified tape drive to down.
   *
   * @param server The host name of the server to which the tape drive is
   * attached.
   * @param unitName The unit name of the tape drive. 
   * @param dgn The device group name of the tape drive.
   */
  void setDriveDown(const std::string &server, const std::string &unitName, const std::string &dgn) throw(castor::exception::Exception);

  /**
   * Sets the status of the specified tape drive to up.
   *
   * @param server The host name of the server to which the tape drive is
   * attached.
   * @param unitName The unit name of the tape drive.
   * @param dgn The device group name of the tape drive.
   */
  void setDriveUp(const std::string &server, const std::string &unitName, const std::string &dgn) throw(castor::exception::Exception);

  /**
   * Sets the status of the specified tape drive to assign.
   *
   * @param server The host name of the server to which the tape drive is
   * attached.
   * @param unitName The unit name of the tape drive.
   * @param dgn The device group name of the tape drive.
   * @param mountTransactionId The mount transaction ID.
   * @param sessionPid The process ID of the tape-server daemon's mount-session
   * process.
   */
  void assignDrive(const std::string &server, const std::string &unitName, const std::string &dgn, const uint32_t mountTransactionId, const pid_t sessionPid) throw(castor::exception::Exception);

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
   * @param sessionPid The process ID of the tape-server daemon's mount-session
   * process.
   */
  void releaseDrive(const std::string &server, const std::string &unitName, const std::string &dgn, const bool forceUnmount, const pid_t sessionPid) throw(castor::exception::Exception);

  /**
   * Notifies the vdqmd daemon that the specified tape has been dismounted.
   *
   * @param server The host name of the server to which the tape drive is
   * attached.
   * @param unitName The unit name of the tape drive.
   * @param dgn The device group name of the tape drive.
   * @param vid The volume identifier of the mounted tape.
   */
  void tapeUnmounted(const std::string &server, const std::string &unitName, const std::string &dgn, const std::string &vid) throw(castor::exception::Exception);

private:

  /**
   * The vdqm job to be returned by the receiveJob() method.
   */
  RtcpJobRqstMsgBody m_job;
  /** Did we construct with a job? */
  const bool m_hasJob;

}; // class VdqmProxyDummy

} // namespace legacymsg
} // namespace castor

