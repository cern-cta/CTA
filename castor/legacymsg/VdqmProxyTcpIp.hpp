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
#include "castor/legacymsg/MessageHeader.hpp"
#include "castor/legacymsg/VdqmDrvRqstMsgBody.hpp"
#include "castor/legacymsg/VdqmProxy.hpp"

namespace castor {
namespace legacymsg {

/**
 * A concrete implementation of the interface to the vdqm daemon.
 */
class VdqmProxyTcpIp: public VdqmProxy {
public:

  /**
   * Constructor.
   *
   * @param log The object representing the API of the CASTOR logging system.
   * @param vdqmHostName The name of the host on which the vdqmd daemon is
   * running.
   * @param vdqmPort The TCP/IP port on which the vdqmd daemon is listening.
   * @param netTimeout The timeout in seconds to be applied when performing
   * network read and write operations.
   */
  VdqmProxyTcpIp(log::Logger &log, const std::string &vdqmHostName, const unsigned short vdqmPort, const int netTimeout) throw();

  /**
   * Destructor.
   *
   * Closes the listening socket created in the constructor to listen for
   * connections from the vdqmd daemon.
   */
  ~VdqmProxyTcpIp() throw();

  /**
   * Sets the status of the specified tape drive to down.
   *
   * @param server The host name of the server to which the tape drive is
   * attached.
   * @param unitName The unit name of the tape drive. 
   * @param dgn The device group name of the tape drive.
   */
  void setDriveDown(const std::string &server, const std::string &unitName, const std::string &dgn);

  /**
   * Sets the status of the specified tape drive to up.
   *
   * @param server The host name of the server to which the tape drive is
   * attached.
   * @param unitName The unit name of the tape drive.
   * @param dgn The device group name of the tape drive.
   */
  void setDriveUp(const std::string &server, const std::string &unitName, const std::string &dgn);

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
  void assignDrive(const std::string &server, const std::string &unitName, const std::string &dgn, const uint32_t mountTransactionId, const pid_t sessionPid);

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
  void tapeMounted(const std::string &server, const std::string &unitName, const std::string &dgn, const std::string &vid, const pid_t sessionPid);

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
  void releaseDrive(const std::string &server, const std::string &unitName,
    const std::string &dgn, const bool forceUnmount, const pid_t sessionPid);

  /**
   * Notifies the vdqmd daemon that the specified tape has been dismounted.
   *
   * @param server The host name of the server to which the tape drive is
   * attached.
   * @param unitName The unit name of the tape drive.
   * @param dgn The device group name of the tape drive.
   * @param vid The volume identifier of the mounted tape.
   */
  void tapeUnmounted(const std::string &server, const std::string &unitName,
    const std::string &dgn, const std::string &vid);

  /**
   * Queries the vdqmd daemon for the status of the specified tape drive.
   *
   * @param unitName The unit name of the tap drive.
   * @param dgn The device group name of the tape drive.
   * @return The status of the tape drive.
   */
  int getDriveStatus(const std::string &unitName, const std::string &dgn);

private:

  /**
   * Sends the specifed request and receives the reply.  This method sends and
   * receives the necessary acknowledge messages.
   *
   * @param rqst The message body defining the request.
   * @return The message body defining the reply.
   */
  VdqmDrvRqstMsgBody sendRqstRecvReply(const VdqmDrvRqstMsgBody &rqst);

  /**
   * Connects to the vdqmd daemon.
   *
   * @return The socket-descriptor of the connection with the vdqmd daemon.
   */
  int connectToVdqm() const ;

  /**
   * Writes a drive status message with the specifed contents to the specified
   * connection.
   *
   * @param body The message body defining the drive and status.
   */
  void writeDriveStatusMsg(const int fd, const VdqmDrvRqstMsgBody &body);

  /**
   * Reads a VDQM_COMMIT ack message from the specified connection.
   *
   * @param fd The file-descriptor of the connection.
   * @return The message.
   */
  void readCommitAck(const int fd);

  /**
   * Reads an ack message from the specified connection.
   *
   * @param fd The file-descriptor of the connection.
   * @return The message.
   */
  MessageHeader readAck(const int fd);

  /**
   * Reads drive status message from the specified connection and discards it.
   *
   * @param fd The file-descriptor of the connection.
   */
  void readDriveStatusMsg(const int fd);

  /**
   * Reads the header of a drive status message from the specified connection.
   *
   * @param fd The file-descriptor of the connection.
   * @return The message header.
   */
  MessageHeader readDriveStatusMsgHeader(const int fd);

  /**
   * Reads the body of a drive status message from the specified connection.
   * @param fd The file-descriptor of the connection.
   * @return The message body.
   */
  VdqmDrvRqstMsgBody readDriveStatusMsgBody(const int fd,
    const uint32_t bodyLen);

  /**
   * Writes a VDQM_COMMIT ack message to the specified connection.
   *
   * @param fd The file-descriptor of the connection.
   */
  void writeCommitAck(const int fd);

  /**
   * The object representing the API of the CASTOR logging system.
   */
  log::Logger &m_log;

  /**
   * The name of the host on which the vdqmd daemon is running.
   */
  const std::string m_vdqmHostName;

  /**
   * The TCP/IP port on which the vdqmd daemon is listening.
   */
  const unsigned short m_vdqmPort;
  
  /**
   * The timeout in seconds to be applied when performing network read and
   * write operations.
   */
  const int m_netTimeout;

}; // class VdqmProxyTcpIp

} // namespace legacymsg
} // namespace castor

