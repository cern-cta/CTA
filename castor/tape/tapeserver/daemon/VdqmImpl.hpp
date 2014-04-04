/******************************************************************************
 *         castor/tape/tapeserver/daemon/VdqmImpl.hpp
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

#include "castor/log/Logger.hpp"
#include "castor/tape/legacymsg/MessageHeader.hpp"
#include "castor/tape/legacymsg/VdqmDrvRqstMsgBody.hpp"
#include "castor/tape/tapeserver/daemon/Vdqm.hpp"

namespace castor     {
namespace tape       {
namespace tapeserver {
namespace daemon     {

/**
 * A concrete implementation of the interface to the vdqm daemon.
 */
class VdqmImpl: public Vdqm {
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
  VdqmImpl(log::Logger &log, const std::string &vdqmHostName, const unsigned short vdqmPort, const int netTimeout) throw();

  /**
   * Destructor.
   *
   * Closes the listening socket created in the constructor to listen for
   * connections from the vdqmd daemon.
   */
  ~VdqmImpl() throw();

  /**
   * Sets the status of the specified tape drive to down.
   *
   * @param server The host name of the server to which the tape drive is
   * attached.
   * @param unitName The unit name of the tape drive. 
   * @param dgn The device group name of the tape drive.
   */
  void setTapeDriveStatusDown(const std::string &server, const std::string &unitName, const std::string &dgn) throw(castor::exception::Exception);

  /**
   * Sets the status of the specified tape drive to up.
   *
   * @param server The host name of the server to which the tape drive is
   * attached.
   * @param unitName The unit name of the tape drive.
   * @param dgn The device group name of the tape drive.
   */
  void setTapeDriveStatusUp(const std::string &server, const std::string &unitName, const std::string &dgn) throw(castor::exception::Exception);

  /**
   * Sets the status of the specified tape drive to release.
   *
   * @param server The host name of the server to which the tape drive is
   * attached.
   * @param unitName The unit name of the tape drive.
   * @param dgn The device group name of the tape drive.
   * @param forceUnmount Set to true if the current tape mount should not be reused.
   */
  void setTapeDriveStatusRelease(const std::string &server, const std::string &unitName, const std::string &dgn, const bool forceUnmount) throw(castor::exception::Exception);

private:

  /**
   * Sets the status of the specified drive to the specified value.
   *
   * @param server The host name of the server to which the tape drive is
   * attached.
   * @param unitName The unit name of the tape drive. 
   * @param dgn The device group name of the tape drive.
   */
  void setTapeDriveStatus(const std::string &server,
    const std::string &unitName, const std::string &dgn,
    const int status) throw(castor::exception::Exception);

  /**
   * Connects to the vdqmd daemon.
   *
   * @return The socket-descriptor of the connection with the vdqmd daemon.
   */
  int connectToVdqm() const throw(castor::exception::Exception);

  void writeDriveStatusMsg(
    const int connection, const std::string &server,
    const std::string &unitName, const std::string &dgn, const int status)
    throw(castor::exception::Exception);

  /**
   * Reads a VDQM_COMMIT ack message from the specified connection.
   *
   * @param connection The file-descriptor of the connection.
   * @return The message.
   */
  void readCommitAck(const int connection)
    throw(castor::exception::Exception);

  /**
   * Reads an ack message from the specified connection.
   *
   * @param connection The file-descriptor of the connection.
   * @return The message.
   */
  legacymsg::MessageHeader readAck(const int connection)
    throw(castor::exception::Exception);

  /**
   * Reads drive status message from the specified connection and discards it.
   *
   * @param connection The file-descriptor of the connection.
   */
  void readDriveStatusMsg(const int connection)
    throw(castor::exception::Exception);

  /**
   * Reads the header of a drive status message from the specified connection.
   *
   * @param connection The file-descriptor of the connection.
   * @return The message header.
   */
  legacymsg::MessageHeader readDriveStatusMsgHeader(const int connection)
    throw(castor::exception::Exception);

  /**
   * Reads the body of a drive status message from the specified connection.
   * @param connection The file-descriptor of the connection.
   * @return The message body.
   */
  legacymsg::VdqmDrvRqstMsgBody readDriveStatusMsgBody(const int connection,
    const uint32_t bodyLen) throw(castor::exception::Exception);

  /**
   * Writes a VDQM_COMMIT ack message to the specified connection.
   *
   * @param connection The file-descriptor of the connection.
   */
  void writeCommitAck(const int connection)
    throw(castor::exception::Exception);

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

}; // class VdqmImpl

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor

