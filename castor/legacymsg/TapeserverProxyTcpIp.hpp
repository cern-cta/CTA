/******************************************************************************
 *         castor/legacymsg/TapeserverProxyTcpIp.hpp
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
 * @author dkruse@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/log/Logger.hpp"
#include "castor/legacymsg/MessageHeader.hpp"
#include "castor/legacymsg/SetVidRequestMsgBody.hpp"
#include "castor/legacymsg/TapeserverProxy.hpp"

namespace castor {
namespace legacymsg {

/**
 * A concrete implementation of the interface to the internal network
 * communications of the tapeserverd daemon.
 */
class TapeserverProxyTcpIp: public TapeserverProxy {
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
  TapeserverProxyTcpIp(log::Logger &log, const unsigned short tapeserverPort, const int netTimeout) throw();

  /**
   * Destructor.
   *
   * Closes the listening socket created in the constructor to listen for
   * connections from the vdqmd daemon.
   */
  ~TapeserverProxyTcpIp() throw();

  /**
   * Sets the VID of the tape mounted in the specified tape drive.
   *
   * @param vid The Volume ID of the tape in the tape drive
   * @param unitName The unit name of the tape drive.
   */
  void setVidInDriveCatalogue(const std::string &vid, const std::string &unitName) throw(castor::exception::Exception);

private:

  /**
   * Connects to the vdqmd daemon.
   *
   * @return The socket-descriptor of the connection with the vdqmd daemon.
   */
  int connectToTapeserver() const throw(castor::exception::Exception);

  /**
   * Writes a drive status message with the specifed contents to the specified
   * connection.
   *
   * @param body The message body defining the drive and status.
   */
  void writeSetVidRequestMsg(const int fd, const legacymsg::SetVidRequestMsgBody &body) throw(castor::exception::Exception);

  /**
   * Reads a VDQM_COMMIT ack message from the specified connection.
   *
   * @param fd The file-descriptor of the connection.
   * @return The message.
   */
  void readSetVidReplyMsg(const int fd) throw(castor::exception::Exception);

  /**
   * The object representing the API of the CASTOR logging system.
   */
  log::Logger &m_log;

  /**
   * The name of the host on which the vdqmd daemon is running.
   */
  const std::string m_tapeserverHostName;

  /**
   * The TCP/IP port on which the vdqmd daemon is listening.
   */
  const unsigned short m_tapeserverPort;
  
  /**
   * The timeout in seconds to be applied when performing network read and
   * write operations.
   */
  const int m_netTimeout;

}; // class TapeserverProxyTcpIp

} // namespace legacymsg
} // namespace castor

