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

#include "castor/tape/tapebridge/ILegacyTxRx.hpp"

namespace castor     {
namespace tape       {
namespace tapebridge {

/**
 * Provides common functions for sending and receiving the messages of the
 * legacy protocols: RTCOPY and VMGR.
 */
class LegacyTxRx: public ILegacyTxRx {

public:

  /**
   * Constructor.
   *
   * @param netReadWriteTimeout The timeout in seconds to be applied when
   *                            performing network read and write operations.
   */
  LegacyTxRx(const int netReadWriteTimeout) throw();

  /**
   * Destructor.
   */
  ~LegacyTxRx() throw();

  /**
   * Returns the timeout in seconds to be applied when performing network read
   * and write operations.
   *
   * @return The timeout in seconds to be applied when performing network read
   *         and write operations.
   */
  int getNetReadWriteTimeout() const throw();

  /**
   * Sends the specified message header to RTCPD using the specified socket.
   *
   * @param socketFd The socket file descriptor of the connection with RTCPD.
   * @param header   The message header to be sent.
   */
  void sendMsgHeader(
    const int                      socketFd,
    const legacymsg::MessageHeader &header)
    ;

  /**
   * Receives a message header.
   *
   * This operation assumes that all of the bytes can be read in.  Failure
   * to read in all the bytes or a closed connection will result in an
   * exception being thrown.
   *
   * If it is normal that the connection can be closed by the peer, for
   * example you are using select, then please use
   * receiveMessageHeaderFromCloseableConn().
   *
   * @param socketFd The socket file descriptor of the connection with RTCPD.
   * @param request  The request which will be filled with the contents of the
   *                 received message.
   */
  void receiveMsgHeader(
    const int                socketFd,
    legacymsg::MessageHeader &header)
    ;

  /**
   * Receives a message header or a connection close message.
   *
   * @param socketFd The socket file-descriptor of the connection with RTCPD.
   * @param request  The request which will be filled with the contents of the
   *                 received message.
   * @return         True if the connection was closed by the peer, else false.
   */
  bool receiveMsgHeaderFromCloseable(
    const int                socketFd,
    legacymsg::MessageHeader &header) 
    ;

private:

  /**
   * The timeout in seconds to be applied when performing network read and
   * write operations.
   */
  int m_netReadWriteTimeout;

}; // class LegacyTxRx

} // namespace tapebridge
} // namespace tape
} // namespace castor

