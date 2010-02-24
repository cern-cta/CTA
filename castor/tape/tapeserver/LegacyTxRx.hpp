/******************************************************************************
 *                castor/tape/tapeserver/LegacyTxRx.hpp
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
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef CASTOR_TAPE_TAPESERVER_LEGACYTXRX_HPP
#define CASTOR_TAPE_TAPESERVER_LEGACYTXRX_HPP 1

#include "castor/exception/Exception.hpp"
#include "castor/io/ClientSocket.hpp"
#include "castor/tape/tapeserver/Constants.hpp"
#include "castor/tape/tapeserver/LogHelper.hpp"
#include "castor/tape/legacymsg/RtcpDumpTapeRqstMsgBody.hpp"
#include "castor/tape/legacymsg/CommonMarshal.hpp"
#include "castor/tape/legacymsg/RtcpMarshal.hpp"
#include "castor/tape/legacymsg/MessageHeader.hpp"
#include "castor/tape/net/net.hpp"
#include "castor/tape/utils/utils.hpp"
#include "h/Cuuid.h"

#include <iostream>
#include <stdint.h>


namespace castor     {
namespace tape       {
namespace tapeserver {

/**
 * Provides common functions for sending and receiving the messages of the
 * legacy protocols: RTCOPY and VMGR.
 */
class LegacyTxRx {

public:

  /**
   * Sends the specified message header to RTCPD using the specified socket.
   *
   * @param cuuid The ccuid to be used for logging.
   * @param volReqId The volume request ID to be used for logging.
   * @param socketFd The socket file descriptor of the connection with RTCPD.
   * @param netReadWriteTimeout The timeout to be applied when performing
   * network read and write operations.
   * @param header The message header to be sent.
   */
  static void sendMsgHeader(const Cuuid_t &cuuid,
    const uint32_t volReqId, const int socketFd, const int netReadWriteTimeout,
    const legacymsg::MessageHeader &header) throw(castor::exception::Exception);

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
   * @param cuuid The ccuid to be used for logging.
   * @param volReqId The volume request ID to be used for logging.
   * @param socketFd The socket file descriptor of the connection with RTCPD.
   * @param netReadWriteTimeout The timeout to be applied when performing
   * network read and write operations.
   * @param request The request which will be filled with the contents of the
   * received message.
   */
  static void receiveMsgHeader(const Cuuid_t &cuuid,
    const uint32_t volReqId, const int socketFd, const int netReadWriteTimeout,
    legacymsg::MessageHeader &header) throw(castor::exception::Exception);

  /**
   * Receives a message header or a connection close message.
   *
   * @param cuuid The ccuid to be used for logging.
   * @param connClosed Output parameter: True if the connection was closed by
   * the peer.
   * @param volReqId The volume request ID to be used for logging.
   * @param socketFd The socket file descriptor of the connection with RTCPD.
   * @param netReadWriteTimeout The timeout to be applied when performing
   * network read and write operations.
   * @param request The request which will be filled with the contents of the
   * received message.
   */
  static void receiveMsgHeaderFromCloseable(const Cuuid_t &cuuid,
    bool &connClosed, const uint32_t volReqId, const int socketFd,
    const int netReadWriteTimeout, legacymsg::MessageHeader &header) 
    throw(castor::exception::Exception);


private:

  /**
   * Private constructor to inhibit instances of this class from being
   * instantiated.
   */
  LegacyTxRx() {}

}; // class LegacyTxRx

} // namespace tapeserver
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TAPESERVER_LEGACYTXRX_HPP
