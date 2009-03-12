/******************************************************************************
 *                      castor/tape/aggregator/BridgeProtocolEngine.hpp
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

#ifndef CASTOR_TAPE_AGGREGATOR_BRIDGEPROTOCOLENGINE
#define CASTOR_TAPE_AGGREGATOR_BRIDGEPROTOCOLENGINE

#include "castor/exception/Exception.hpp"
#include "h/Castor_limits.h"
#include "h/Cuuid.h"


namespace castor     {
namespace tape       {
namespace aggregator {

/**
 * Acts as a bridge between the tape gatway and RTCPD.
 */
class BridgeProtocolEngine {

public:

  /**
   * Act as a bridge between the tape gatway and RTCPD.
   *
   * @param cuuid The ccuid to be used for logging.
   * @param volReqId The volume request ID.
   * @param gatewayHost The tape gateway host name.
   * @param gatewayPort The tape gateway port number.
   * @param rtcpdCallbackSocketFd The file descriptor of the listener socket
   * to be used to accept callback connections from RTCPD.
   * @param rtcpdInitialSocketFd The socket file descriptor of initial RTCPD
   * connection.
   * @param mode The access mode.
   * @param unit The drive unit.
   * @param vid The volume ID.
   * @param vsn The volume serial number.
   * @param label The volume label.
   * @param density The volume density.
   */
  void run(const Cuuid_t &cuuid, const uint32_t volReqId,
    const char (&gatewayHost)[CA_MAXHOSTNAMELEN+1],
    const unsigned short gatewayPort, const int rtcpdCallbackSocketFd,
    const int rtcpdInitialSocketFd, const uint32_t mode,
    char (&unit)[CA_MAXUNMLEN+1], const char (&vid)[CA_MAXVIDLEN+1],
    char (&vsn)[CA_MAXVSNLEN+1], const char (&label)[CA_MAXLBLTYPLEN+1],
    const char (&density)[CA_MAXDENLEN+1]) throw(castor::exception::Exception);


private:

  /**
   * Processes an incomming error message on the initial RTCPD connection.
   *
   * @param cuuid The ccuid to be used for logging.
   * @param volReqId The volume request ID.
   * @param socketFd The socket file descriptor of initial RTCPD connection.
   */
  void processErrorOnInitialRtcpdConnection(const Cuuid_t &cuuid,
    const uint32_t volReqId, const int socketFd)
    throw(castor::exception::Exception);

  /**
   * Accepts an RTCPD connection using the specified listener socket.
   *
   * @param cuuid The ccuid to be used for logging.
   * @param volReqId The volume request ID.
   * @param rtcpdCallbackSocketFd The file descriptor of the listener socket
   * to be used to accept callback connections from RTCPD.
   * @return The file descriptor of the accepted connection.
   */
  int acceptRtcpdConnection(const Cuuid_t &cuuid, const uint32_t volReqId,
    const int rtcpdCallbackSocketFd) throw(castor::exception::Exception);

  /**
   * Processes the following RTCPD sockets:
   * <ul>
   * <li>The connected socket of the initial RTCPD connection
   * <li>The RTCPD callback listener socket
   * <li>The connected sockets of the tape and disk I/O threads
   * </ul>
   *
   * @param cuuid The ccuid to be used for logging.
   * @param volReqId The volume request ID.
   * @param gatewayHost The tape gateway host name.
   * @param gatewayPort The tape gateway port number.
   * @param mode The tape access mode.
   * @param rtcpdCallbackSocketFd The file descriptor of the listener socket
   * to be used to accept callback connections from RTCPD.
   * @param rtcpdInitialSocketFd The socket file descriptor of initial RTCPD
   * connection.
   */
  void processRtcpdSockets(const Cuuid_t &cuuid, const uint32_t volReqId,
    const char (&gatewayHost)[CA_MAXHOSTNAMELEN+1],
    const unsigned short gatewayPort, const uint32_t mode,
    const int rtcpdCallbackSocketFd, const int rtcpdInitialSocketFd)
    throw(castor::exception::Exception);
};

} // namespace aggregator
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_AGGREGATOR_BRIDGEPROTOCOLENGINE
