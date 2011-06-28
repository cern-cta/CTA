/******************************************************************************
 *                      castor/tape/tapebridge/BridgeClientInfo2Sender.hpp
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
 *
 * @author Steven.Murray@cern.ch
 *****************************************************************************/
#ifndef CASTOR_TAPE_TAPEBRIDGE_BRIGEINFO2SENDER_HPP
#define CASTOR_TAPE_TAPEBRIDGE_BRIGEINFO2SENDER_HPP

#include "castor/io/AbstractTCPSocket.hpp"
#include "castor/tape/legacymsg/RtcpMarshal.hpp"
#include "h/tapebridge_marshall.h"

namespace castor {
namespace tape {
namespace tapebridge {
    
  /**
   * A helper class for sending client information from the tape-bridge to the
   * rtcpd daemon.
   */
  class BridgeClientInfo2Sender {

  public:

    /**
     * Sends a TAPEBRIDGE_CLIENT info message to the rtcpd daemon and receives
     * the reply.
     * 
     * @param rtcpdHost           The hostname of the rtcpd daemon.
     * @param rtcpdPort           The port number of the rtcpd daemon.
     * @param netReadWriteTimeout The timeout to be used when performing
     *                            network reads and writes.
     * @param msgBody             The body of the message to be sent.
     * @param reply               The reply from rtcpd which may be positive or
     *                            negative.
     */
    static void send(
      const std::string              &rtcpdHost,
      const unsigned int             rtcpdPort,
      const int                      netReadWriteTimeout,
      tapeBridgeClientInfo2MsgBody_t &msgBody,
      legacymsg::RtcpJobReplyMsgBody &reply)
      throw(castor::exception::Exception);    

      
  private:
      
    /**
     * Reads the reply of rtcpd daemon.
     * 
     * @param sock                The socket of the connection to the rtcpd
     *                            daemon.
     * @param netReadWriteTimeout The timeout to be used when performing
     *                            network reads and writes.
     * @param reply               The reply from the rtcpd daemon.
     */
    static void readReply(
      castor::io::AbstractTCPSocket  &sock,
      const int                      netReadWriteTimeout,
      legacymsg::RtcpJobReplyMsgBody &reply)
      throw(castor::exception::Exception);    

  }; // class BridgeClientInfo2Sender

} // namespace tapebridge
} // namespace tape
} // namespace castor      

#endif // CASTOR_TAPE_TAPEBRIDGE_BRIGEINFO2SENDER_HPP
