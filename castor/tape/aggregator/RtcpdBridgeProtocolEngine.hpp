/******************************************************************************
 *                castor/tape/aggregator/RtcpdBridgeProtocolEngine.hpp
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

#ifndef CASTOR_TAPE_AGGREGATOR_RTCPDBRIDGEPROTOCOLENGINE_HPP
#define CASTOR_TAPE_AGGREGATOR_RTCPDBRIDGEPROTOCOLENGINE_HPP 1

#include "castor/exception/Exception.hpp"
#include "castor/tape/aggregator/MessageHeader.hpp"
#include "castor/tape/aggregator/RcpJobRqstMsgBody.hpp"
#include "h/Cuuid.h"

#include <map>


namespace castor {
namespace tape {
namespace aggregator {

  /**
   * The RTCPD driven component of the gateway/RTCPD bridge protocol engine.
   */
  class RtcpdBridgeProtocolEngine {

  public:

    /**
     * Constructor
     */
    RtcpdBridgeProtocolEngine() throw();


    /**
     * Datatype for the map of message body handlers.
     */
    enum RunResult {
      REQUEST_PROCESSED=0, /* "" */
      RECEIVED_EOR=1,       /* "" */
      CONNECTION_CLOSED_BYPEER=2   /* "" */
    }; // enum 

    /**
     * Processes the specified request.
     *
     * @param cuuid The cuuid of the request.
     * @param volReqId The volume request ID.
     * @param gatewayHost The tape gateway host name.
     * @param gatewayPort The tape gateway port number.
     * @param mode The tape access mode.
     * @param socketFd The file descriptor of the socket from which the message
     * should be read from.
     * @return True if there is a possibility of more work to do, else false.
     */
    RunResult run(const Cuuid_t &cuuid, const uint32_t volReqId,
      const char (&gatewayHost)[CA_MAXHOSTNAMELEN+1],
      const unsigned short gatewayPort, const uint32_t mode,
      const int socketFd) throw(castor::exception::Exception);

  private:

    /**
     * Pointer to a message body handler function, where the handler function
     * is a member of this class.
     *
     * @param cuuid The cuuid of the request.
     * @param volReqId The volume request ID.
     * @param gatewayHost The tape gateway host name.
     * @param gatewayPort The tape gateway port number.
     * @param mode The tape access mode.
     * @param header The already unmarshalled message header structure.
     * @param socketFd The file descriptor of the socket from which the message
     * body should be read from.
     * @return True if there is a possibility of more work to do, else false.
     */
    typedef RunResult (RtcpdBridgeProtocolEngine::*MsgBodyCallback) (const Cuuid_t &cuuid,
       const uint32_t volReqId, const char (&gatewayHost)[CA_MAXHOSTNAMELEN+1],
       const unsigned short gatewayPort, const uint32_t mode,
       const MessageHeader &header, const int socketFd);

    /**
     * Datatype for the map of message body handlers.
     */
    typedef std::map<uint32_t, MsgBodyCallback> MsgBodyCallbackMap;

    /**
     * Map of message body handlers.
     */
    MsgBodyCallbackMap m_handlers;

    /**
     * RTCP_FILE_REQ message body handler.
     *
     * @param cuuid The cuuid of the request.
     * @param volReqId The volume request ID.
     * @param gatewayHost The tape gateway host name.
     * @param gatewayPort The tape gateway port number.
     * @param mode The tape access mode.
     * @param header The already unmarshalled message header structure.
     * @param socketFd The file descriptor of the socket from which the message
     * body should be read from.
     * @return True if there is a possibility of more work to do, else false.
     */
    RunResult rtcpFileReqCallback(const Cuuid_t &cuuid, const uint32_t volReqId,
      const char (&gatewayHost)[CA_MAXHOSTNAMELEN+1],
      const unsigned short gatewayPort, const uint32_t mode,
      const MessageHeader &header, const int socketFd)
      throw(castor::exception::Exception);

    /**
     * RTCP_FILEERR_REQ message body handler.
     *
     * @param cuuid The cuuid of the request.
     * @param volReqId The volume request ID.
     * @param gatewayHost The tape gateway host name.
     * @param gatewayPort The tape gateway port number.
     * @param mode The tape access mode.
     * @param header The already unmarshalled message header structure.
     * @param socketFd The file descriptor of the socket from which the message
     * body should be read from.
     * @return True if there is a possibility of more work to do, else false.
     */
    RunResult rtcpFileErrReqCallback(const Cuuid_t &cuuid, const uint32_t volReqId,
      const char (&gatewayHost)[CA_MAXHOSTNAMELEN+1],
      const unsigned short gatewayPort, const uint32_t mode,
      const MessageHeader &header, const int socketFd)
      throw(castor::exception::Exception);

    /**
     * RTCP_TAPEREQ message body handler.
     *
     * @param cuuid The cuuid of the request.
     * @param volReqId The volume request ID.
     * @param gatewayHost The tape gateway host name.
     * @param gatewayPort The tape gateway port number.
     * @param mode The tape access mode.
     * @param header The already unmarshalled message header structure.
     * @param socketFd The file descriptor of the socket from which the message
     * body should be read from.
     * @return True if there is a possibility of more work to do, else false.
     */
    RunResult rtcpTapeReqCallback(const Cuuid_t &cuuid, const uint32_t volReqId,
      const char (&gatewayHost)[CA_MAXHOSTNAMELEN+1],
      const unsigned short gatewayPort, const uint32_t mode,
      const MessageHeader &header, const int socketFd)
      throw(castor::exception::Exception);

    /**
     * RTCP_TAPEERR message body handler.
     *
     * @param cuuid The cuuid of the request.
     * @param volReqId The volume request ID.
     * @param gatewayHost The tape gateway host name.
     * @param gatewayPort The tape gateway port number.
     * @param mode The tape access mode.
     * @param header The already unmarshalled message header structure.
     * @param socketFd The file descriptor of the socket from which the message
     * body should be read from.
     * @return True if there is a possibility of more work to do, else false.
     */
    RunResult rtcpTapeErrReqCallback(const Cuuid_t &cuuid, const uint32_t volReqId,
      const char (&gatewayHost)[CA_MAXHOSTNAMELEN+1],
      const unsigned short gatewayPort, const uint32_t mode,
      const MessageHeader &header, const int socketFd)
      throw(castor::exception::Exception);

    /**
     * RTCP_ENDOF_REQ message body handler.
     *
     * @param cuuid The cuuid of the request.
     * @param volReqId The volume request ID.
     * @param gatewayHost The tape gateway host name.
     * @param gatewayPort The tape gateway port number.
     * @param mode The tape access mode.
     * @param header The already unmarshalled message header structure.
     * @param socketFd The file descriptor of the socket from which the message
     * body should be read from.
     * @return True if there is a possibility of more work to do, else false.
     */
    RunResult rtcpEndOfReqCallback(const Cuuid_t &cuuid, const uint32_t volReqId,
      const char (&gatewayHost)[CA_MAXHOSTNAMELEN+1],
      const unsigned short gatewayPort, const uint32_t mode,
      const MessageHeader &header, const int socketFd)
      throw(castor::exception::Exception);

  }; // class RtcpdBridgeProtocolEngine

} // namespace aggregator
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_AGGREGATOR_RTCPDBRIDGEPROTOCOLENGINE_HPP
