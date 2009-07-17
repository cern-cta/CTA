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
#include "castor/tape/aggregator/Constants.hpp"
#include "castor/tape/aggregator/MessageHeader.hpp"
#include "castor/tape/aggregator/RtcpFileRqstMsgBody.hpp"
#include "castor/tape/aggregator/RtcpTapeRqstMsgBody.hpp"
#include "castor/tape/aggregator/SmartFdList.hpp"
#include "castor/tape/utils/IndexedContainer.hpp"
#include "h/Castor_limits.h"
#include "h/Cuuid.h"

#include <map>


namespace castor     {
namespace tape       {
namespace aggregator {

/**
 * Acts as a bridge between the tape gatway and RTCPD.
 */
class BridgeProtocolEngine {

public:

  /**
   * Constructor.
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
  BridgeProtocolEngine(const Cuuid_t &cuuid, const uint32_t volReqId,
    const char (&gatewayHost)[CA_MAXHOSTNAMELEN+1],
    const unsigned short gatewayPort, const int rtcpdCallbackSocketFd,
    const int rtcpdInitialSocketFd, const uint32_t mode,
    char (&unit)[CA_MAXUNMLEN+1], const char (&vid)[CA_MAXVIDLEN+1],
    char (&vsn)[CA_MAXVSNLEN+1], const char (&label)[CA_MAXLBLTYPLEN+1],
    const char (&density)[CA_MAXDENLEN+1]);

  /**
   * Run a recall/migration session.
   */
  void run() throw(castor::exception::Exception);


private:

  /**
   * The cuuid to be used for logging.
   */
  const Cuuid_t &m_cuuid;

  /**
   * The volume request ID.
   */
  const uint32_t m_volReqId;

  /**
   * The tape gateway host name.
   */
  const char (&m_gatewayHost)[CA_MAXHOSTNAMELEN+1];

  /**
   * The tape gateway port number.
   */
  const unsigned short m_gatewayPort;

  /**
   * The file descriptor of the listener socket
   * to be used to accept callback connections from RTCPD.
   */
  const int m_rtcpdCallbackSocketFd;

  /**
   * The socket file descriptor of initial RTCPD
   * connection.
   */
  const int m_rtcpdInitialSocketFd;

  /**
   * The access mode.
   */
  const uint32_t m_mode;

  /**
   * The drive unit.
   */
  char (&m_unit)[CA_MAXUNMLEN+1];

  /**
   * The volume ID.
   */
  const char (&m_vid)[CA_MAXVIDLEN+1];

  /**
   * The volume serial number.
   */
  const char (&m_vsn)[CA_MAXVSNLEN+1];

  /**
   * The volume label.
   */
  const char (&m_label)[CA_MAXLBLTYPLEN+1];

  /**
   * The volume density.
   */
  const char (&m_density)[CA_MAXDENLEN+1];

  /**
   * The set of read RTCPD socket descriptors to be de-multiplexed by
   * the BridgeProtocolEngine.
   */
  SmartFdList m_readFds;

  /**
   * The number of callback connections.
   */
  uint32_t m_nbCallbackConnections;

  /**
   * The number of received RTCP_ENDOF_REQ messages.
   */
  uint32_t m_nbReceivedENDOF_REQs;

  /**
   * Pointer to a message body handler function, where the handler function
   * is a member of this class.
   *
   * @param header The header of the RTCPD request.
   * @param socketFd The file descriptor of the socket from which the message
   * should be read from.
   * @param endOfSession Out parameter: Will be set to true by this function
   * if the end of the recall/migration session has been reached.
   */
  typedef void (BridgeProtocolEngine::*MsgBodyCallback)(
    const MessageHeader &header, const int socketFd, bool &endOfSession);

  /**
   * Datatype for the map of magic+reqType to message body handler.
   */
  typedef std::map<uint64_t, MsgBodyCallback> MsgBodyCallbackMap;

  /**
   * Map of magic+reqType to message body handler.
   */
  MsgBodyCallbackMap m_handlers;

  /**
   * Indexed container of the file transaction IDs of all the files currently
   * being transfered either for recall or migration.
   */
  utils::IndexedContainer<uint64_t, MAXPENDINGTRANSFERS> m_pendingTransferIds;

  /**
   * In-line helper function that returns a 64-bit message body handler key to
   * be used in the m_handler map.
   *
   * The 64-bit key is generated by combining the specified 32-bit magic
   * number with the specified 32-bit request type.
   *
   * @param magic   The magic number.
   * @param reqType The request type.
   * @return The 64-bit key.
   */
  uint64_t createHandlerKey(uint32_t magic, uint32_t reqType) throw() {

    struct TwoUint32 {
      uint32_t a;
      uint32_t b;
    };

    uint64_t  key;
    TwoUint32 *twoUint32 = (TwoUint32*)&key;

    twoUint32->a = reqType;
    twoUint32->b = magic;

    return key;
  }

  /**
   * Accepts an RTCPD connection using the specified listener socket.
   */
  int acceptRtcpdConnection() throw(castor::exception::Exception);

  /**
   * Processes the following RTCPD sockets:
   * <ul>
   * <li>The connected socket of the initial RTCPD connection
   * <li>The RTCPD callback listener socket
   * <li>The connected sockets of the tape and disk I/O threads
   * </ul>
   *
   * @param filesBeingMigrated
   * @param filesBeingRecalled
   */
  void processRtcpdSockets() throw(castor::exception::Exception);

  /**
   * Processes the specified RTCPD socket.
   *
   * @param socketFd The file descriptor of the socket to be processed.
   * @param endOfSession Out parameter: Will be set to true by this function
   * if the end of the recall/migration session has been reached.
   */
  void processRtcpdSocket(const int socketFd, bool &endOfSession)
    throw(castor::exception::Exception);

  /**
   * Processes the specified RTCPD request.
   *
   * @param header The header of the RTCPD request.
   * @param socketFd The file descriptor of the socket from which the message
   * should be read from.
   * @param receivedENDOF_REQ Out parameter: Will be set to true by this
   * function of an RTCP_ENDOF_REQ was received.
   */
  void processRtcpdRequest(const MessageHeader &header, const int socketFd,
    bool &receivedENDOF_REQ) throw(castor::exception::Exception);

  /**
   * RTCP_FILE_REQ message body handler.
   *
   * For full documenation please see the documentation of the type
   * RtcpdBridgeProtocolEngine::MsgBodyCallback.
   */
  void rtcpFileReqCallback(const MessageHeader &header,
    const int socketFd, bool &receivedENDOF_REQ)
    throw(castor::exception::Exception);

  /**
   * RTCP_FILEERR_REQ message body handler.
   *
   * For full documenation please see the documentation of the type
   * RtcpdBridgeProtocolEngine::MsgBodyCallback.
   */
  void rtcpFileErrReqCallback(const MessageHeader &header,
    const int socketFd, bool &receivedENDOF_REQ)
    throw(castor::exception::Exception);

  /**
   * Processes the specified RTCP file request.
   *
   * This function implements the common logic of the rtcpFileReqCallback and
   * rtcpFileErrReqCallback functions.
   *
   * @param header The header of the request.
   * @param body The body of the request which maybe either of type
   * RtcpFileRqstMsgBody or RtcpFileRqstErrMsgBody, as RtcpFileRqstErrMsgBody
   * inherits from RtcpFileRqstMsgBody.
   * @param socketFd The file descriptor of the socket from which both the
   * header and the body of the message have already been read from.
   * @param receivedENDOF_REQ Out parameter: Will be set to true by this
   * function of an RTCP_ENDOF_REQ was received.
   */
  void processRtcpFileReq(const MessageHeader &header,
    RtcpFileRqstMsgBody &body, const int socketFd, bool &receivedENDOF_REQ)
    throw(castor::exception::Exception);

  /**
   * RTCP_TAPEREQ message body handler.
   *
   * For full documenation please see the documentation of the type
   * RtcpdBridgeProtocolEngine::MsgBodyCallback.
   */
  void rtcpTapeReqCallback(const MessageHeader &header,
    const int socketFd, bool &receivedENDOF_REQ)
    throw(castor::exception::Exception);

  /**
   * RTCP_TAPEERR message body handler.
   *
   * For full documenation please see the documentation of the type
   * RtcpdBridgeProtocolEngine::MsgBodyCallback.
   */
  void rtcpTapeErrReqCallback(const MessageHeader &header,
    const int socketFd, bool &receivedENDOF_REQ)
    throw(castor::exception::Exception);

  /**
   * Processes the specified RTCP tape request.
   *
   * This function implements the common logic of the rtcpTapeReqCallback and
   * rtcpTapeErrReqCallback functions.
   *
   * @param header The header of the request.
   * @param body The body of the request which maybe either of type
   * RtcpTapeRqstMsgBody or RtcpTapeRqstErrMsgBody, as RtcpTapeRqstErrMsgBody
   * inherits from RtcpTapeRqstMsgBody.
   * @param socketFd The file descriptor of the socket from which both the
   * header and the body of the message have already been read from.
   * @param receivedENDOF_REQ Out parameter: Will be set to true by this
   * function of an RTCP_ENDOF_REQ was received.
   */
  void processRtcpTape(const MessageHeader &header,
    RtcpTapeRqstMsgBody &body, const int socketFd, bool &receivedENDOF_REQ)
    throw(castor::exception::Exception);

  /**
   * RTCP_ENDOF_REQ message body handler.
   *
   * For full documenation please see the documentation of the type
   * RtcpdBridgeProtocolEngine::MsgBodyCallback.
   */
  void rtcpEndOfReqCallback(const MessageHeader &header,
    const int socketFd, bool &receivedENDOF_REQ)
    throw(castor::exception::Exception);

  /**
   * GIVE_OUTP message body handler.
   *
   * For full documenation please see the documentation of the type
   * RtcpdBridgeProtocolEngine::MsgBodyCallback.
   */
  void giveOutpCallback(const MessageHeader &header,
    const int socketFd, bool &receivedENDOF_REQ)
    throw(castor::exception::Exception);

  /**
   * Runs a migration session.
   */
  void runMigrationSession() throw(castor::exception::Exception);

  /**
   * Runs a recall session.
   */
  void runRecallSession() throw(castor::exception::Exception);
};

} // namespace aggregator
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_AGGREGATOR_BRIDGEPROTOCOLENGINE
