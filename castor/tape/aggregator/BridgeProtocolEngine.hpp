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
#include "castor/tape/aggregator/BoolFunctor.hpp"
#include "castor/tape/aggregator/Constants.hpp"
#include "castor/tape/aggregator/MessageHeader.hpp"
#include "castor/tape/aggregator/RcpJobRqstMsgBody.hpp"
#include "castor/tape/aggregator/RtcpFileRqstMsgBody.hpp"
#include "castor/tape/aggregator/RtcpTapeRqstMsgBody.hpp"
#include "castor/tape/aggregator/SmartFdList.hpp"
#include "castor/tape/tapegateway/Volume.hpp"
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
   * @param cuuid               The ccuid to be used for logging.
   * @param rtcpdCallbackSockFd The file descriptor of the listener socket to
   *                            be used to accept callback connections from
   *                            RTCPD.
   * @param rtcpdInitialSockFd  The socket file descriptor of initial RTCPD
   *                            connection.
   * @param jobRequest          The RTCOPY job request from the VDQM.
   * @param volume              The volume message received from the tape
   *                            gateway.
   * @param vsn                 The volume serial number.
   * @param stoppingGracefully  Functor that returns true if the daemon is
   *                            stopping gracefully.
   */
  BridgeProtocolEngine(
    const Cuuid_t           &cuuid,
    const int               rtcpdCallbackSockFd,
    const int               rtcpdInitialSockFd,
    const RcpJobRqstMsgBody &jobRequest,
    tapegateway::Volume     &volume,
    char                    (&vsn)[CA_MAXVSNLEN+1],
    BoolFunctor             &stoppingGracefully) throw();

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
   * The file descriptor of the listener socket
   * to be used to accept callback connections from RTCPD.
   */
  const int m_rtcpdCallbackSockFd;

  /**
   * The socket file descriptor of initial RTCPD
   * connection.
   */
  const int m_rtcpdInitialSockFd;

  /**
   * The RTCOPY job request from the VDQM.
   */
  const RcpJobRqstMsgBody &m_jobRequest;

  /**
   * The volume message received from the tape gateway.
   */
  tapegateway::Volume &m_volume;

  /**
   * The volume serial number.
   */
  const char (&m_vsn)[CA_MAXVSNLEN+1];

  /**
   * Functor that returns true if the daemon is stopping gracefully.
   */
  BoolFunctor &m_stoppingGracefully;

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
   * Processes the RTCPD sockets.
   */
  void processRtcpdSocks() throw(castor::exception::Exception);

  /**
   * Accepts an RTCPD connection using the specified listener socket.
   */
  int acceptRtcpdConnection() throw(castor::exception::Exception);

  /**
   * Processes a pending RTCPD socket given the list of read file descriptors
   * to be checked and the result of calling select().
   *
   * Please note that this method will modify the list of read file descriptors
   * as necessary.  For example closed connection will be removed and newly
   * accepted connections will be added.
   *
   * @param fds   The read file descriptors to be checked.
   * @param fdSet The file descriptor set from calling select().
   * @return      True if the RTCOPY session should continue else false.
   */
  bool processAPendingRtcpdSocket(SmartFdList &fds, fd_set *fdSet)
    throw (castor::exception::Exception);

  /**
   * Finds a file descriptor in a select() file descriptor set that requires
   * attention.
   *
   * @param fds   The file descriptors to be checked.
   * @param fdSet The file descriptor set from calling select().
   * @return      The file descriptor requiring attention or -1 if none was
   *              found.
   */
  int findPendingFd(const std::list<int> &fds, fd_set *fdSet) throw();

  /**
   * The possible return values of processRtcpdSock.  For documentation please
   * see the documentation of processRtcpdSock().
   */
  enum ProcessRtcpdSockResult {
    MSG_PROCESSED,
    PEER_CLOSED,
    END_RECEIVED
  };

  /**
   * Processes the specified RTCPD socket.
   *
   * @param socketFd The file descriptor of the socket to be processed.
   * @return         Returns MSG_PROCESSED if a message was succesfully received
   *                 and processed and it was not the ENDOF_REQ message.
   *                 Returns PEER_CLOSED if the peerc losed the connection.
   *                 Returns END_RECEIVED if the ENDOF_REQ message was received.
   */
  ProcessRtcpdSockResult processRtcpdSock(const int socketFd)
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
   * Runs a recall or dump session.
   */
  void runRecallSession() throw(castor::exception::Exception);

  /**
   * Runs a dump session.
   */
  void runDumpSession() throw(castor::exception::Exception);
};

} // namespace aggregator
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_AGGREGATOR_BRIDGEPROTOCOLENGINE
