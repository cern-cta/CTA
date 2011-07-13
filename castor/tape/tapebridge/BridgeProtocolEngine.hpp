/******************************************************************************
 *                      castor/tape/tapebridge/BridgeProtocolEngine.hpp
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

#ifndef CASTOR_TAPE_TAPEBRIDGE_BRIDGEPROTOCOLENGINE
#define CASTOR_TAPE_TAPEBRIDGE_BRIDGEPROTOCOLENGINE

#include "castor/exception/Exception.hpp"
#include "castor/tape/tapebridge/BoolFunctor.hpp"
#include "castor/tape/tapebridge/BridgeSocketCatalogue.hpp"
#include "castor/tape/tapebridge/ConfigParamAndSource.hpp"
#include "castor/tape/tapebridge/Constants.hpp"
#include "castor/tape/tapebridge/Counter.hpp"
#include "castor/tape/legacymsg/CommonMarshal.hpp"
#include "castor/tape/legacymsg/RtcpMarshal.hpp"
#include "castor/tape/tapegateway/Volume.hpp"
#include "castor/tape/utils/IndexedContainer.hpp"
#include "castor/tape/utils/SmartFdList.hpp"
#include "h/Castor_limits.h"
#include "h/Cuuid.h"

#include <map>
#include <stdint.h>


namespace castor     {
namespace tape       {
namespace tapebridge {

/**
 * Acts as a bridge between the tape gatway and rtcpd.
 */
class BridgeProtocolEngine {

public:

  /**
   * Constructor.
   *
   * @param cuuid                    The ccuid to be used for logging.
   * @param listenSock               The socket-descriptor of the listen socket
   *                                 to be used to accept callback connections
   *                                 from rtcpd.
   * @param initialRtcpdSock         The socket-descriptor of the initial rtcpd
   *                                 connection.
   * @param jobRequest               The RTCOPY job request from the VDQM.
   * @param volume                   The volume message received from the
   *                                 client.
   * @param nbFilesOnDestinationTape If migrating and the client is the tape
   *                                 gateway then this must be set to the
   *                                 current number of files on the tape, else
   *                                 this parameter is ignored.
   * @param stoppingGracefully       Functor that returns true if the daemon is
   *                                 stopping gracefully.
   * @param tapebridgeTransactionCounter  The counter used to generate
   *                                 tapebridge transaction IDs.  These are the
   *                                 IDS used in requests to the clients.
   */
  BridgeProtocolEngine(
    const Cuuid_t                       &cuuid,
    const int                           rtcpdListenSock,
    const int                           initialRtcpdSock,
    const legacymsg::RtcpJobRqstMsgBody &jobRequest,
    const tapegateway::Volume           &volume,
    const uint32_t                      nbFilesOnDestinationTape,
    BoolFunctor                         &stoppingGracefully,
    Counter<uint64_t>                   &tapebridgeTransactionCounter) throw();

  /**
   * Run a recall/migration session.
   */
  void run() throw(castor::exception::Exception);

  /**
   * Determines the number of rtcpd disk-IO threads.
   *
   * This method deteremines the required value by first reading the
   * environment variables, then if unsuccessful by reading castor.conf and
   * finally if still unsuccessfull by using the compile-time default.
   *
   * @return The configuration parameter including its source, either
   *         "environment variable", "castor.conf" or "compile-time default".
   */
  ConfigParamAndSource<uint32_t> getNbRtcpdDiskIOThreads()
    throw(castor::exception::Exception);

private:

  /**
   * The cuuid to be used for logging.
   */
  const Cuuid_t &m_cuuid;

  /**
   * The catalogue of all the rtcpd and tapegateway connections used by the
   * bridge protocol engine.
   *
   * The catalogue behaves like a smart pointer for the rtcpd disk/tape IO
   * control-connections and the client connections in that its destructor will
   * close them if they are still open.
   *
   * The catalogue will not close the listen socket used to accept rtcpd
   * connections.  This is the responsibility of the VdqmRequestHandler.
   *
   * The catalogue will not close the initial rtcpd connection.  This is the
   * responsibility of the VdqmRequestHandler.
   */
  BridgeSocketCatalogue m_sockCatalogue;

  /**
   * The RTCOPY job request from the VDQM.
   */
  const legacymsg::RtcpJobRqstMsgBody &m_jobRequest;

  /**
   * The volume message received from the client.
   */
  const tapegateway::Volume &m_volume;

  /**
   * If migrating and the client is the tape-gateway, then this is the next
   * expected tape file sequence, else this member is ignored.
   */
  uint32_t m_nextDestinationFseq;

  /**
   * Functor that returns true if the daemon is stopping gracefully.
   */
  BoolFunctor &m_stoppingGracefully;

  /**
   * The counter used to generate the next tapebridge transaction ID to be used
   * in a request to the client.
   */
  Counter<uint64_t> &m_tapebridgeTransactionCounter;

  /**
   * The number of received RTCP_ENDOF_REQ messages.
   */
  uint32_t m_nbReceivedENDOF_REQs;

  /**
   * Pointer to an rtcpd message body handler function, where the handler
   * function is a member of this class.
   *
   * @param header The header of the rtcpd request.
   * @param socketFd The file descriptor of the socket from which the message
   * should be read from.
   * @param endOfSession Out parameter: Will be set to true by this function
   * if the end of the recall/migration session has been reached.
   */
  typedef void (BridgeProtocolEngine::*RtcpdMsgBodyCallback)(
    const legacymsg::MessageHeader &header, const int socketFd,
    bool &endOfSession);

  /**
   * Datatype for the map of magic+reqType to rtcpd message-body handler.
   */
  typedef std::map<uint64_t, RtcpdMsgBodyCallback> RtcpdCallbackMap;

  /**
   * Map of magic+reqType to rtcpd message-body handler.
   */
  RtcpdCallbackMap m_rtcpdHandlers;

  /**
   * Indexed container of the file transaction IDs of all the files currently
   * being transfered either for recall or migration.
   */
  utils::IndexedContainer<uint64_t> m_pendingTransferIds;

  /**
   * In-line helper function that returns a 64-bit rtcpd message body handler
   * key to be used in the m_rtcpdHandler map.
   *
   * The 64-bit key is generated by combining the specified 32-bit magic
   * number with the specified 32-bit request type.
   *
   * @param magic   The magic number.
   * @param reqType The request type.
   * @return        The 64-bit key.
   */
  uint64_t createRtcpdHandlerKey(uint32_t magic, uint32_t reqType) throw() {

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
   * Pointer to a client message handler function, where the handler function
   * is a member of this class.
   *
   * @param clientSock         The socket-descriptor of the client-connection
   *                           which has already been released from the socket
   *                           catalogue and has already been closed.  The
   *                           value of this parameter is only used for logging
   *                           and the creation of error messages.
   * @param obj                The message object received from the client.
   * @param rtcpdSock          The file descriptor of the rtcpd socket which is
   *                           awaiting a reply.
   * @param rtcpdMagic         The magic number of the initiating rtcpd request.
   *                           This magic number will be used in any acknowledge
   *                           message to be sent to rtcpd.
   * @param rtcpdReqType       The request type of the initiating rtcpd
   *                           request. This request type will be used in any
   *                           acknowledge message to be sent to rtcpd.
   * @param rtcpdTapePath      The tape path associated with the initiating
   *                           rtcpd request.  If there is no such tape path,
   *                           then the value of this parameter should be set
   *                           to NULL.
   * @param tapebridgeTransId  The tapebridge transaction ID associated
   *                           with the request sent to the client.  If no
   *                           request was sent then this parameter should be
   *                           set to 0.
   * @param clientReqTimeStamp The time at which the client request was sent.
   */
  typedef void (BridgeProtocolEngine::*ClientMsgCallback)(
    const int            clientSock,
    const IObject *const obj,
    const int            rtcpdSock,
    const uint32_t       rtcpdMagic,
    const uint32_t       rtcpdReqType,
    const char *const    rtcpdTapePath,
    const uint64_t       tapebridgeTransId,
    const struct timeval clientReqTimeStamp);

  /**
   * Datatype for the map of IObject types to client message handler.
   */
  typedef std::map<int, ClientMsgCallback> ClientCallbackMap;

  /**
   * Map of client message handlers.
   */
  ClientCallbackMap m_clientHandlers;

  /**
   * Processes the rtcpd and client sockets.
   */
  void processSocks() throw(castor::exception::Exception);

  /**
   * Accepts an rtcpd connection using the specified listener socket.
   */
  int acceptRtcpdConnection() throw(castor::exception::Exception);

  /**
   * Processes a pending socket the result of calling select().
   *
   * Please note that this method will modify the catalogue of
   * socket-descriptors as necessary.  For example closed connections will be
   * released from the catalogue and newly accepted connections added.
   *
   * @param readFdSet The read file-descriptor set from calling select().
   * @return          True if the RTCOPY session should continue else false.
   */
  bool processAPendingSocket(fd_set &readFdSet)
    throw (castor::exception::Exception);

  /**
   * Processes the specified rtcpd request.
   *
   * @param header The header of the rtcpd request.
   * @param socketFd The file descriptor of the socket from which the message
   * should be read from.
   * @param receivedENDOF_REQ Out parameter: Will be set to true by this
   * function of an RTCP_ENDOF_REQ was received.
   */
  void processRtcpdRequest(const legacymsg::MessageHeader &header,
    const int socketFd, bool &receivedENDOF_REQ)
    throw(castor::exception::Exception);

  /**
   * RTCP_FILE_REQ rtcpd message-body handler.
   *
   * For full documenation please see the documentation of the type
   * BridgeProtocolEngine::MsgBodyCallback.
   */
  void rtcpFileReqRtcpdCallback(const legacymsg::MessageHeader &header,
    const int socketFd, bool &receivedENDOF_REQ)
    throw(castor::exception::Exception);

  /**
   * RTCP_FILEERR_REQ rtcpd message-body handler.
   *
   * For full documenation please see the documentation of the type
   * BridgeProtocolEngine::MsgBodyCallback.
   */
  void rtcpFileErrReqRtcpdCallback(const legacymsg::MessageHeader &header,
    const int socketFd, bool &receivedENDOF_REQ)
    throw(castor::exception::Exception);

  /**
   * Processes the specified RTCP file request.
   *
   * This function implements the common logic of the rtcpFileReqRtcpdCallback
   * and rtcpFileErrReqRtcpdCallback functions.
   *
   * @param header            The header of the request.
   * @param body              The body of the request which maybe either of type
   *                          RtcpFileRqstMsgBody or RtcpFileRqstErrMsgBody, as
   *                          RtcpFileRqstErrMsgBody inherits from
   *                          RtcpFileRqstMsgBody.
   * @param rtcpdSock         The file descriptor of the socket from which both
   *                          the header and the body of the message have
   *                          already been read from.
   * @param receivedENDOF_REQ Out parameter: Will be set to true by this
   *                          function of an RTCP_ENDOF_REQ was received.
   */
  void processRtcpFileReq(const legacymsg::MessageHeader &header,
    legacymsg::RtcpFileRqstMsgBody &body, const int rtcpdSock, 
    bool &receivedENDOF_REQ) throw(castor::exception::Exception);

  /**
   * RTCP_TAPEREQ rtcpd message-body handler.
   *
   * For full documenation please see the documentation of the type
   * BridgeProtocolEngine::MsgBodyCallback.
   */
  void rtcpTapeReqRtcpdCallback(const legacymsg::MessageHeader &header,
    const int socketFd, bool &receivedENDOF_REQ)
    throw(castor::exception::Exception);

  /**
   * RTCP_TAPEERR rtcpd message-body handler.
   *
   * For full documenation please see the documentation of the type
   * BridgeProtocolEngine::MsgBodyCallback.
   */
  void rtcpTapeErrReqRtcpdCallback(const legacymsg::MessageHeader &header,
    const int socketFd, bool &receivedENDOF_REQ)
    throw(castor::exception::Exception);

  /**
   * Processes the specified RTCP tape request.
   *
   * This function implements the common logic of the rtcpTapeReqRtcpdCallback
   * and rtcpTapeErrReqRtcpdCallback functions.
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
  void processRtcpTape(const legacymsg::MessageHeader &header,
    legacymsg::RtcpTapeRqstMsgBody &body, const int socketFd, 
    bool &receivedENDOF_REQ) throw(castor::exception::Exception);

  /**
   * RTCP_ENDOF_REQ rtcpd message-body handler.
   *
   * For full documenation please see the documentation of the type
   * BridgeProtocolEngine::MsgBodyCallback.
   */
  void rtcpEndOfReqRtcpdCallback(const legacymsg::MessageHeader &header,
    const int socketFd, bool &receivedENDOF_REQ)
    throw(castor::exception::Exception);

  /**
   * TAPEBRIDGE_FLUSHEDTOTAPE tape-bridge message-body handler.
   *
   * For full documenation please see the documentation of the type
   * BridgeProtocolEngine::MsgBodyCallback.
   */
  void tapeBridgeFlushedToTapeCallback(const legacymsg::MessageHeader &header,
    const int socketFd, bool &receivedENDOF_REQ)
    throw(castor::exception::Exception);

  /**
   * GIVE_OUTP rtcpd message-body handler.
   *
   * For full documenation please see the documentation of the type
   * BridgeProtocolEngine::MsgBodyCallback.
   */
  void giveOutpRtcpdCallback(const legacymsg::MessageHeader &header,
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

  /**
   * Throws an exception if the peer host associated with the specified
   * socket is not localhost.
   *
   * @param socketFd The socket file descriptor.
   */
  void checkPeerIsLocalhost(const int socketFd)
    throw(castor::exception::Exception);

  /**
   * FileToMigrate client message handler.
   *
   * For full documenation please see the documentation of the type
   * BridgeProtocolEngine::ClientMsgCallback.
   */
  void fileToMigrateClientCallback(
    const int            clientSock,
    const IObject *const obj,
    const int            rtcpdSock,
    const uint32_t       rtcpdMagic,
    const uint32_t       rtcpdReqType,
    const char *const    rtcpdTapePath,
    const uint64_t       tapebridgeTransId,
    const struct timeval clientReqTimeStamp)
    throw(castor::exception::Exception);

  /**
   * FileToRecall client message handler.
   *
   * For full documenation please see the documentation of the type
   * BridgeProtocolEngine::ClientMsgCallback.
   */
  void fileToRecallClientCallback(
    const int            clientSock,
    const IObject *const obj,
    const int            rtcpdSock,
    const uint32_t       rtcpdMagic,
    const uint32_t       rtcpdReqType,
    const char *const    rtcpdTapePath,
    const uint64_t       tapebridgeTransId,
    const struct timeval clientReqTimeStamp)
    throw(castor::exception::Exception);

  /**
   * NoMoreFiles client message handler.
   *
   * For full documenation please see the documentation of the type
   * BridgeProtocolEngine::ClientMsgCallback.
   */
  void noMoreFilesClientCallback(
    const int            clientSock,
    const IObject *const obj,
    const int            rtcpdSock,
    const uint32_t       rtcpdMagic,
    const uint32_t       rtcpdReqType,
    const char *const    rtcpdTapePath,
    const uint64_t       tapebridgeTransId,
    const struct timeval clientReqTimeStamp)
    throw(castor::exception::Exception);

  /**
   * EndNotificationErrorReport client message handler.
   *
   * For full documenation please see the documentation of the type
   * BridgeProtocolEngine::ClientMsgCallback.
   */
  void endNotificationErrorReportClientCallback(
    const int            clientSock,
    const IObject *const obj,
    const int            rtcpdSock,
    const uint32_t       rtcpdMagic,
    const uint32_t       rtcpdReqType,
    const char *const    rtcpdTapePath,
    const uint64_t       tapebridgeTransId,
    const struct timeval clientReqTimeStamp)
    throw(castor::exception::Exception);

  /**
   * NotificationAcknowledge client message handler.
   *
   * For full documenation please see the documentation of the type
   * BridgeProtocolEngine::ClientMsgCallback.
   */
  void notificationAcknowledge(
    const int            clientSock,
    const IObject *const obj,
    const int            rtcpdSock,
    const uint32_t       rtcpdMagic,
    const uint32_t       rtcpdReqType,
    const char *const    rtcpdTapePath,
    const uint64_t       tapebridgeTransId,
    const struct timeval clientReqTimeStamp)
    throw(castor::exception::Exception);

}; // class BridgeProtocolEngine

} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TAPEBRIDGE_BRIDGEPROTOCOLENGINE
