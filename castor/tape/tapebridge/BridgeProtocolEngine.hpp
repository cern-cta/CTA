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
#include "castor/tape/tapebridge/BridgeSocketCatalogue.hpp"
#include "castor/tape/tapebridge/BulkRequestConfigParams.hpp"
#include "castor/tape/tapebridge/Constants.hpp"
#include "castor/tape/tapebridge/Counter.hpp"
#include "castor/tape/tapebridge/FileToMigrate.hpp"
#include "castor/tape/tapebridge/FileToRecall.hpp"
#include "castor/tape/tapebridge/FileWrittenNotificationList.hpp"
#include "castor/tape/tapebridge/GetMoreWorkConnection.hpp"
#include "castor/tape/tapebridge/IClientProxy.hpp"
#include "castor/tape/tapebridge/IFileCloser.hpp"
#include "castor/tape/tapebridge/ILegacyTxRx.hpp"
#include "castor/tape/tapebridge/PendingMigrationsStore.hpp"
#include "castor/tape/tapebridge/SessionErrorList.hpp"
#include "castor/tape/tapebridge/TapeFlushConfigParams.hpp"
#include "castor/tape/tapegateway/FileToMigrateStruct.hpp"
#include "castor/tape/tapegateway/FileToRecallStruct.hpp"
#include "castor/tape/legacymsg/CommonMarshal.hpp"
#include "castor/tape/legacymsg/RtcpMarshal.hpp"
#include "castor/tape/tapegateway/Volume.hpp"
#include "castor/tape/utils/BoolFunctor.hpp"
#include "castor/tape/utils/IndexedContainer.hpp"
#include "castor/tape/utils/SmartFdList.hpp"
#include "h/Castor_limits.h"
#include "h/Cuuid.h"

#include <list>
#include <map>
#include <stdint.h>
#include <time.h>

namespace castor     {
namespace tape       {
namespace tapebridge {

/**
 * Acts as a bridge between the tapegatewayd and rtcpd daemons.
 *
 * The BridgeProtocolEngine behaves like a smart pointer for the initital
 * rtcpd-connection.  This means the destructor of the BridgeSocketCatalogue
 * will close the inititalrtcpd-connection if it is still open.
 *
 * The BridgeProtocolEngine will not close the listen socket used to accept
 * rtcpd connections.
 */
class BridgeProtocolEngine {

public:

  /**
   * Constructor.
   *
   * @param fileCloser               The object used to close file-descriptors.
   *                                 The main goal of this object to facilitate
   *                                 in unit-testing the BridgeProtocolEngine.
   * @param bulkRequestConfigParams  The values of the bulk-request
   *                                 configuration-parameters to be used by the
   *                                 tapebridged daemon.
   * @param tapeFlushConfigParams    The values of the tape-flush
   *                                 configuration-parameters to be used by the
   *                                 tapebridged daemon.
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
   * @param logPeerOfCallbackConnectionsFromRtcpd Set to true if the
   *                                 BridgedProtocolEngine should log the peer
   *                                 IP, port and host name of newly accepted
   *                                 rtcpd callback-connections.
   * @param checkRtcpdIsConnectingFromLocalHost Set to true if the
   *                                 BridgedProtocolEngine should check and
   *                                 abort if the rtcpd daemon is not
   *                                 connecting from the local host.
   * @param clientProxy              Object representing the client of the
   *                                 tapebridged daemon.
   * @param legacyTxRx               Object reponsible for sending and
   *                                 receiving the headers of messages
   *                                 belonging to the legacy RTCOPY protocol.
   */
  BridgeProtocolEngine(
    IFileCloser                         &fileCloser,
    const BulkRequestConfigParams       &bulkRequestConfigParams,
    const TapeFlushConfigParams         &tapeFlushConfigParams,
    const Cuuid_t                       &cuuid,
    const int                           rtcpdListenSock,
    const int                           initialRtcpdSock,
    const legacymsg::RtcpJobRqstMsgBody &jobRequest,
    const tapegateway::Volume           &volume,
    const uint32_t                      nbFilesOnDestinationTape,
    utils::BoolFunctor                  &stoppingGracefully,
    Counter<uint64_t>                   &tapebridgeTransactionCounter,
    const bool                          logPeerOfCallbackConnectionsFromRtcpd,
    const bool                          checkRtcpdIsConnectingFromLocalHost,
    IClientProxy                        &clientProxy,
    ILegacyTxRx                         &legacyTxRx)
    throw(castor::exception::Exception);

  /**
   * Run a dump/migration/recall session.
   */
  void run() throw();

private:

  /**
   * The object used to close file-descriptors.
   *
   * The main goal of this object is to facilitate in unit-testing the
   * BridgeProtocolEngine.
   */
  IFileCloser &m_fileCloser;

  /**
   * The values of the bulk-request configuration-parameters to be used by the
   * tapebridged daemon.
   */
  const BulkRequestConfigParams m_bulkRequestConfigParams;

  /**
   * The values of the tape-flush configuration-parameters to be used by the
   * tapebridged daemon.
   */
  const TapeFlushConfigParams m_tapeFlushConfigParams;

  /**
   * The cuuid to be used for logging.
   */
  const Cuuid_t &m_cuuid;

  /**
   * The catalogue of all the rtcpd and tapegateway connections used by the
   * bridge protocol engine.
   *
   * The catalogue behaves as a smart pointer for the initial rtcpd connection,
   * the rtcpd disk/tape IO control-connections and the client connections.  In
   * other words the destructor of the catalogue will close these connections
   * if they are still open.
   *
   * The catalogue will not close the listen socket used to accept rtcpd
   * connections.  This is the responsibility of the VdqmRequestHandler.
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
  int32_t m_nextDestinationTapeFSeq;

  /**
   * Functor that returns true if the daemon is stopping gracefully.
   */
  utils::BoolFunctor &m_stoppingGracefully;

  /**
   * The counter used to generate the next tapebridge transaction ID to be used
   * in a request to the client.
   */
  Counter<uint64_t> &m_tapebridgeTransactionCounter;

  /**
   * The time at which the tapebridged daemon last pinged the rtcpd daemon.
   *
   * The time is measured as seconds since the epoch.
   */
  time_t m_timeOfLastRtcpdPing;

  /**
   * The time at which the tapebridged daemon last pinged the client.
   *
   * The time is measured as seconds since the epoch.
   */
  time_t m_timeOfLastClientPing;

  /**
   * The number of received RTCP_ENDOF_REQ messages.
   */
  uint32_t m_nbReceivedENDOF_REQs;

  /**
   * Indexed container of the file transaction IDs of all the files currently
   * being transfered either for recall or migration.
   */
  utils::IndexedContainer<uint64_t> m_pendingTransferIds;

  /**
   * The store of pending file-migrations.  A pending file-migration is one
   * that has either just been sent to the rtcpd daemon or has been both sent
   * and written to tape but not yet flushed to tape.
   */
  PendingMigrationsStore m_pendingMigrationsStore;

  /**
   * A list of lists, where each sub-list represents a batch of files that were
   * flushed to tape by a common flush.
   */
  std::list<FileWrittenNotificationList> m_flushedBatches;

  /**
   * This member variable is set to true when the session with the rtcpd
   * daemon has been finished and therefore all connections with the rtcpd
   * daemon, accept for the initial callback connection are closed.
   */
  bool m_sessionWithRtcpdIsFinished;

  /**
   * Set to true if the BridgedProtocolEngine should log the peer IP, port and
   * host name of newly accepted rtcpd callback-connections.
   */
  const bool m_logPeerOfCallbackConnectionsFromRtcpd;

  /**
   * Set to true if the BridgedProtocolEngine should check and abort if the
   * rtcpd daemon is not connecting from the local host.
   */
  const bool m_checkRtcpdIsConnectingFromLocalHost;

  /**
   * Object representing the client of the tapebridged daemon.
   */
  IClientProxy &m_clientProxy;

  /**
   * Object reponsible for sending and receiving the headers of messages
   * belonging to the legacy RTCOPY protocol.
   */
  ILegacyTxRx &m_legacyTxRx;

  /**
   * The files to be migrated to tape that have been returned by the
   * tapegatewayd daemon but have not yet been consumed by the rtcpd daemon.
   */
  std::list<FileToMigrate> m_filesToMigrate;

  /**
   * The files to be recalled from tape that have been returned by the
   * tapegatewayd daemon but have not yet been consumed by the rtcpd daemon.
   */
  std::list<FileToRecall> m_filesToRecall;

  /**
   * The list of errors generated during the tape session.
   *
   * The list is ordered chronologically ordered with the front of the list
   * being the first and oldest error and the back of the list being the last
   * and youngest error.
   *
   * The list will log to DLF the very first session-error that is pushed onto
   * the back of the list.  This is because the first very first session-error
   * will be the one that starts the graceful shutdown of the current
   * tape-session.
   */
  SessionErrorList m_sessionErrors;

protected:

  /**
   * Starts the session with the rtcpd daemon.
   *
   * Please note that this method does not throw any exceptions.
   *
   * @return true if the session was started else false.
   */
  bool startRtcpdSession() throw();

  /**
   * Ends the session with the rtcpd daemon including closing the initial-rtcpd
   * connection.
   *
   * Please note that this method does not throw any exceptions.
   */
  void endRtcpdSession() throw();

  /**
   * Processes the rtcpd and client sockets in a loop until the end of the
   * rtcpd-session has been reached.
   *
   * Please note that this method does not send the final "end-of-session"
   * message to the rtcpd daemon and close the initial rtcpd-connection; these
   * are the responsibilities of the caller.
   *
   * Please note that this method does not throw any exceptions.
   */
  void runRtcpdSession() throw();

  /**
   * Processes the rtcpd and client sockets in a loop until the end of the
   * rtcpd session has been reached.
   *
   * Please note that this method does not send the final "end-of-session"
   * message to the rtcpd daemon and close the initial rtcpd-connection; these
   * are the responsibilities of the caller.
   */
  void processSocksInALoop() throw(castor::exception::Exception);

  /**
   * Handles the events returned by a call to select with all of the open rtcpd
   * and client TCP/IP connections.
   *
   * @param selectTimeout The timeout value to be passed to select.
   */
  void handleSelectEvents(struct timeval selectTimeout)
    throw(castor::exception::Exception);

  /**
   * Accepts an rtcpd connection using the specified listener-socket.
   */
  int acceptRtcpdConnection() throw(castor::exception::Exception);

  /**
   * Logs the peer IP, port and host name of the specified callback connection
   * received from the rtcpd daemon.
   *
   * This method throws a castor::exception::Exception exception if the peer
   * IP, port and host name could not be determined.
   *
   * @param connectedSock The newly accepted connection-socket.
   */
  void logCallbackConnectionFromRtcpd(const int connectedSock)
    throw(castor::exception::Exception);

  /**
   * Processes a pending socket from the result of calling select().
   *
   * Please note that this method will modify the catalogue of
   * socket-descriptors as necessary.  For example closed connections will be
   * released from the catalogue and newly accepted connections added.
   *
   * @param readFdSet The read file-descriptor set from calling select().
   */
  void processAPendingSocket(fd_set &readFdSet)
    throw (castor::exception::Exception);

  /**
   * Processes the socket listening for connection from the rtcpd daemon.  The
   * socket should be pending.
   *
   * Please note that this method will modify the catalogue of
   * socket-descriptors as necessary.
   */
  void processPendingListenSocket() throw (castor::exception::Exception);

  /**
   * Processes the specified socket which must be both pending and the initial
   * connection made from the rtcpd daemon to this tapebridged daemon.
   *
   * @param pendingSock the file descriptior of the pending socket.
   */
  void processPendingInitialRtcpdSocket(const int pendingSock)
    throw (castor::exception::Exception);

  /**
   * Processes the specified socket which must be both pending and an rtcpd
   * disk ot tape IO control connection.
   *
   * Please note that this method will modify the catalogue of
   * socket-descriptors as necessary.
   *
   * @param pendingSock the file descriptior of the pending socket.
   */
  void processPendingRtcpdDiskTapeIOControlSocket(const int pendingSock)
    throw (castor::exception::Exception);

  /**
   * Processes the specified socket which must be both pending and a
   * "get more work" connection made from the tapebridged daemon to a client
   * (readtp, writetp, dumptp or tapegatewayd).
   *
   * Please note that this method will modify the catalogue of
   * socket-descriptors as necessary.
   *
   * @param pendingSock the file descriptior of the pending socket.
   */
  void processPendingClientGetMoreWorkSocket(const int pendingSock)
    throw (castor::exception::Exception);

  /**
   * Dispatches the appropriate client-reply callback-method for the reply
   * received from the client to a request for more work.
   *
   * @param obj                   The message object received from the client.
   * @param getMoreWorkConnection Information about the "get more work"
   *                              connection with the client.  This includes
   *                              among other things, the socket-descriptor of
   *                              the rtcpd disk/tape IO control-connection
   *                              that effectively requested the creation of
   *                              the connection with the client.
   */
  void dispatchCallbackForReplyToGetMoreWork(
    IObject *const              obj,
    const GetMoreWorkConnection &getMoreWorkConnection)
    throw(castor::exception::Exception);

  /**
   * Processes the specified socket which must be both pending and a connection
   * made from this tapebridged daemon to a migration client (writetp or
   * tapegatewayd).
   *
   * Please note that this method will modify the catalogue of
   * socket-descriptors as necessary.
   *
   * @param pendingSock the file descriptior of the pending socket.
   */
  void processPendingClientMigrationReportSocket(const int pendingSock)
    throw (castor::exception::Exception);

  /**
   * Processes the specified rtcpd request.
   *
   * @param header   The header of the rtcpd request.
   * @param socketFd The file descriptor of the socket from which the message
   *                 should be read from.
   */
  void processRtcpdRequest(
    const legacymsg::MessageHeader &header,
    const int                      socketFd)
    throw(castor::exception::Exception);

  /**
   * Dispatches the appropriate rtcpd request handler.
   *
   * @param header   The header of the rtcpd request.
   * @param socketFd The file descriptor of the socket from which the message
   *                 should be read from.
   */
  void dispatchRtcpdRequestHandler(
    const legacymsg::MessageHeader &header,
    const int                      socketFd)
    throw(castor::exception::Exception);

  /**
   * RTCP_FILE_REQ rtcpd message-body handler.
   *
   * This method calls dispatchRtcpFileErrReq() with "no error".
   *
   * @param header   The header of the message that has already been read in
   *                 from the connection socket.
   * @param socketFd The socket-descriptor of the connection socket.
   */
  void rtcpFileReqRtcpdCallback(
    const legacymsg::MessageHeader &header,
    const int                      socketFd)
    throw(castor::exception::Exception);

  /**
   * RTCP_FILEERR_REQ rtcpd message-body handler.
   *
   * This method throws an exception if the RTCP_FILEERR_REQ message signals an
   * error, else this method calls dispatchRtcpFileErrReq().
   *
   * @param header   The header of the message that has already been read in
   *                 from the connection socket.
   * @param socketFd The socket-descriptor of the connection socket.
   */
  void rtcpFileErrReqRtcpdCallback(
    const legacymsg::MessageHeader &header,
    const int                      socketFd)
    throw(castor::exception::Exception);

  /**
   * Dispatches the appropriate helper-method based on the value of the
   * procStatus field of the specified RTCP_FILE_REQ/RTCP_FILEERR_REQ message.
   *
   * This method implements the common logic of the rtcpFileReqRtcpdCallback
   * and rtcpFileErrReqRtcpdCallback functions.
   *
   * @param header    The header of the request.
   * @param body      The body of the request.
   * @param rtcpdSock The file descriptor of the socket from which both the
   *                  header and the body of the message have already been read
   *                  from.
   */
  void dispatchRtcpFileErrReq(const legacymsg::MessageHeader &header,
    legacymsg::RtcpFileRqstErrMsgBody &body, const int rtcpdSock)
    throw(castor::exception::Exception);

  /**
   * Processes the specified RTCP file request in the context of dumping a
   * tape.
   *
   * This method simply replies to the rtcpd daemon with a positive
   * acknowledgement.
   *
   * @param header    The header of the request.
   * @param rtcpdSock The file descriptor of the socket from which both the
   *                  header and the body of the message have already been read
   *                  from.
   */
  void processRtcpFileErrReqDump(const legacymsg::MessageHeader &header,
    const int rtcpdSock) throw(castor::exception::Exception);

  /**
   * Processes the specified RTCP waiting request.
   *
   * This method first replies to the rtpcd daemon with a positive
   * acknowledgement.
   *
   * If the RTCP waiting request signals an error then this method pushes the
   * error onto the back of the list of session errors.
   *
   * @param header    The header of the request.
   * @param rtcpdSock The file descriptor of the socket from which both the
   *                  header and the body of the message have already been read
   *                  from.
   */
  void processRtcpWaiting(const legacymsg::MessageHeader &header,
    legacymsg::RtcpFileRqstErrMsgBody &body, const int rtcpdSock)
    throw(castor::exception::Exception);

  /**
   * Processes the specified RTCP file request for more work.
   *
   * If the rtcpd session is being shutdown, then this method replies with a no
   * more work message and returns.
   *
   * @param header    The header of the request.
   * @param body      The body of the request.
   * @param rtcpdSock The file descriptor of the socket from which both the
   *                  header and the body of the message have already been read
   *                  from.
   */
  void processRtcpRequestMoreWork(const legacymsg::MessageHeader &header,
    legacymsg::RtcpFileRqstErrMsgBody &body, const int rtcpdSock)
    throw(castor::exception::Exception);

  /**
   * Processes the specified RTCP file request for more migration work.
   *
   * @param header    The header of the request.
   * @param body      The body of the request.
   * @param rtcpdSock The file descriptor of the socket from which both the
   *                  header and the body of the message have already been read
   *                  from.
   */
  void processRtcpRequestMoreMigrationWork(
    const legacymsg::MessageHeader    &header,
    legacymsg::RtcpFileRqstErrMsgBody &body,
    const int                         rtcpdSock)
    throw(castor::exception::Exception);

  /**
   * Sends a FilesToMigrateListRequest message to the client.
   */
  void sendFilesToMigrateListRequestToClient(
    const legacymsg::MessageHeader    &header,
    legacymsg::RtcpFileRqstErrMsgBody &body,
    const int                         rtcpdSock)
    throw(castor::exception::Exception);

  /**
   * Takes a file to migrate request from the cache and sends it to the rtcpd
   * daemon.
   *
   * @param rtcpdSock     The socket-descriptor of the rtcpd-connection.
   * @param rtcpdReqMagic The magic number of the rtcpd request message
   *                      awaiting a reply from the client.
   * @param rtcpdReqType  The type of the rtcpd message awaiting a reply from
   *                      the client.
   */
  void sendCachedFileToMigrateToRtcpd(
    const int      rtcpdSock,
    const uint32_t rtcpdReqMagic,
    const uint32_t rtcpdReqType)
    throw(castor::exception::Exception);

  /**
   * Processes the specified RTCP file request for more recall work.
   *
   * @param header    The header of the request.
   * @param body      The body of the request.
   * @param rtcpdSock The file descriptor of the socket from which both the
   *                  header and the body of the message have already been read
   *                  from.
   */
  void processRtcpRequestMoreRecallWork(
    const legacymsg::MessageHeader    &header,
    legacymsg::RtcpFileRqstErrMsgBody &body,
    const int                         rtcpdSock)
    throw(castor::exception::Exception);

  /**
   * Sends a FilesToRecallListRequest message to the client.
   */
  void sendFilesToRecallListRequestToClient(
    const legacymsg::MessageHeader    &header,
    legacymsg::RtcpFileRqstErrMsgBody &body,
    const int                         rtcpdSock)
    throw(castor::exception::Exception);

  /**
   * Takes a file to recall request from the cache and sends it to the rtcpd
   * daemon.
   *
   * @param rtcpdSock        The socket-descriptor of the rtcpd-connection.
   * @param rtcpdReqMagic    The magic number of the rtcpd request message
   *                         awaiting a reply from the client.
   * @param rtcpdReqType     The type of the rtcpd message awaiting a reply
   *                         from the client.
   * @param rtcpdReqTapePath The tape path associated with the rtcpd request
   *                         message awaiting a reply from the client.
   */
  void sendCachedFileToRecallToRtcpd(
    const int         rtcpdSock,
    const uint32_t    rtcpdReqMagic,
    const uint32_t    rtcpdReqType,
    const std::string &rtcpdReqTapePath)
    throw(castor::exception::Exception);

  /**
   * Sends the specified file to recall to the rtcpd daemon.
   *
   * @param fileToRecall     The file to be recalled.
   * @param rtcpdSock        The socket-descriptor of the rtcpd-connection.
   * @param rtcpdReqMagic    The magic number of the rtcpd request message
   *                         awaiting a reply from the client.
   * @param rtcpdReqType     The type of the rtcpd message awaiting a reply
   *                         from the client.
   * @param rtcpdReqTapePath The tape path associated with the rtcpd request
   *                         message awaiting a reply from the client.
   */
  void sendFileToRecallToRtcpd(
    const FileToRecall &fileToRecall,
    const int          rtcpdSock,
    const uint32_t     rtcpdReqMagic,
    const uint32_t     rtcpdReqType,
    const std::string  &rtcpdReqTapePath)
    throw(castor::exception::Exception);

  /**
   * Processes the specified RTCP file positioned request.
   *
   * This method first replies to the rtpcd daemon with a positive
   * acknowledgement.
   *
   * If the RTCP waiting request signals an error then this method pushes the
   * error onto the back of the list of session errors.
   *
   * @param header    The header of the request.
   * @param body      The body of the request.
   * @param rtcpdSock The file descriptor of the socket from which both the
   *                  header and the body of the message have already been read
   *                  from.
   */
  void processRtcpFilePositionedRequest(const legacymsg::MessageHeader &header,
    legacymsg::RtcpFileRqstErrMsgBody &body, const int rtcpdSock)
    throw(castor::exception::Exception);

  /**
   * Processes the specified RTCP file transfer finished request.
   *
   * This method first replies to the rtpcd daemon with a positive
   * acknowledgement.
   *
   * If the rtcpd session is being shutdown, then this method drops the
   * RTCP_FINISHED message and returns.
   *
   * If the RTCP_FINISHED message signals an error, then this method pushes the
   * error onto the back of the list of session errors and returns.
   *
   * If the RTCP_FINISHED message does not signal an error, then this method
   * calls processSuccessfullRtcpFileFinishedRequest().
   *
   * If processSuccessfullRtcpFileFinishedRequest() throws an exception then
   * this method pushes the error onto the back of the list of session errors
   * and returns.
   *
   * @param header    The header of the request.
   * @param body      The body of the request.
   * @param rtcpdSock The file descriptor of the socket from which both the
   *                  header and the body of the message have already been read
   *                  from.
   */
  void processRtcpFileFinishedRequest(const legacymsg::MessageHeader &header,
    legacymsg::RtcpFileRqstErrMsgBody &body, const int rtcpdSock)
    throw(castor::exception::Exception);

  /**
   * Processes the specified successful (no embedded error message) RTCP file
   * transfer finished request.
   *
   * If the current tape-mount is for migration then this method adds the
   * file-written notification to the pending migrations store as the
   * associated data has not yet been flushed to the physical tape.
   *
   * If the current tape-mount is for recall then this method tries to notify
   * the client (readtp, tapegatewayd or writetp) of the successful recall.
   *
   * @param fileTransactonId The file transaction ID that was sent by the
   *                         client (the tapegatewayd daemon or the readtp or
   *                         writetp command-line tool)
   * @param body             The body of the RTCP request.
   */
  void processSuccessfullRtcpFileFinishedRequest(
    const uint64_t fileTransactonId, legacymsg::RtcpFileRqstErrMsgBody &body)
    throw(castor::exception::Exception);

  /**
   * RTCP_TAPEREQ rtcpd message-body handler.
   *
   * This receives the rtcp tape request message from the rtcpd daemon and
   * replies with a positive acknowledgement.
   *
   * @param header   The header of the message that has already been read in
   *                 from the connection socket.
   * @param socketFd The socket-descriptor of the connection socket.
   */
  void rtcpTapeReqRtcpdCallback(
    const legacymsg::MessageHeader &header,
    const int                      socketFd)
    throw(castor::exception::Exception);

  /**
   * RTCP_TAPEERR rtcpd message-body handler.
   *
   * This method first receives the rtcp tape request message from the rtcpd
   * daemon and replies with a positive acknowledgement.
   *
   * If the rtcp tape request message signals an error then this method
   * throws an exception.
   *
   * @param header   The header of the message that has already been read in
   *                 from the connection socket.
   * @param socketFd The socket-descriptor of the connection socket.
   */
  void rtcpTapeErrReqRtcpdCallback(
    const legacymsg::MessageHeader &header,
    const int                      socketFd)
    throw(castor::exception::Exception);

  /**
   * RTCP_ENDOF_REQ rtcpd message-body handler.
   *
   * @param socketFd The socket-descriptor of the connection socket.
   */
  void rtcpEndOfReqRtcpdCallback(const int socketFd)
    throw(castor::exception::Exception);

  /**
   * TAPEBRIDGE_FLUSHEDTOTAPE tape-bridge message-body handler.
   *
   * This method first recieves the rtcp tape request message from the rtcpd
   * daemon and replies with a positive acknowledgement.
   *
   * If the rtcpd session is being shutdown, then this method drops the
   * TAPEBRIDGE_FLUSHEDTOTAPE message and returns.
   *
   * This method then processes the files that were migrated to tape by the
   * flush.
   *
   * @param header   The header of the message that has already been read in
   *                 from the connection socket.
   * @param socketFd The socket-descriptor of the connection socket.
   */
  void tapeBridgeFlushedToTapeCallback(
    const legacymsg::MessageHeader &header,
    const int                      socketFd)
    throw(castor::exception::Exception);

  /**
   * Sends the "file flushed to tape" notifications that correspond to the
   * specified "file written to tape" notifications that have now received
   * their corresponding "flushed to tape" message from the rtcpd daemon.
   */
  void sendFlushedMigrationsToClient(
    const FileWrittenNotificationList &notifications)
    throw (castor::exception::Exception);

  /**
   * GIVE_OUTP rtcpd message-body handler.
   *
   * This method receives the GIVE_OUTP message from the rtcpd daemon and then
   * relays it to the client (dumptp).
   *
   * @param header   The header of the message that has already been read in
   *                 from the connection socket.
   * @param socketFd The socket-descriptor of the connection socket.
   */
  void giveOutpRtcpdCallback(
    const legacymsg::MessageHeader &header,
    const int                      socketFd)
    throw(castor::exception::Exception);

  /**
   * Starts a migration session.
   *
   * @return true if the session was started, else false
   */
  bool startMigrationSession() throw(castor::exception::Exception);

  /**
   * Starts a recall session.
   */
  void startRecallSession() throw(castor::exception::Exception);

  /**
   * Starts a dump session.
   */
  void startDumpSession() throw(castor::exception::Exception);

  /**
   * Throws an exception if the peer host associated with the specified
   * socket is not localhost.
   *
   * @param socketFd The socket file descriptor.
   */
  void checkPeerIsLocalhost(const int socketFd)
    throw(castor::exception::Exception);

  /**
   * FilesToMigrateList client message handler.
   *
   * @param obj                   The message object received from the client.
   * @param getMoreWorkConnection Information about the "get more work"
   *                              connection with the client.  This includes
   *                              among other things, the socket-descriptor of
   *                              the rtcpd disk/tape IO control-connection
   *                              that effectively requested the creation of
   *                              the connection with the client.
   */
  void filesToMigrateListClientCallback(
    IObject *const              obj,
    const GetMoreWorkConnection &getMoreWorkConnection)
    throw(castor::exception::Exception);

  /**
   * Adds the specified files to be migrated that have been received from the
   * the client to the cache of files to be migrated.
   *
   * @param filesToMigrate The files to be migrated that have been received
   *                       from the client.
   */
  void addFilesToMigrateToCache(
    const std::vector<tapegateway::FileToMigrateStruct*> &clientFilesToMigrate)
    throw(castor::exception::Exception);

  /**
   * Adds the specified file to be migrated that has been received from the
   * the client to the cache of files to be migrated.
   *
   * @param clientFileToMigrate The file to be migrated that has been received
   *                            from the client.
   */
  void addFileToMigrateToCache(
    const tapegateway::FileToMigrateStruct &clientFileToMigrate)
    throw(castor::exception::Exception);

  /**
   * Sends the specified file to migrate to the rtcpd daemon.
   *
   * @param fileToMigrate The file to be migrated.
   * @param rtcpdSock     The socket-descriptor of the rtcpd-connection.
   * @param rtcpdReqMagic The magic number of the rtcpd request message
   *                      awaiting a reply from the client.
   * @param rtcpdReqType  The type of the rtcpd message awaiting a reply from
   *                      the client.
   */
  void sendFileToMigrateToRtcpd(
    const FileToMigrate &fileToMigrate,
    const int           rtcpdSock,
    const uint32_t      rtcpdReqMagic,
    const uint32_t      rtcpdReqType)
    throw(castor::exception::Exception);

  /**
   * FilesToRecallListClientCallback client message handler.
   *
   * @param obj                   The message object received from the client.
   * @param getMoreWorkConnection Information about the "get more work"
   *                              connection with the client.  This includes
   *                              among other things, the socket-descriptor of
   *                              the rtcpd disk/tape IO control-connection
   *                              that effectively requested the creation of
   *                              the connection with the client.
   */
  void filesToRecallListClientCallback(
    IObject *const              obj,
    const GetMoreWorkConnection &getMoreWorkConnection)
    throw(castor::exception::Exception);

  /**
   * Adds the specified files to be recalled that have been received from the
   * the client to the cache of files to be recalled.
   *
   * @param filesToRecall The files to be recalled that have been received
   *                      from the client.
   */
  void addFilesToRecallToCache(
    const std::vector<tapegateway::FileToRecallStruct*> &clientFilesToRecall)
    throw(castor::exception::Exception);

  /**
   * Adds the specified file to be migrated that has been received from the
   * the client to the cache of files to be recalled.
   *
   * @param clientFileToRecall The file to be recalled that has been received
   *                           from the client.
   */
  void addFileToRecallToCache(
    const tapegateway::FileToRecallStruct &clientFileToRecall)
    throw(castor::exception::Exception);

  /**
   * NoMoreFiles client message handler.
   *
   * @param obj                   The message object received from the client.
   * @param getMoreWorkConnection Information about the "get more work"
   *                              connection with the client.  This includes
   *                              among other things, the socket-descriptor of
   *                              the rtcpd disk/tape IO control-connection
   *                              that effectively requested the creation of
   *                              the connection with the client.
   */
  void noMoreFilesClientCallback(
    IObject *const              obj,
    const GetMoreWorkConnection &getMoreWorkConnection)
    throw(castor::exception::Exception);

  /**
   * Tells the rtcpd daemon that there are no for files to transfer.  In the
   * case of migrating files to tape, this method first notifies the internal
   * pending-migration store that there are no more files to migrate.
   *
   * @param rtcpdSock          The file descriptor of the rtcpd socket which is
   *                           awaiting a reply.
   * @param rtcpdMagic         The magic number of the initiating rtcpd request.
   *                           This magic number will be used in any acknowledge
   *                           message to be sent to rtcpd.
   * @param rtcpdReqType       The request type of the initiating rtcpd
   *                           request. This request type will be used in any
   *                           acknowledge message to be sent to rtcpd.
   */
  void tellRtcpdThereAreNoMoreFiles(
    const int      rtcpdSock,
    const uint32_t rtcpdMagic,
    const uint32_t rtcpdReqType)
  throw(castor::exception::Exception);

  /**
   * EndNotificationErrorReport client message handler.
   *
   * @param obj                   The message object received from the client.
   * @param getMoreWorkConnection Information about the "get more work"
   *                              connection with the client.  This includes
   *                              among other things, the socket-descriptor of
   *                              the rtcpd disk/tape IO control-connection
   *                              that effectively requested the creation of
   *                              the connection with the client.
   */
  void endNotificationErrorReportClientCallback(
    IObject *const              obj,
    const GetMoreWorkConnection &getMoreWorkConnection)
    throw(castor::exception::Exception);

  /**
   * Notifies the client of the end of session.
   *
   * Please note that this method does not throw an exception.
   */
  void notifyClientEndOfSession() throw();

  /**
   * Notifies the rtcpd daemon of the end of session using the initial
   * rtcpd connection.
   */
  void notifyRtcpdEndOfSession() throw(castor::exception::Exception);

  /**
   * Notifies the client of any file-specific errors that were received from the
   * rtcpd daemon.
   *
   * Please note that this method does not throw any exceptions.
   */
  void notifyClientOfAnyFileSpecificErrors() throw();

  /**
   * Notifies the client of a failure to migrate a file to tape.
   *
   * @param cuuid The ccuid to be used for logging.
   * @param e     The error.
   */
  void notifyClientOfFailedMigrations(const Cuuid_t &cuuid,
    const std::list<SessionError> &sessionErrors)
    throw(castor::exception::Exception);

  /**
   * Notifies the client of a failure to recall a file from tape.
   *
   * @param cuuid The ccuid to be used for logging.
   * @param e     The error.
   */
  void notifyClientOfFailedRecalls(const Cuuid_t &cuuid,
    const std::list<SessionError> &sessionErrors)
    throw(castor::exception::Exception);

  /**
   * Returns true if for any reason the session with the rtcpd daemon is being
   * shutdown.
   */
  bool shuttingDownRtcpdSession() const;

  /**
   * Returns true if the session with the rtcpd daemon is finished else false.
   */
  bool sessionWithRtcpdIsFinished() const throw();

  /**
   * Returns the total number of disk/tape IO control-connections.
   */
  int getNbDiskTapeIOControlConns() const;

  /**
   * Returns the number of RTCP_ENDOF_REQ messages that have been received so
   * far from the rtcpd daemon.
   */
  uint32_t getNbReceivedENDOF_REQs() const throw();

  /**
   * Returns true if the BridgeProtocolEngine should continue to process
   * sockets.
   *
   * Please note that the BridgeProtocolEngine should continue to process
   * sockets as long it is still participating in a dialog with the client
   * (readtp, tapegatewayd or writetp) and/or the rtcpd daemon.
   */
  bool continueProcessingSocks() const;

  /**
   * Uses the specified unsigned 64-bit integer to generate and write the
   * corresponding migration tape-file-id into the specified destination
   * character array.
   *
   * The migration tape-file-id is in fact a hexadecimal string where the
   * letter digits are all capital letters.
   *
   * @param i   The unsigned 64-bit integer.
   * @param dst The destination character array.
   */
  void generateMigrationTapeFileId(
    const uint64_t i,
    char           (&dst)[CA_MAXPATHLEN+1])
    const throw(castor::exception::Exception);

}; // class BridgeProtocolEngine

} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TAPEBRIDGE_BRIDGEPROTOCOLENGINE
