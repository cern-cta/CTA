/******************************************************************************
 *                castor/tape/tapebridge/ClientProxy.hpp
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef CASTOR_TAPE_TAPEBRIDGE_CLIENTPROXY_HPP
#define CASTOR_TAPE_TAPEBRIDGE_CLIENTPROXY_HPP 1

#include "castor/tape/tapebridge/ClientAddress.hpp"
#include "castor/tape/tapebridge/ClientAddressLocal.hpp"
#include "castor/tape/tapebridge/ClientAddressTcpIp.hpp"
#include "castor/tape/tapebridge/IClientProxy.hpp"

namespace castor     {
namespace tape       {
namespace tapebridge {

/**
 * Acts as a proxy for the client of the tapebridged daemon.
 *
 * A client of the tapebridged daemon may be dumptp, readtp, tapegatewayd, or
 * writetp.
 *
 * Please do not confuse the use of the word client with that of a TCP/IP
 * client.  A client of the tapebridged daemon is infact a TCP/IP server.  The
 * the tapebridged daemon initiates the TCP/IP connections with its clients
 * (dumptp, readtp, tapegatewayd, and writetp).
 */
class ClientProxy: public IClientProxy {

public:

  /**
   * Constructor.
   *
   * This method throws a castor::exception::InvalidArgument if the client
   * host-name is an empty string or is longer than CA_MAXHOSTNAMELEN
   * characters.
   *
   * This method throws a castor::exception::InvalidArgument if the
   * device-group-name associated with the current tape-session is an empty
   * string or is longer than CA_MAXDGNLEN characters.
   *
   * This method throws a castor::exception::InvalidArgument if the name of the
   * tape-drive unit is an empty string or is longer than CA_MAXUNMLEN
   * characters.
   *
   * @param cuuid              The cuuid to be used for logging.
   * @param mountTransactionId The mount transaction ID to be used with the
   *                           client.
   * @param netTimeout         The timeout in seconds used when sending and
   *                           receiving client network messages.
   * @param clientAddress      The address if the client.
   * @param dgn                The device-group-name associated with the
   *                           current tape-session.  The maximum allowed
   *                           length for this string is CA_MAXDGNLEN.
   * @param driveUnit          The name of the tape-drive unit associated with
   *                           the current tape-session.  The maximum allowed
   *                           length for this string is CA_MAXUNMLEN.
   */
  ClientProxy(
    const Cuuid_t          &cuuid,
    const uint32_t         mountTransactionId,
    const int              netTimeout,
    const ClientAddress    &clientAddress,
    const std::string      &dgn,
    const std::string      &driveUnit)
    throw(castor::exception::InvalidArgument);

  /**
   * Destructor.
   */
  ~ClientProxy() throw();
  
  /**
   * Gets the volume to be mounted from the client.
   *
   * @param aggregatorTransactionId The tapebridge transaction ID to be sent to
   *         the client.
   * @return A pointer to the volume message received from the client or NULL
   *         if there is no volume to mount.  The callee is responsible for
   *         deallocating the message.
   */
  tapegateway::Volume *getVolume(
    const uint64_t aggregatorTransactionId)
    throw(castor::exception::Exception);

  /**
   * Sends a FilesToMigrateListRequest to the client and returns the
   * socket-descriptor of the client connection from which the reply will be
   * read later.
   *
   * @param aggregatorTransactionId The tapebridge transaction ID to be sent to
   *                 the client.
   * @param maxFiles The maximum number of files the client can send in the
   *                 reply.
   * @param maxBytes The maximum number of files the client can send in the
   *                 reply represented indirectly by the sum of their
   *                 file-sizes.
   * @return         The socket-descriptor of the client connection from which
   *                 the reply will be read later.
   */
  int sendFilesToMigrateListRequest(
    const uint64_t aggregatorTransactionId,
    const uint64_t maxFiles,
    const uint64_t maxBytes) const
    throw(castor::exception::Exception);

  /**
   * Receives the reply to a FilesToMigrateListRequest from the specified
   * client socket and then closes the connection.
   *
   * This method throws an exception if the client replies with a
   * FilesToMigrateList containing 0 files.  If a client has no more files to
   * migrate then it must send a NoMoreFiles message.
   *
   * @param aggregatorTransactionId The tapebridge transaction ID to be sent to
   *                           the client.
   * @param clientSock         The socket-descriptor of the client connection.
   * @return                   A pointer to the FilesToMigrateList message
   *                           received from the client or NULL if there is no
   *                           file to be migrated.  The callee is responsible
   *                           for deallocating the message.
   */
  castor::tape::tapegateway::FilesToMigrateList
    *receiveFilesToMigrateListRequestReplyAndClose(
    const uint64_t aggregatorTransactionId,
    const int      clientSock) const
    throw(castor::exception::Exception);

  /**
   * Sends a FilesToRecallListRequest to the client and returns the
   * socket-descriptor of the client connection from which the reply will be
   * read later.
   *
   * @param aggregatorTransactionId The tapebridge transaction ID to be sent to
   *                           the client.
   * @param clientHost         The client host name.
   * @param clientPort         The client port number.
   * @param maxFiles           The maximum number of files the client can send
   *                           in the reply.
   * @param maxBytes           The maximum number of files the client can send
   *                           in the reply represented indirectly by the sum
   *                           of their file-sizes.
   * @return                   The socket-descriptor of the tape-gateway
   *                           connection from which the reply will be read
   *                           later.
   */
  int sendFilesToRecallListRequest(
    const uint64_t aggregatorTransactionId,
    const uint64_t maxFiles,
    const uint64_t maxBytes) const
    throw(castor::exception::Exception);

  /**
   * Receives the reply to a FilesToRecallListRequest from the specified client
   * socket and then closes the connection.
   *
   * This method throws an exception if the client replies with a
   * FilesToRecallList containing 0 files.  If a client has no more files to
   * recall then it must send a NoMoreFiles message.
   *
   * @param aggregatorTransactionId The tapebridge transaction ID to be sent to
   *                           the client.
   * @param clientSock         The socket-descriptor of the client connection.
   * @return                   A pointer to the file to recall message received
   *                           from the client or NULL if there is no file to
   *                           be recalled.  The callee is responsible for
   *                           deallocating the message.
   */
  castor::tape::tapegateway::FilesToRecallList
    *receiveFilesToRecallListRequestReplyAndClose(
    const uint64_t aggregatorTransactionId,
    const int      clientSock) const
    throw(castor::exception::Exception);

  /**
   * Receives the reply to a FileMigrationReportList or a FileRecallReportList
   * from the specified client socket and then closes the connection.
   *
   * @param aggregatorTransactionId The tapebridge transaction ID to be sent to
   *                            the client.
   * @param clientSock          The socket-descriptor of the client connection.
   */
  void receiveNotificationReplyAndClose(
    const uint64_t aggregatorTransactionId,
    const int      clientSock) const
    throw(castor::exception::Exception);

  /**
   * Gets the parameters to be used when dumping a tape.
   *
   * @param  aggregatorTransactionId The tapebridge transaction ID to be sent
   *         to the client.
   * @return A pointer to the DumpParamaters message.  The callee is
   *         responsible for deallocating the message.
   */
  tapegateway::DumpParameters *getDumpParameters(
    const uint64_t aggregatorTransactionId) const
    throw(castor::exception::Exception);

  /**
   * Notifies the client of a dump tape message string.
   *
   * @param aggregatorTransactionId The tapebridge transaction ID to be sent to
   *                the client.
   * @param message The dump tape message string.
   */
  void notifyDumpMessage(
    const uint64_t aggregatorTransactionId,
    const char     (&message)[CA_MAXLINELEN+1]) const
    throw(castor::exception::Exception);

  /**
   * Pings the client and throws an exception if the ping has failed.
   *
   * @param aggregatorTransactionId The tapebridge transaction ID to be sent to
   *                                the client.
   */
  void ping(
    const uint64_t aggregatorTransactionId) const
    throw(castor::exception::Exception);

  /**
   * Receives a reply from the specified client socket and then closes the
   * connection.
   *
   * @param clientSock The socket-descriptor of the client connection.
   * @return           A pointer to the reply object.  It is the responsibility
   *                   of the caller to deallocate the memory of the reply
   *                   object.
   */
  IObject *receiveReplyAndClose(const int clientSock) const
    throw(castor::exception::Exception);

  /**
   * Throws an exception if there is a mount transaction ID mismatch and/or
   * a aggregator transaction ID mismatch.
   *
   * @param messageTypeName                 The type name of the client message.
   * @param actualMountTransactionIdA       The actual mount transaction ID.
   * @param expectedAggregatorTransactionId The expected aggregator transaction
   *                                        ID.
   * @param actualAggregatorTransactionId   The actual aggregator transaction
   *                                        ID.
   */
  void checkTransactionIds(
    const char *const messageTypeName,
    const uint32_t    actualMountTransactionId,
    const uint64_t    expectedAggregatorTransactionId,
    const uint64_t    actualAggregatorTransactionId) const
    throw(castor::exception::Exception);

  /**
   * Notifies the client of the end of the recall/migration session.
   *
   * @param aggregatorTransactionId The tapebridge transaction ID to be sent to
   *                                the client.
   */
  void notifyEndOfSession(
    const uint64_t aggregatorTransactionId) const
    throw(castor::exception::Exception);

  /**
   * Notifies the client of the end of the recall/migration session due to an
   * error not caused by a specific file.
   *
   * @param aggregatorTransactionId The tapebridge transaction ID to be sent to
   *                     the client.
   * @param errorCode    The error code to be reported to the client.
   * @patam errorMessage The error message to be reported to the client.
   */
  void notifyEndOfFailedSession(
    const uint64_t    aggregatorTransactionId,
    const int         errorCode,
    const std::string &errorMessage) const
    throw(castor::exception::Exception);

  /**
   * Connects to the client and sends the specified message to the client.
   *
   * This methods returns the socket-descriptor of the connection with the
   * client so the reply from the client can be read in later.
   *
   * @param request         Out parameter: The request to be sent to the
   *                        client.
   * @param connectDuration Out parameter: The time it took to connect to the
   *                        client.
   * @return                The socket-descriptor of the connection with the
   *                        client.
   */
  int connectAndSendMessage(
    IObject &message,
    timeval &connectDuration) const
    throw(castor::exception::Exception);

protected:

  /**
   * Connects to the client, sends the specified request message and then
   * receives the reply.
   *
   * @param requestTypeName  The name of the type of the request.  This name
   *                         will be used in logging messages.
   * @param request          Out parameter: The request to be sent to the
   *                         client.
   * @param connectDuration  Out parameter: The time it took to connect to the
   *                         client.
   * @param sendRecvDuration Out parameter: The time it took to send the
   *                         request and receive the reply.
   * @return                 A pointer to the reply object.  It is the
   *                         responsibility of the caller to deallocate the
   *                         memory of the reply object.
   */
  IObject *connectSendRequestAndReceiveReply(
    const char *const requestTypeName,
    IObject           &request,
    timeval           &connectDuration,
    timeval           &sendRecvDuration) const
    throw(castor::exception::Exception);

  /**
   * Notifies the client using the specified request message.
   *
   * @param aggregatorTransactionId The tapebridge transaction ID to be sent to
   *                        the client.
   * @param requestTypeName The name of the type of the request.  This name
   *                        will be used in logging messages.
   * @param request         Out parameter: The request to be sent to the
   *                        client.
   */
  void notifyClient(
    const uint64_t    aggregatorTransactionId,
    const char *const requestTypeName,
    IObject           &request) const
    throw(castor::exception::Exception);

  /**
   * Translates the specified incoming EndNotificationErrorReport message into
   * the throwing of the appropriate exception.
   *
   * This method is not typical because it throws an exception if it succeeds.
   *
   * @param obj The received IObject which represents the
   *            OBJ_EndNotificationErrorReport message.
   */
  void throwEndNotificationErrorReport(IObject *const obj) const
    throw(castor::exception::Exception);

  /**
   * The cuuid to be used for logging.
   */
  const Cuuid_t &m_cuuid;

  /**
   * The mount transaction ID to be used with the client.
   */
  const uint32_t m_mountTransactionId;

  /**
   * The timeout in seconds used when sending and receiving client network
   * messages.
   */
  const int m_netTimeout;

  /**
   * The address of the client.
   */
  const ClientAddress &m_clientAddress;

  /**
   * The device-group name of the tape-session or "UNKNOWN" if it is not yet
   * known.
   */
  const std::string m_dgn;

  /**
   * The name of the tape-drive unit associated with the current tape session
   * or "UNKNOWN" if it is not yet known.
   */
  const std::string m_driveUnit;

  /**
   * The volume identifier of the tape associated with the current tape session
   * or the string "UNKNOWN" if it is not yet known.
   */
  std::string m_vid;

  /**
   * User readable string describing the type of client associated with the
   * current tape session or the string "UNKNOWN" if it is not yet known.
   */
  std::string m_clientType;

}; // class ClientProxy

} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TAPEBRIDGE_CLIENTPROXY_HPP
