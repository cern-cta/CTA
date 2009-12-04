/******************************************************************************
 *                castor/tape/aggregator/ClientTxRx.hpp
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

#ifndef CASTOR_TAPE_AGGREGATOR_CLIENTTXRX_HPP
#define CASTOR_TAPE_AGGREGATOR_CLIENTTXRX_HPP 1

#include "castor/exception/Exception.hpp"
#include "castor/tape/tapegateway/DumpParameters.hpp"
#include "castor/tape/tapegateway/FileToMigrate.hpp"
#include "castor/tape/tapegateway/FileToRecall.hpp"
#include "castor/tape/tapegateway/Volume.hpp"
#include "h/Castor_limits.h"
#include "h/Cuuid.h"

#include <stdint.h>


namespace castor     {
namespace tape       {
namespace aggregator {


/**
 * Provides functions for sending and receiving messages to and from aggregator
 * clients (tape-gateway, tpread, tpwrite and tpdump).
 */
class ClientTxRx {

public:

  /**
   * Gets the volume to be mounted from the client.
   *
   * @param cuuid              The ccuid to be used for logging.
   * @param mountTransactionId The mount transaction ID to be sent to the
   *                           client.
   * @param aggregatorTransactionId The aggregator transaction ID to be sent to
   *                           the client.
   * @param clientHost         The client host name.
   * @param clientPort         The client port number.
   * @param unit               The tape unit.
   * @return                   A pointer to the volume message received from
   *                           the client or NULL if there is no volume to
   *                           mount.  The callee is responsible for
   *                           deallocating the message.
   */
  static tapegateway::Volume *getVolume(
    const Cuuid_t        &cuuid,
    const uint32_t       mountTransactionId,
    const uint64_t       aggregatorTransactionId,
    const char           *clientHost,
    const unsigned short clientPort,
    const char           (&unit)[CA_MAXUNMLEN+1])
    throw(castor::exception::Exception);

  /**
   * Sends a FileToMigrateRequest to the client and returns the
   * socket-descriptor of the client connection from which the reply will be
   * read later.
   *
   * @param mountTransactionId The mount transaction ID to be sent to the
   *                           client.
   * @param aggregatorTransactionId The aggregator transaction ID to be sent to
   *                           the client.
   * @param clientHost         The client host name.
   * @param clientPort         The client port number.
   * @param connectDuration    Out parameter: The number of seconds it took to
   *                           connect to the client.
   * @return                   The socket-descriptor of the tape-gateway
   *                           connection from which the reply will be read
   *                           later.
   */
  static int sendFileToMigrateRequest(
    const uint32_t       mountTransactionId,
    const uint64_t       aggregatorTransactionId,
    const char           *clientHost,
    const unsigned short clientPort,
    time_t               &connectDuration)
    throw(castor::exception::Exception);

  /**
   * Receives the reply to a FileToMigrateRequest from the specified client
   * socket and then closes the connection.
   *
   * @param mountTransactionId The mount transaction ID to be sent to the
   *                           client.
   * @param aggregatorTransactionId The aggregator transaction ID to be sent to
   *                           the client.
   * @param clientSock         The socket-descriptor of the client connection.
   * @return                   A pointer to the file to migrate message
   *                           received from the client or NULL if there is no
   *                           file to be migrated.  The callee is responsible
   *                           for deallocating the message.
   */
  static castor::tape::tapegateway::FileToMigrate
    *receiveFileToMigrateReplyAndClose(
    const uint32_t mountTransactionId,
    const uint64_t aggregatorTransactionId,
    const int      clientSock)
    throw(castor::exception::Exception);

  /**
   * Sends a FileToRecallRequest to the client and returns the
   * socket-descriptor of the client connection from which the reply will be
   * read later.
   *
   * @param mountTransactionId The mount transaction ID to be sent to the
   *                           client.
   * @param aggregatorTransactionId The aggregator transaction ID to be sent to
   *                           the client.
   * @param clientHost         The client host name.
   * @param clientPort         The client port number.
   * @param connectDuration    Out parameter: The number of seconds it took to
   *                           connect to the client.
   * @return                   The socket-descriptor of the tape-gateway
   *                           connection from which the reply will be read
   *                           later.
   */
  static int sendFileToRecallRequest(
    const uint32_t       mountTransactionId,
    const uint64_t       aggregatorTransactionId,
    const char           *clientHost,
    const unsigned short clientPort,
    time_t               &connectDuration)
    throw(castor::exception::Exception);

  /**
   * Receives the reply to a FileToRecallRequest from the specified client
   * socket and then closes the connection.
   *
   * @param mountTransactionId The mount transaction ID to be sent to the
   *                           client.
   * @param aggregatorTransactionId The aggregator transaction ID to be sent to
   *                           the client.
   * @param clientSock         The socket-descriptor of the client connection.
   * @return                   A pointer to the file to recall message received
   *                           from the client or NULL if there is no file to
   *                           be recalled.  The callee is responsible for
   *                           deallocating the message.
   */
  static castor::tape::tapegateway::FileToRecall
    *receiveFileToRecallReplyAndClose(
    const uint32_t mountTransactionId,
    const uint64_t aggregatorTransactionId,
    const int      clientSock)
    throw(castor::exception::Exception);

  /**
   * Sends a FileMigratedNotification to the client and returns the
   * socket-descriptor of the client connection from which the reply will be
   * read later.
   *
   * @param mountTransactionId  The mount transaction ID to be sent to the
   *                            client.
   * @param aggregatorTransactionId The aggregator transaction ID to be sent to
   *                            the client.
   * @param clientHost          The client host name.
   * @param clientPort          The client port number.
   * @param connectDuration     Out parameter: The number of seconds it took to
   *                            connect to the client.
   * @param fileTransactionId   The file transaction ID.
   * @param nsHost              The name server host.
   * @param fileId              The CASTOR file ID.
   * @param tapeFileSeq         The tape file seuence number.
   * @param blockId             The tape block ID.
   * @param positionCommandCode The position method uesd by the drive.
   * @param checksumAlgorithm   The name of the checksum algorithm.
   * @param checksum            The file checksum.
   * @param fileSize            The size of the file without compression.
   * @param compressedFileSize  The size of on-tape compressed file.
   * @return                    The socket-descriptor of the tape-gateway
   *                            connection from which the reply will be read
   *                            later.
   */
  static int sendFileMigratedNotification(
    const uint32_t       mountTransactionId,
    const uint64_t       aggregatorTransactionId,
    const char           *clientHost,
    const unsigned short clientPort,
    time_t               &connectDuration,
    const uint64_t       fileTransactionId,
    const char           (&nsHost)[CA_MAXHOSTNAMELEN+1],
    const uint64_t       fileId,
    const int32_t        tapeFileSeq,
    const unsigned char  (&blockId)[4],
    const int32_t        positionCommandCode,
    const char           (&checksumAlgorithm)[CA_MAXCKSUMNAMELEN+1],
    const uint32_t       checksum,
    const uint64_t       fileSize,
    const uint64_t       compressedFileSize)
    throw(castor::exception::Exception);

  /**
   * Sends a FileRecalledNotification to the client and returns the
   * socket-descriptor of the client connection from which the reply will be
   * read later.
   *
   * @param mountTransactionId  The mount transaction ID to be sent to the
   *                            client.
   * @param aggregatorTransactionId The aggregator transaction ID to be sent to
   *                            the client.
   * @param clientHost          The client host name.
   * @param clientPort          The client port number.
   * @param connectDuration     Out parameter: The number of seconds it took to
   *                            connect to the client.
   * @param fileTransactionId   The file transaction ID.
   * @param filePath            The path of the disk file.
   * @param nsHost              The name server host.
   * @param fileId              The CASTOR file ID.
   * @param tapeFileSeq         The tape file seuence number.
   * @param filePath            The path of the disk file.
   * @param positionCommandCode The position method uesd by the drive.
   * @param checksumAlgorithm   The name of the checksum algorithm.
   * @param checksum            The file checksum.
   * @param fileSize            The size of the file without compression.
   */
  static int sendFileRecalledNotification(
    const uint32_t       mountTransactionId,
    const uint64_t       aggregatorTransactionId,
    const char           *clientHost,
    const unsigned short clientPort,
    time_t               &connectDuration,
    const uint64_t       fileTransactionId,
    const char           (&nsHost)[CA_MAXHOSTNAMELEN+1],
    const uint64_t       fileId,
    const int32_t        tapeFileSeq,
    const char           (&filePath)[CA_MAXPATHLEN+1],
    const int32_t        positionCommandCode,
    const char           (&checksumAlgorithm)[CA_MAXCKSUMNAMELEN+1],
    const uint32_t       checksum,
    const uint64_t       fileSize)
    throw(castor::exception::Exception);

  /**
   * Receives the reply to a FileToMigratedNotification or a
   * FileToRecallNotification from the specified client socket and then closes
   * the connection.
   *
   * @param mountTransactionId  The mount transaction ID to be sent to the
   *                            client.
   * @param aggregatorTransactionId The aggregator transaction ID to be sent to
   *                            the client.
   * @param clientSock          The socket-descriptor of the client connection.
   */
  static void receiveNotificationReplyAndClose(
    const uint32_t mountTransactionId,
    const uint64_t aggregatorTransactionId,
    const int      clientSock)
    throw(castor::exception::Exception);

  /**
   * Notifies the client of the end of the recall/migration session.
   *
   * @param cuuid               The ccuid to be used for logging.
   * @param mountTransactionId  The mount transaction ID to be sent to the
   *                            client.
   * @param aggregatorTransactionId The aggregator transaction ID to be sent to
   *                            the client.
   * @param clientHost          The client host name.
   * @param clientPort          The client port number.
   */
  static void notifyEndOfSession(
    const Cuuid_t        &cuuid,
    const uint32_t       mountTransactionId,
    const uint64_t       aggregatorTransactionId,
    const char           *clientHost,
    const unsigned short clientPort)
    throw(castor::exception::Exception);

  /**
   * Notifies the client of the end of the recall/migration session.
   *
   * @param cuuid               The ccuid to be used for logging.
   * @param mountTransactionId  The mount transaction ID to be sent to the
   *                            client.
   * @param aggregatorTransactionId The aggregator transaction ID to be sent to
   *                            the client.
   * @param clientHost          The client host name.
   * @param clientPort          The client port number.
   * @param ex                  The exception which failed the session.
   */
  static void notifyEndOfFailedSession(
    const Cuuid_t        &cuuid,
    const uint32_t       mountTransactionId,
    const uint64_t       aggregatorTransactionId,
    const char           *clientHost,
    const unsigned short clientPort,
    castor::exception::Exception &e)
    throw(castor::exception::Exception);

  /**
   * Gets the parameters to be used when dumping a tape.
   *
   * @param cuuid               The ccuid to be used for logging.
   * @param mountTransactionId  The mount transaction ID to be sent to the
   *                            client.
   * @param aggregatorTransactionId The aggregator transaction ID to be sent to
   *                            the client.
   * @param clientHost          The client host name.
   * @param clientPort          The client port number.
   * @return                    A pointer to the DumpParamaters message.  The
   *                            callee is responsible for deallocating the
   *                            message.
   */
  static tapegateway::DumpParameters *getDumpParameters(
    const Cuuid_t        &cuuid,
    const uint32_t       mountTransactionId,
    const uint64_t       aggregatorTransactionId,
    const char           *clientHost,
    const unsigned short clientPort)
    throw(castor::exception::Exception);

  /**
   * Notifies the client of a dump tape message string.
   *
   * @param cuuid               The ccuid to be used for logging.
   * @param mountTransactionId  The mount transaction ID to be sent to the
   *                            client.
   * @param aggregatorTransactionId The aggregator transaction ID to be sent to
   *                            the client.
   * @param clientHost          The client host name.
   * @param clientPor        t  The client port number.
   * @param message             The dump tape message string.
   */
  static void notifyDumpMessage(
    const Cuuid_t        &cuuid,
    const uint32_t       mountTransactionId,
    const uint64_t       aggregatorTransactionId,
    const char           *clientHost,
    const unsigned short clientPort,
    const char           (&message)[CA_MAXLINELEN+1])
    throw(castor::exception::Exception);

  /**
   * Pings the client and throws an exception if the ping has failed.
   *
   * @param cuuid               The ccuid to be used for logging.
   * @param mountTransactionId  The mount transaction ID to be sent to the
   *                            client.
   * @param aggregatorTransactionId The aggregator transaction ID to be sent to
   *                            the client.
   * @param clientHost          The client host name.
   * @param clientPort          The client port number.
   */
  static void ping(
    const Cuuid_t        &cuuid,
    const uint32_t       mountTransactionId,
    const uint64_t       aggregatorTransactionId,
    const char           *clientHost,
    const unsigned short clientPort)
    throw(castor::exception::Exception);

  /**
   * Receives a reply from the specified client socket and then closes the
   * connection.
   *
   * @param clientSock         The socket-descriptor of the client connection.
   * @param clientNetRWTimeout The timeout in seconds used when sending and
   *                           receiving client network messages.
   * @return                   A pointer to the reply object.  It is the
   *                           responsibility of the caller to deallocate the
   *                           memory of the reply object.
   */
  static IObject *receiveReplyAndClose(
    const int clientSock,
    const int clientNetRWTimeout)
    throw(castor::exception::Exception);

  /**
   * Throws an exception if there is a mount transaction ID mismatch and/or
   * an aggregator transaction ID mismatch.
   *
   * @param messageTypeName                 The type name of the client message.
   * @param expectedMountTransactionId      The expected mount transaction ID.
   * @param actualMountTransactionIdA       The actual mount transaction ID.
   * @param expectedAggregatorTransactionId The expected aggregator transaction
   *                                         ID.
   * @param actualAggregatorTransactionId   The actualaggregator transaction ID.
   */
  static void checkTransactionIds(
    const char *const messageTypeName,
    const uint32_t    expectedMountTransactionId,
    const uint32_t    actualMountTransactionId,
    const uint64_t    expectedAggregatorTransactionId,
    const uint64_t    actualAggregatorTransactionId)
    throw(castor::exception::Exception);


private:

  /**
   * Private constructor to inhibit instances of this class from being
   * instantiated.
   */
  ClientTxRx() {}

  /**
   * Sends the specified message to the client and returns the
   * socket-descriptor of the client connection from which a reply will be read
   * later.
   *
   * @param clientHost         The client host name.
   * @param clientPort         The client port number.
   * @param clientNetRWTimeout The timeout in seconds used when sending and
   *                           receiving client network messages.
   * @param request            Out parameter: The request to be sent to the
   *                           client.
   * @param connectDuration    Out parameter: The number of seconds it took to
   *                           connect to the client.
   */
  static int sendMessage(
    const char           *clientHost,
    const unsigned short clientPort,
    const int            clientNetRWTimeout,
    IObject              &message,
    time_t               &connectDuration)
    throw(castor::exception::Exception);

  /**
   * Sends the specified request message to the client and receives the reply.
   *
   * @param requestTypeName    The name of the type of the request.  This name
   *                           will be used in logging messages.
   * @param clientHost         The client host name.
   * @param clientPort         The client port number.
   * @param clientNetRWTimeout The timeout in seconds used when sending and
   *                           receiving client network messages.
   * @param request            Out parameter: The request to be sent to the
   *                           client.
   * @param connectDuration    Out parameter: The number of seconds it took to
   *                           connect to the client.
   * @param sendRecvDuration   Out parameter: The number of seconds it took to
   *                           send the request and receive the reply.
   * @return                   A pointer to the reply object.  It is the
   *                           responsibility of the caller to deallocate the
   *                           memory of the reply object.
   */
  static IObject *sendRequestAndReceiveReply(
    const char           *requestTypeName,
    const char           *clientHost,
    const unsigned short clientPort,
    const int            clientNetRWTimeout,
    IObject              &request,
    time_t               &connectDuration,
    time_t               &sendRecvDuration)
    throw(castor::exception::Exception);

  /**
   * Notifies the client using the specified request message.
   *
   * @param cuuid               The ccuid to be used for logging.
   * @param mountTransactionId  The mount transaction ID to be sent to the
   *                            client.
   * @param aggregatorTransactionId The aggregator transaction ID to be sent to
   *                            the client.
   * @param requestTypeName     The name of the type of the request.  This name
   *                            will be used in logging messages.
   * @param clientHost          The client host name.
   * @param clientPort          The client port number.
   * @param clientNetRWTimeout  The timeout in seconds used when sending and
   *                            receiving client network messages.
   * @param request             Out parameter: The request to be sent to the
   *                            client.
   */
  static void notifyClient(
    const Cuuid_t        &cuuid,
    const uint32_t       mountTransactionId,
    const uint64_t       aggregatorTransactionId,
    const char           *requestTypeName,
    const char           *clientHost,
    const unsigned short clientPort,
    const int            clientNetRWTimeout,
    IObject              &request)
    throw(castor::exception::Exception);

  /**
   * Translates the specified incoming EndNotificationErrorReport message into
   * the throwing of the appropriate exception.  This method is not typical
   * because it throws an exception if it succeeds to carry out its it task.
   *
   * @param mountTransactionId The mount transaction request ID of the current
   *                           mount as known by the aggregator.
   * @param aggregatorTransactionId The aggregator transaction ID.
   * @param obj                The received IObject which represents the
   *                           OBJ_EndNotificationErrorReport message.
   */
  static void throwEndNotificationErrorReport(
    const uint32_t mountTransactionId,
    const uint64_t aggregatorTransactionId,
    IObject *const obj)
    throw(castor::exception::Exception);

}; // class ClientTxRx

} // namespace aggregator
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_AGGREGATOR_CLIENTTXRX_HPP
