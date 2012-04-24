/******************************************************************************
 *                castor/tape/tapebridge/IClientProxy.hpp
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

#ifndef CASTOR_TAPE_TAPEBRIDGE_ICLIENTPROXY_HPP
#define CASTOR_TAPE_TAPEBRIDGE_ICLIENTPROXY_HPP 1

#include "castor/exception/Exception.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/tape/tapegateway/DumpParameters.hpp"
#include "castor/tape/tapegateway/FilesToMigrateList.hpp"
#include "castor/tape/tapegateway/FilesToRecallList.hpp"
#include "castor/tape/tapegateway/Volume.hpp"
#include "h/Castor_limits.h"
#include "h/Cuuid.h"

#include <stdint.h>
#include <string>
#include <sys/time.h>


namespace castor     {
namespace tape       {
namespace tapebridge {

/**
 * The interface to implemented by an object acting as a proxy for the client
 * of the tapebridged daemon.
 *
 * A client of the tapebridged daemon may be dumptp, readtp, tapegatewayd, or
 * writetp.
 *
 * Please do not confuse the use of the word client with that of a TCP/IP
 * client.  A client of the tapebridged daemon is infact a TCP/IP server.  The
 * the tapebridged daemon initiates the TCP/IP connections with its clients
 * (dumptp, readtp, tapegatewayd, and writetp).
 */
class IClientProxy {

public:

  /**
   * Virtual destructor.
   */
  virtual ~IClientProxy() throw();

  /**
   * Gets the volume to be mounted from the client.
   *
   * @param aggregatorTransactionId The tapebridge transaction ID to be sent to
   *         the client.
   * @return A pointer to the volume message received from the client or NULL
   *         if there is no volume to mount.  The callee is responsible for
   *         deallocating the message.
   */
  virtual tapegateway::Volume *getVolume(
    const uint64_t aggregatorTransactionId)
    throw(castor::exception::Exception) = 0;

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
  virtual int sendFilesToMigrateListRequest(
    const uint64_t aggregatorTransactionId,
    const uint64_t maxFiles,
    const uint64_t maxBytes) const
    throw(castor::exception::Exception) = 0;

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
  virtual castor::tape::tapegateway::FilesToMigrateList
    *receiveFilesToMigrateListRequestReplyAndClose(
    const uint64_t aggregatorTransactionId,
    const int      clientSock) const
    throw(castor::exception::Exception) = 0;

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
  virtual int sendFilesToRecallListRequest(
    const uint64_t aggregatorTransactionId,
    const uint64_t maxFiles,
    const uint64_t maxBytes) const
    throw(castor::exception::Exception) = 0;

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
  virtual castor::tape::tapegateway::FilesToRecallList
    *receiveFilesToRecallListRequestReplyAndClose(
    const uint64_t aggregatorTransactionId,
    const int      clientSock) const
    throw(castor::exception::Exception) = 0;

  /**
   * Receives the reply to a FileMigrationReportList or a FileRecallReportList
   * from the specified client socket and then closes the connection.
   *
   * @param aggregatorTransactionId The tapebridge transaction ID to be sent to
   *                            the client.
   * @param clientSock          The socket-descriptor of the client connection.
   */
  virtual void receiveNotificationReplyAndClose(
    const uint64_t aggregatorTransactionId,
    const int      clientSock) const
    throw(castor::exception::Exception) = 0;

  /**
   * Gets the parameters to be used when dumping a tape.
   *
   * @param  aggregatorTransactionId The tapebridge transaction ID to be sent
   *         to the client.
   * @return A pointer to the DumpParamaters message.  The callee is
   *         responsible for deallocating the message.
   */
  virtual tapegateway::DumpParameters *getDumpParameters(
    const uint64_t aggregatorTransactionId) const
    throw(castor::exception::Exception) = 0;

  /**
   * Notifies the client of a dump tape message string.
   *
   * @param aggregatorTransactionId The tapebridge transaction ID to be sent to
   *                the client.
   * @param message The dump tape message string.
   */
  virtual void notifyDumpMessage(
    const uint64_t aggregatorTransactionId,
    const char     (&message)[CA_MAXLINELEN+1]) const
    throw(castor::exception::Exception) = 0;

  /**
   * Pings the client and throws an exception if the ping has failed.
   *
   * @param aggregatorTransactionId The tapebridge transaction ID to be sent to
   *                                the client.
   */
  virtual void ping(
    const uint64_t aggregatorTransactionId) const
    throw(castor::exception::Exception) = 0;

  /**
   * Receives a reply from the specified client socket and then closes the
   * connection.
   *
   * @param clientSock The socket-descriptor of the client connection.
   * @return           A pointer to the reply object.  It is the responsibility
   *                   of the caller to deallocate the memory of the reply
   *                   object.
   */
  virtual IObject *receiveReplyAndClose(const int clientSock) const
    throw(castor::exception::Exception) = 0;

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
  virtual void checkTransactionIds(
    const char *const messageTypeName,
    const uint32_t    actualMountTransactionId,
    const uint64_t    expectedAggregatorTransactionId,
    const uint64_t    actualAggregatorTransactionId) const
    throw(castor::exception::Exception) = 0;

  /**
   * Notifies the client of the end of the recall/migration session.
   *
   * @param aggregatorTransactionId The tapebridge transaction ID to be sent to
   *                                the client.
   */
  virtual void notifyEndOfSession(
    const uint64_t aggregatorTransactionId) const
    throw(castor::exception::Exception) = 0;

  /**
   * Notifies the client of the end of the recall/migration session due to an
   * error not caused by a specific file.
   *
   * @param aggregatorTransactionId The tapebridge transaction ID to be sent to
   *                     the client.
   * @param errorCode    The error code to be reported to the client.
   * @patam errorMessage The error message to be reported to the client.
   */
  virtual void notifyEndOfFailedSession(
    const uint64_t    aggregatorTransactionId,
    const int         errorCode,
    const std::string &errorMessage) const
    throw(castor::exception::Exception) = 0;

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
  virtual int connectAndSendMessage(
    IObject &message,
    timeval &connectDuration) const
    throw(castor::exception::Exception) = 0;

}; // class IClientProxy

} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TAPEBRIDGE_ICLIENTPROXY_HPP
