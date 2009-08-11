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
 * clients (tape gateway, tpread, tpwrite and tpdump).
 */
class ClientTxRx {

public:

  /**
   * Gets the volume to be mounted from the client.
   *
   * @param cuuid      The ccuid to be used for logging.
   * @param volReqId   The volume request ID to be sent to the client.
   * @param clientHost The client host name.
   * @param clientPort The client port number.
   * @param unit       The tape unit.
   * @return           A pointer to the volume message received from the client
   *                   or NULL if there is no volume to mount.  The callee is
   *                   responsible for deallocating the message.
   */
  static tapegateway::Volume *getVolume(const Cuuid_t &cuuid,
    const uint32_t volReqId, const char *clientHost,
    const unsigned short clientPort, const char (&unit)[CA_MAXUNMLEN+1])
    throw(castor::exception::Exception);

  /**
   * Gets a file to migrate from the client.
   *
   * @param cuuid      The ccuid to be used for logging.
   * @param volReqId   The volume request ID to be sent to the client.
   * @param clientHost The client host name.
   * @param clientPort The client port number.
   * @return           A pointer to the file to migrate message received from
   *                   the client or NULL if there is no file to be migrated.
   *                   The callee is responsible for deallocating the message.
   */
  static tapegateway::FileToMigrate *getFileToMigrate(const Cuuid_t &cuuid,
    const uint32_t volReqId, const char *clientHost,
    const unsigned short clientPort) throw(castor::exception::Exception);

  /**
   * Gets a file to recall from the client.
   *
   * @param cuuid      The ccuid to be used for logging.
   * @param volReqId   The volume request ID to be sent to the client. 
   * @param clientHost The client host name.
   * @param clientPort The client port number.
   * @return           A pointer to the file to recall message received from
   *                   the client or NULL if there is no file to be recalled.
   *                   The callee is responsible for deallocating the message.
   */
  static tapegateway::FileToRecall *getFileToRecall(const Cuuid_t &cuuid,
    const uint32_t volReqId, const char *clientHost,
    const unsigned short clientPort) throw(castor::exception::Exception);

  /**
   * Notifies the client of the successful migration of a file to tape.
   *
   * @param cuuid               The ccuid to be used for logging.
   * @param volReqId            The volume request ID to be sent to the client.
   * @param clientHost          The client host name.
   * @param clientPort          The client port number.
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
   */
  static void notifyFileMigrated(
    const Cuuid_t        &cuuid,
    const uint32_t       volReqId,
    const char           *clientHost,
    const unsigned short clientPort,
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
   * Notifies the client of the successful recall of a file from tape.
   *
   * @param cuuid               The ccuid to be used for logging.
   * @param volReqId            The volume request ID to be sent to the client. 
   * @param clientHost          The client host name.
   * @param clientPort         The client port number.
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
  static void notifyFileRecalled(
    const Cuuid_t        &cuuid,
    const uint32_t       volReqId,
    const char           *clientHost, 
    const unsigned short clientPort,
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
   * Notifies the client of the end of the recall/migration session.
   *
   * @param cuuid       The ccuid to be used for logging.
   * @param volReqId    The volume request ID to be sent to the client.
   * @param clientHost  The client host name.
   * @param clientPort  The client port number.
   */
  static void notifyEndOfSession(const Cuuid_t &cuuid,
    const uint32_t volReqId, const char *clientHost,
    const unsigned short clientPort) throw(castor::exception::Exception);

  /**
   * Notifies the client of the end of the recall/migration session.
   *
   * @param cuuid       The ccuid to be used for logging.
   * @param volReqId    The volume request ID to be sent to the client.
   * @param clientHost  The client host name.
   * @param clientPort  The client port number.
   * @param ex          The exception which failed the session.
   */
  static void notifyEndOfFailedSession(const Cuuid_t &cuuid,
    const uint32_t volReqId, const char *clientHost,
    const unsigned short clientPort, castor::exception::Exception &e)
    throw(castor::exception::Exception);

  /**
   * Gets the parameters to be used when dumping a tape.
   *
   * @param cuuid      The ccuid to be used for logging.
   * @param volReqId   The volume request ID to be sent to the client.
   * @param clientHost The client host name.
   * @param clientPort The client port number.
   * @return           A pointer to the DumpParamaters message.  The callee is
   *                   responsible for deallocating the message.
   */
  static tapegateway::DumpParameters *getDumpParameters(
    const Cuuid_t &cuuid, const uint32_t volReqId, const char *clientHost,
    const unsigned short clientPort) throw(castor::exception::Exception);

  /**
   * Notifies the client of a dump tape message string.
   *
   * @param cuuid       The ccuid to be used for logging.
   * @param volReqId    The volume request ID to be sent to the client.
   * @param clientHost  The client host name.
   * @param clientPort  The client port number.
   * @param message     The dump tape message string.
   */
  static void notifyDumpMessage(const Cuuid_t &cuuid,
    const uint32_t volReqId, const char *clientHost,
    const unsigned short clientPort, const char (&message)[CA_MAXLINELEN+1])
    throw(castor::exception::Exception);

  /**
   * Pings the client and throws an exception if the ping has failed.
   *
   * @param cuuid      The ccuid to be used for logging.
   * @param volReqId   The volume request ID to be sent to the client.
   * @param clientHost The client host name.
   * @param clientPort The client port number.
   * @param message    The dump tape message string.
   */
  static void ping(const Cuuid_t &cuuid, const uint32_t volReqId,
    const char *clientHost, const unsigned short clientPort)
    throw(castor::exception::Exception);


private:

  /**
   * Private constructor to inhibit instances of this class from being
   * instantiated.
   */
  ClientTxRx() {}

}; // class ClientTxRx

} // namespace aggregator
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_AGGREGATOR_CLIENTTXRX_HPP
