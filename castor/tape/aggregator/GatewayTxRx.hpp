/******************************************************************************
 *                castor/tape/aggregator/GatewayTxRx.hpp
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

#ifndef CASTOR_TAPE_AGGREGATOR_GATEWAYTXRX_HPP
#define CASTOR_TAPE_AGGREGATOR_GATEWAYTXRX_HPP 1

#include "castor/exception/Exception.hpp"
#include "h/Castor_limits.h"

#include <stdint.h>


namespace castor     {
namespace tape       {
namespace aggregator {

/**
 * Provides functions for sending and receiving the messages of the tape
 * gateway/aggregator protocol.
 */
class GatewayTxRx {

public:

  /**
   * Gets a the volume to be mounted from the tape tape gateway.
   *
   * @param gatewayHost The tape gateway host name.
   * @param gatewayPort The tape gateway port number.
   * @param volReqId The volume request ID to be sent to the tape gateway.
   * @param vid Out parameter: The volume ID returned by the tape gateway.
   * @param mode Out parameter: The access mode returned by the tape gateway.
   * @param label Out parameter: The volume label returned by the tape gateway.
   * @param density Out parameter: The volume density returned by the tape
   * @return True if there is a volume to mount.
   */
  static bool getVolumeFromGateway(const std::string gatewayHost,
    const unsigned short gatewayPort, const uint32_t volReqId,
    char (&vid)[CA_MAXVIDLEN+1], uint32_t &mode,
    char (&label)[CA_MAXLBLTYPLEN+1], char (&density)[CA_MAXDENLEN+1])
    throw(castor::exception::Exception);


  /**
   * Gets a file to migrate from the tape tape gateway.
   *
   * @param gatewayHost The tape gateway host name.
   * @param gatewayPort The tape gateway port number.
   * @param transactionId The transaction ID to be sent to the tape gateway.
   * @param filePath Out parameter: The path of the disk file.
   * @param recordFormat Out parameter: The record format.
   * @param nsHost Out parameter: The name server host.
   * @param fileId Out parmeter: The CASTOR file ID.
   * @param tapeFileSeq Out parameter: The tape file sequence number.
   * @param fileSize Out parameter: The six of the file.
   * @param lastKnownFileName Out parameter: The last known name of the file.
   * @param lastModificationTime Out parameter: The time of the last
   * modification.
   * @return True if there is a file to migrate.
   */
  static bool getFileToMigrateFromGateway(const std::string gatewayHost,
    const unsigned short gatewayPort, const uint32_t transactionId,
    char (&filePath)[CA_MAXPATHLEN+1], char (&recordFormat)[CA_MAXRECFMLEN+1],
    char (&nsHost)[CA_MAXHOSTNAMELEN], uint64_t &fileId, uint32_t &tapeFileSeq,
    uint64_t &fileSize, char (&lastKnownFileName)[CA_MAXPATHLEN+1],
    uint64_t &lastModificationTime) throw(castor::exception::Exception);

  /**
   * Gets a file to recall from the tape tape gateway.
   *
   * @param gatewayHost The tape gateway host name.
   * @param gatewayPort The tape gateway port number.
   * @param transactionId The transaction ID to be sent to the tape gateway.
   * @param filePath Out parameter: The path of the disk file.
   * @param recordFormat Out parameter: The record format.
   * @param nsHost Out parameter: The name server host.
   * @param fileId Out parmeter: The CASTOR file ID.
   * @param tapeFileSeq Out parameter: The tape file sequence number.
   * @return True if there is a file to recall.
   */
  static bool getFileToRecallFromGateway(const std::string gatewayHost,
    const unsigned short gatewayPort, const uint32_t transactionId,
    char (&filePath)[CA_MAXPATHLEN+1], char (&recordFormat)[CA_MAXRECFMLEN+1],
    char (&nsHost)[CA_MAXHOSTNAMELEN], uint64_t &fileId, uint32_t &tapeFileSeq)
    throw(castor::exception::Exception);

  /**
   * Notifies the tape gateway of the successful migration of a file to tape.
   *
   * @param gatewayHost The tape gateway host name.
   * @param gatewayPort The tape gateway port number.
   * @param transactionId The transaction ID to be sent to the tape gateway.
   */
  static void notifyGatewayOfFileMigrated(const std::string gatewayHost,
    const unsigned short gatewayPort, const uint32_t transactionId)
    throw(castor::exception::Exception);

  /**
   * Notifies the tape gateway of the successful recall of a file from tape.
   *
   * @param gatewayHost The tape gateway host name.
   * @param gatewayPort The tape gateway port number.
   * @param transactionId The transaction ID to be sent to the tape gateway.
   */
  static void notifyGatewayOfFileRecalled(const std::string gatewayHost,
    const unsigned short gatewayPort, const uint32_t transactionId)
    throw(castor::exception::Exception);

  /**
   * Notifies the tape gateway of the end of the entire set of
   * recalls/migrations.
   *
   * @param gatewayHost The tape gateway host name.
   * @param gatewayPort The tape gateway port number.
   * @param transactionId The transaction ID to be sent to the tape gateway.
   */
  static void notifyGatewayOfEnd(const std::string gatewayHost,
    const unsigned short gatewayPort, const uint32_t transactionId)
    throw(castor::exception::Exception);


private:

  /**
   * Private constructor to inhibit instances of this class from being
   * instantiated.
   */
  GatewayTxRx() {}

}; // class GatewayTxRx

} // namespace aggregator
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_AGGREGATOR_GATEWAYTXRX_HPP
