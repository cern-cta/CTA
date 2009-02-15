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
 * gateway <-> aggregator protocol.
 */
class GatewayTxRx {

public:

  /**
   * Gets a file to migrate from the tape tape gateway by sending and receiving
   * the necessary messages.
   *
   * @param gatewayHost The tape gateway host name.
   * @param gatewayPort The tape gateway port number.
   * @param transactionId The transaction ID to be sent to the tape gateway.
   * the tape gateway which should match the value sent.
   * @param filePath Out parameter: The path of the disk file.
   * @param recordFormat Out parameter: The record format.
   * @param nsHost Out parameter: The name server host.
   * @param fileId Out parmeter: The CASTOR file ID.
   * @param tapeFileSeq Out parameter: The tape file sequence number.
   * @return True if there is file to migrate.
   */
  static bool getFileToMigrateFromGateway(const std::string gatewayHost,
    const unsigned short gatewayPort, const uint32_t transactionId,
    char (&filePath)[CA_MAXPATHLEN+1], char (&recordFormat)[CA_MAXRECFMLEN+1],
    char (&nsHost)[CA_MAXHOSTNAMELEN], uint64_t &fileId, uint32_t &tapeFileSeq)
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
