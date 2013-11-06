/******************************************************************************
 *                      castor/tape/tapebridge/MigrationReportConnection.hpp
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

#ifndef CASTOR_TAPE_TAPEBRIDGE_MIGRATIONREPORTCONNECTION_HPP
#define CASTOR_TAPE_TAPEBRIDGE_MIGRATIONREPORTCONNECTION_HPP 1

#include <stdint.h>

namespace castor     {
namespace tape       {
namespace tapebridge {

/**
 * Structure representing a pending client connection.  This is a connection
 * that has been opened with the client and has had a message written to it.
 */
struct MigrationReportConnection {

  /**
   * Constructor that will initialise the member variable clientSock to -1 and
   * the member variable tapebridgeTransId to 0.
   */
  MigrationReportConnection();

  /**
   * The socket-descriptor of the client-connection if there is one, else -1
   * if there is not.
   */
  int clientSock;

  /**
   * The tapebridge transaction ID associated with the message that was sent
   * to the client.
   *
   * This member of the structure should be ignored if the clientSock member
   * is negative.
   */
  uint64_t tapebridgeTransId;

}; // struct MigrationReportConnection

} // namespace tapebridge
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_TAPEBRIDGE_MIGRATIONREPORTCONNECTION_HPP
