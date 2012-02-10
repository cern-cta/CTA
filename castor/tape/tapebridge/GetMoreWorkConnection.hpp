/******************************************************************************
 *                      castor/tape/tapebridge/GetMoreWorkConnection.hpp
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

#ifndef CASTOR_TAPE_TAPEBRIDGE_GETMOREWORKCONNECTION_HPP
#define CASTOR_TAPE_TAPEBRIDGE_GETMOREWORKCONNECTION_HPP 1

#include "h/Castor_limits.h"

#include <stdint.h>
#include <string>
#include <sys/time.h>

namespace castor     {
namespace tape       {
namespace tapebridge {

/**
 * The information stored in the socket catalogue about a connection that has
 * been opened with the client and then had either a FilesToMigrateListRequest
 * or a FilesToRecallListRequest message written to it by the tapebridged
 * daemon.
 */
struct GetMoreWorkConnection {

  /**
   * The socket-descriptor of an rtcpd-connection.
   */
  int rtcpdSock;

  /**
   * The magic number of the rtcpd request message awaiting a reply from the
   * client.
   *
   * If there is no such rtcpd request message, then the value of this member
   * will be 0.
   *
   * The primary goal of this member is to enable the tapebridge to send
   * acknowledgement messages to rtcpd with the correct magic number, i.e.
   * the same magic number as the initiating rtcpd request message.
   */
  uint32_t rtcpdReqMagic;

  /**
   * The type of the rtcpd message awaiting a reply from the client.
   *
   * If there is no such rtcpd request message, then the value of this member
   * will be 0.
   *
   * The primary goal of this member is to enable the tapebridge to send
   * acknowledgement messages to rtcpd with the correct request type, i.e.
   * the same request type as the initiating rtcpd request message.
   */
  uint32_t rtcpdReqType;

  /**
   * The tape path associated with the rtcpd request message awaiting a reply
   * from the client.
   */
  std::string rtcpdReqTapePath;

  /**
   * The socket-descriptor of an associated client-connection.
   *
   * If the rtcpd-connection is waiting for a reply from the client, then
   * this the value of this member is the client-connection socket-descriptor,
   * else the value of this member is -1.
   */
  int clientSock;

  /**
   * If a request has been resent to the client, then this is the tapebridge
   * transaction ID associated with that request.
   *
   * If no message has been sent, then the value of this member is 0.
   */
  uint64_t aggregatorTransactionId;

  /**
   * If a request has been resent to the client, then this is the absolute
   * time when the request was sent.
   *
   * If no message has been sent, then the value of this member is 0 seconds
   * and 0 micro-seconds.
   */
  struct timeval clientReqTimeStamp;

  /**
   * Constructor.
   *
   * Initialises all of the member variables.
   *
   * All non-file-descriptor integer are set to 0.
   *
   * All booleans are set to false.
   *
   * All file-descriptors are set to -1.
   *
   * All strings are set to the empty string.
   *
   * All timeval structures are set to 0 seconds and 0 micro-seconds.
   */
  GetMoreWorkConnection();

  /**
   * Clears all of the member variables.
   *
   * All non-file-descriptor integer are set to 0.
   *
   * All booleans are set to false.
   *
   * All file-descriptors are set to -1.
   *
   * All strings are set to the empty string.
   *
   * All timeval structures are set to 0 seconds and 0 micro-seconds.
   */
  void clear();

}; // struct GetMoreWorkConnection

} // namespace tapebridge
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_TAPEBRIDGE_GETMOREWORKCONNECTION_HPP
