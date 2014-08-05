/******************************************************************************
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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/messages/Header.pb.h"
#include "castor/messages/ZmqMsg.hpp"
#include "castor/utils/utils.hpp"

#include <string>

namespace castor   {
namespace messages {

/**
 * Structure representing a message frame.
 */
struct Frame {
  /**
   * The header of the frame.
   */
  messages::Header header;

  /**
   * The body of the frame.
   */
  std::string body;

  /**
   * Checks the hash value field of the header against the body of the frame.
   */
  void checkHashValueOfBody() const;

  /**
   * Serializes the frame header to the specified ZMQ message.
   *
   * Please note that the specified size of the specified ZMQ message must
   * match that of the header.
   *
   * @param msg Output parameter: The ZMQ message.
   */
  void serializeHeaderToZmqMsg(ZmqMsg &msg) const;

  /**
   * Parses the specified ZMQ message into the frame header.
   *
   * @param msg The ZMQ message.
   */
  void parseZmqMsgIntoHeader(const ZmqMsg &msg);

  /**
   * Serializes the specified protocol buffer into the frame body, calculates
   * it hash value and stores the has value in the frame header.
   *
   * @param protocolBuffer The protocol buffer.
   */
  template <class T> void serializeProtocolBufferIntoBody(
    const T &protocolBuffer) {
    try {
      if(!protocolBuffer.SerializeToString(&body)) {
        castor::exception::Exception ex;
        ex.getMessage() << "SerializeToString() returned false";
        throw ex;
      }

      calcAndSetHashValueOfBody();
    } catch(castor::exception::Exception &ne) {
      castor::exception::Exception ex;
      ex.getMessage() << "Frame failed to serialize protocol buffer " <<
        castor::utils::demangledNameOf(protocolBuffer) << " into frame body: "
        << ne.getMessage().str();
      throw ex;
    }
  }

  /**
   * Parses the body into the specified protocol buffer.
   *
   * @param pb Output parameter: The protocol buffer to be written to.
   */
  template <class T> void parseBodyIntoProtocolBuffer(T &protocolBuffer) const {
    if(!protocolBuffer.ParseFromString(body)) {
      castor::exception::Exception ex;
      ex.getMessage() << "Frame failed to parse contents of enclosed ZMQ"
        " message into protocol buffer " <<
        castor::utils::demangledNameOf(protocolBuffer)
        << ": ParseFromString() returned false";
      throw ex;
    } 
  }   

private:

  /**
   * Calculates the hash value of the frame body and records the result in the
   * frame header.
   */
  void calcAndSetHashValueOfBody();
}; // struct Frame

} // namespace messages
} // namespace castor
