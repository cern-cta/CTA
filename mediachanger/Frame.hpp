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

#include "mediachanger/Header.pb.h"
#include "mediachanger/ZmqMsg.hpp"

#include <cxxabi.h>
#include <google/protobuf/message.h>
#include <string>

namespace cta {
namespace mediachanger {

/**
 * Structure representing a message frame.
 */
struct Frame {
  /**
   * The header of the frame.
   */
  Header header;

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
  void serializeProtocolBufferIntoBody(
    const google::protobuf::Message &protocolBuffer);

  /**
   * Parses the body into the specified protocol buffer.
   *
   * @param pb Output parameter: The protocol buffer to be written to.
   */
  void parseBodyIntoProtocolBuffer(google::protobuf::Message &protocolBuffer)
    const;

private:

  /**
   * Calculates the hash value of the frame body and records the result in the
   * frame header.
   */
  void calcAndSetHashValueOfBody();

  /**
   * Determines the demangled type name of the specified object.
   *
   * @param t The object.
   * @return The demangled type name.
   */  
  template <class T> static std::string demangledNameOf(const T&t) {
    std::string responseType = typeid(t).name();
    int status = -1;
    char * demangled = abi::__cxa_demangle(responseType.c_str(), NULL, NULL, &status);
    if (!status) {
      responseType = demangled; 
    }
    free(demangled);
  
    return responseType;
  }
}; // struct Frame

} // namespace mediachanger
} // namespace cta
