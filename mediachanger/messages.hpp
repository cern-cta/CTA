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

#include "common/exception/Exception.hpp"
#include "mediachanger/Constants.hpp"
#include "mediachanger/Frame.hpp"
#include "mediachanger/Header.pb.h"
#include "mediachanger/ZmqMsg.hpp"
#include "mediachanger/ZmqSocket.hpp"

#include <openssl/rsa.h>
#include <openssl/evp.h>
#include <openssl/err.h>
#include <openssl/pem.h>

namespace cta {
namespace mediachanger {
  
/**
 * Sends the specified message frame over the specified socket.
 *
 * @param socket The ZMQ socket.
 * @param frame The message frame.
 */
void sendFrame(ZmqSocket &socket, const Frame &frame);

/**
 * Receives a message frame from the specified socket.
 *
 * @param socket The ZMQ socket.
 * @return The message frame.
 */
Frame recvFrame(ZmqSocket &socket);

/**
 * Function to compute inn Base64 the SHA1 of a given buffer
 * @param data The data
 * @param len, the length of the buffer
 * @return the base64 sha1 of the serialized buffer
 */
std::string computeSHA1Base64(const std::string &data);

/**
 * Function to compute inn Base64 the SHA1 of a given ProtoBuff message.
 * All fields must be set because we are going to serialize the message.
 * @param msg
 * @return the base64 sha1 of the serialized body
 */
std::string computeSHA1Base64(const google::protobuf::Message& msg);

/**
 * Function to compute inn Base64 the SHA1 of a given buffer
 * @param data The data
 * @param len, the length of the buffer
 * @return the base64 sha1 of the serialized buffer
 */
std::string computeSHA1Base64(void const* const data, const int len);

/**
 * Connects the specified ZMQ socket to localhost on the given TCP/IP port.
 *
 * @param socket The ZMQ socket.
 * @param port The TCP/IP port.
 */
void connectZmqSocketToLocalhost(ZmqSocket &socket, const int port);

/**
 * Header factory which pre fill several fields
 * magic(TPMAGIC)
 * protocoltype(protocolType);
 * protocolversion(protocolVersion;
 * bodyhashtype("SHA1");
 * bodysignaturetype("SHA1");
 *  After, the only  fields left are reqtype, bodyhashvalue and bodyhashsignature
 * @return The header
 */
template <int magic, int protocolType, int protocolVersion> Header genericPreFillHeader() {
  Header header;
  header.set_magic(magic);
  header.set_protocoltype(protocolType);
  header.set_protocolversion(protocolVersion);
  header.set_bodyhashtype("SHA1");
  header.set_bodysignaturetype("SHA1");
  return header;
}

/**
 * Return genericPreFillHeader() with
 * protocolType = <protocolType::Tape
 * protocolVersion = protocolVersion::prototype
 * @return 
 */
Header protoTapePreFillHeader();

/**
 * Receives either a good-day reply-message or an exception message from the
 * specified ZMQ socket.
 *
 * If a good-day reply-message is read from the socket then it's body is parsed
 * into the specified Google protocol-buffer.
 *
 * If an exception message is read from the socket then it is converted into a
 * a C++ exception and thrown.
 *
 * @param socket The ZMQ socket.
 * @param body Output parameter: The body of the good-day reply-message in the
 * form of a Google protocol-buffer.
 */
void recvTapeReplyOrEx(ZmqSocket& socket, google::protobuf::Message &body);

/**
 * Receives either a good-day reply-message or an exception message from the
 * specified ZMQ socket.
 *
 * If a good-day reply-message is read from the socket then it's body is parsed
 * into the specified Google protocol-buffer.
 *
 * If an exception message is read from the socket then it is converted into a
 * a C++ exception and thrown.
 *
 * @param socket The ZMQ socket.
 * @param body Output parameter: The body of the good-day reply-message in the
 * form of a Google protocol-buffer.
 * @param magic The expected magic number of the message.
 * @param protocolType The expected protocol type of the message.
 * @param protocolVersion The expected protocol version of the message.
 */
void recvReplyOrEx(ZmqSocket& socket,
  google::protobuf::Message &body,
  const uint32_t magic, const uint32_t protocolType,
  const uint32_t protocolVersion);

/**
 * Returns the string representation of the specified ZMQ errno.
 *
 * This method does not throw an exception if the specified errno is unknown.
 * Instead the method returns a string explaining that the errno is unknown.
 */
std::string zmqErrnoToStr(const int zmqErrno);

} // namespace mediachanger
} // namespace cta
