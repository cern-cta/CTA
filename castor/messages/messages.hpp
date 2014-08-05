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

#include "castor/exception/Exception.hpp"
#include "castor/messages/Constants.hpp"
#include "castor/messages/Exception.pb.h"
#include "castor/messages/Frame.hpp"
#include "castor/messages/Header.pb.h"
#include "castor/messages/ZmqMsg.hpp"
#include "castor/messages/ZmqSocket.hpp"
#include "castor/tape/tapeserver/daemon/Constants.hpp"
#include "h/Ctape.h"

#include <openssl/rsa.h>
#include <openssl/evp.h>
#include <openssl/err.h>
#include <openssl/pem.h>

namespace castor {
namespace messages {
  
/**
 * Sends the specified message frame over the specified socket.
 *
 * @param socket The ZMQ socket.
 * @param frame The message frame.
 */
template <class T> void sendFrame(T& socket, const Frame &frame) {
  try {
    // Prepare header
    ZmqMsg header(frame.header.ByteSize());
    frame.serializeHeaderToZmqMsg(header);

    // Prepare body
    ZmqMsg body(frame.body.length());
    memcpy(body.getData(), frame.body.c_str(), body.size());

    // Send header and body as a two part ZMQ message
    socket.send(header, ZMQ_SNDMORE);
    socket.send(body, 0);

  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to send message frame: " <<
      ne.getMessage().str();
    throw ex;
  }
}

/**
 * Receives a message frame from the specified socket.
 *
 * @param socket The ZMQ socket.
 * @return The message frame.
 */
template <class T> Frame recvFrame(T& socket) {
  try {
    ZmqMsg header;
    try {
      socket.recv(header);
    } catch(castor::exception::Exception &ne) {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to receive header of message frame: " <<
        ne.getMessage().str();
      throw ex;
    }

    if(!header.more()){
      castor::exception::Exception ex;
      ex.getMessage() << "No message body after receiving the header";
      throw ex;
    }

    Frame frame;
    frame.parseZmqMsgIntoHeader(header);

    ZmqMsg body;
    try {
      socket.recv(body);
    } catch(castor::exception::Exception &ne) {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to receive body of message frame: " <<
        ne.getMessage().str();
      throw ex;
    }

    frame.body = std::string((const char *)body.getData(), body.size());

    frame.checkHashValueOfBody();

    return frame;

  } catch(castor::exception::Exception &ne) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to receive message frame: " <<
      ne.getMessage().str();
    throw ex;
  }
}

/**
 * Function to compute inn Base64 the SHA1 of a given buffer
 * @param data The data
 * @param len, the length of the buffer
 * @return the base64 sha1 of the serialized buffer
 */
std::string computeSHA1Base64(const std::string &data);

/**
 * Function to compute inn Base64 the SHA1 of a given buffer
 * @param data The data
 * @param len, the length of the buffer
 * @return the base64 sha1 of the serialized buffer
 */
std::string computeSHA1Base64(void const* const data, const int len);

/**
 * Template function to compute inn Base64 the SHA1 of a given ProtoBuff message
 * All fields  have to be set because we are going to serialize the message
 * @param msg
 * @return the base64 sha1 of the serialized body
 */
template <class T> std::string computeSHA1Base64(const T& msg) {
  std::string buffer;
  msg.SerializeToString(&buffer);
  return computeSHA1Base64(buffer.c_str(),buffer.size());
}

/**
 * Connects the specified ZMQ socket to localhost on the given TCP/IP port.
 *
 * @param socket The ZMQ socket.
 * @param port The TCP/IP port.
 */
template<class T> void connectZmqSocketToLocalhost(T& socket, const int port) {
  std::string bindingAdress("tcp://127.0.0.1:");
  bindingAdress+=castor::utils::toString(port);
  socket.connect(bindingAdress.c_str());
}

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
template <int magic, int protocolType, int protocolVersion>
castor::messages::Header genericPreFillHeader(){
  castor::messages::Header header;
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
template<class ZS, class PB> void recvTapeReplyOrEx(ZS& socket, PB &body) {
  recvReplyOrEx(socket, body, TPMAGIC, PROTOCOL_TYPE_TAPE, PROTOCOL_VERSION_1);
}

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
template<class ZS, class PB> void recvReplyOrEx(ZS& socket, PB &body,
  const uint32_t magic, const uint32_t protocolType,
  const uint32_t protocolVersion) {
  const Frame frame = recvFrame(socket);

  // Check the magic number
  if(magic != frame.header.magic()) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to receive message"
      ": Unexpected magic number: excpected=" << magic << " actual= " <<
      frame.header.magic();
    throw ex;
  }

  // Check the protocol type
  if(protocolType != frame.header.protocoltype()) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to receive message"
      ": Unexpected protocol type: excpected=" << protocolType << " actual= "
      << frame.header.protocoltype();
    throw ex;
  }

  // Check the protocol version
  if(protocolVersion != frame.header.protocolversion()) {
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to receive message"
      ": Unexpected protocol version: excpected=" << protocolVersion <<
      " actual= " << frame.header.protocolversion();
    throw ex;
  }

  // If an exception message was received
  if(messages::MSG_TYPE_EXCEPTION == frame.header.msgtype()) {
    // Convert it into a C++ exception and throw it
    messages::Exception exMsg;
    frame.parseBodyIntoProtocolBuffer(exMsg);
    castor::exception::Exception ex(exMsg.code());
    ex.getMessage() << exMsg.message();
    throw ex;
  }

  frame.parseBodyIntoProtocolBuffer(body);
}

} // namespace messages
} // namespace castor
