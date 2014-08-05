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

#include "castor/exception/Exception.hpp"
#include "castor/messages/Header.pb.h"
#include "castor/messages/Constants.hpp"
#include "castor/messages/Frame.hpp"
#include "castor/messages/ZmqMsg.hpp"
#include "castor/messages/ZmqSocket.hpp"
#include "castor/tape/tapeserver/daemon/Constants.hpp"
#include "h/Ctape.h"
#include "castor/exception/Exception.hpp"

#include <openssl/rsa.h>
#include <openssl/evp.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#pragma once 

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

} // namespace messages
} // namespace castor
