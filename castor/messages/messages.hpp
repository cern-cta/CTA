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
 *  Semd the google protobuf message msg over the socket. The flasg is passed to socket
 */
template <class T> void sendMessage(ZmqSocket& socket,const T& msg,int flag=0) {

  if(!msg.IsInitialized()){
    castor::exception::Exception ex("the protocol buffer message was not correctly set");
    throw ex;
  }

  const int size=msg.ByteSize();
  ZmqMsg blob(size);
  msg.SerializeToArray(zmq_msg_data(&blob.getZmqMsg()),size);
  socket.send(&blob.getZmqMsg(), flag);
}

/**
 * Sends the specified message frame over the specified socket.
 *
 * @param socket The ZMQ socket.
 * @param frame The message frame.
 */
void sendFrame(ZmqSocket& socket, const Frame &frame);

/**
 * Receives a message frame from the specified socket.
 *
 * @param socket The ZMQ socket.
 * @return The message frame.
 */
Frame recvFrame(ZmqSocket& socket);

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
 * Connect the socket to localhost on the givent port 
 * @param socket
 */
void connectToLocalhost(ZmqSocket&  socket,int port);

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
  castor::messages::Header protoTapePreFillHeader();

} // namespace messages
} // namespace castor
