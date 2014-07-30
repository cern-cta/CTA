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
#include "castor/tape/tapeserver/daemon/Constants.hpp"
#include "castor/tape/utils/ZmqMsg.hpp"
#include "castor/tape/utils/ZmqSocket.hpp"
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
template <class T> void sendMessage(tape::utils::ZmqSocket& socket,const T& msg,int flag=0) {

  if(!msg.IsInitialized()){
    castor::exception::Exception ex("the protocol buffer message was not correctly set");
    throw ex;
  }

  const int size=msg.ByteSize();
  tape::utils::ZmqMsg blob(size);
  msg.SerializeToArray(zmq_msg_data(&blob.getZmqMsg()),size);
  socket.send(&blob.getZmqMsg(), flag);
}

/**
 * Template function to compute inn Base64 the SHA1 of a given buffer
 * @param data The data
 * @param len, the length of the buffer
 * @return the base64 sha1 of the serialized buffer
 */
std::string computeSHA1Base64(void const* const data,int len);

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
 * Check if the sha1 one computed from the body match the one from the header
 * If not, the function throws an exception
 * @param header
 * @param body
 */
template <class T> void checkSHA1(Header header,const T& body){
  const std::string bodyHash = castor::messages::computeSHA1Base64(body);
  if(bodyHash != header.bodyhashvalue()){
      std::ostringstream out;
      out<<"SHA1 mismatch between the one in the header("<<header.bodyhashvalue() 
         <<") and the one computed from the body("<<bodyHash<<")";
    throw castor::exception::Exception(out.str());
    }
}

/**
 * Check if the sha1 one computed from the body match the one from the header
 * If not, the function throws an exception
 * @param header
 * @param body
 */
void checkSHA1(Header header,const castor::tape::utils::ZmqMsg& body);

/**
 * Connect the socket to localhost on the givent port 
 * @param socket
 */
void connectToLocalhost(tape::utils::ZmqSocket&  socket,int port);

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
template <int protocolType, int protocolVersion>
castor::messages::Header genericPreFillHeader(){
  castor::messages::Header header;
  header.set_magic(TPMAGIC);
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
