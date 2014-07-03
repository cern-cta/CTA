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
#include <openssl/evp.h>
#pragma once 

namespace castor {
namespace messages {

  
  struct ReplyContainer{
    castor::messages::Header header;
    tape::utils::ZmqMsg blobBody;
    ReplyContainer(tape::utils::ZmqSocket& socket);
  private :
    ReplyContainer(const ReplyContainer&);
    ReplyContainer& operator=(const ReplyContainer&);
  };
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

template <class T>std::string computeSHA1(const char* data,int len) {
  // Create a context and hash the data
  EVP_MD_CTX ctx;
  EVP_MD_CTX_init(&ctx);
  EVP_SignInit(&ctx, EVP_sha1());
  if (!EVP_SignUpdate(&ctx, data, len)) {
    EVP_MD_CTX_cleanup(&ctx);
    return "";
  }
    // cleanup context
  EVP_MD_CTX_cleanup(&ctx);
  return "";
}

void connectToLocalhost(tape::utils::ZmqSocket&  m_socket);
castor::messages::Header preFillHeader();
ReplyContainer readReplyMsg(tape::utils::ZmqSocket& socket);

} // namespace messages
} // namespace castor
