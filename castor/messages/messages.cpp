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

#include "castor/messages/messages.hpp"
#include "castor/utils/utils.hpp"

#include <string.h>

//------------------------------------------------------------------------------
// sendFrame
//------------------------------------------------------------------------------
void castor::messages::sendFrame(ZmqSocket& socket, const Frame &frame) {
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

//------------------------------------------------------------------------------
// recvFrame
//------------------------------------------------------------------------------
castor::messages::Frame castor::messages::recvFrame(ZmqSocket& socket) {
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

//------------------------------------------------------------------------------
// connectToLocalhost
//------------------------------------------------------------------------------
void castor::messages::connectToLocalhost(ZmqSocket& m_socket, const int port) {
  std::string bindingAdress("tcp://127.0.0.1:");
  bindingAdress+=castor::utils::toString(port);
  m_socket.connect(bindingAdress.c_str());
}

//------------------------------------------------------------------------------
// preFillHeader
//------------------------------------------------------------------------------
castor::messages::Header castor::messages::protoTapePreFillHeader() {
  return genericPreFillHeader<TPMAGIC, PROTOCOL_TYPE_TAPE,
    PROTOCOL_VERSION_1>();
}

//------------------------------------------------------------------------------
// computeSHA1Base64
//------------------------------------------------------------------------------
std::string castor::messages::computeSHA1Base64(const std::string &data) {
  return computeSHA1Base64(data.c_str(), data.length());
}

//------------------------------------------------------------------------------
// computeSHA1Base64
//------------------------------------------------------------------------------
std::string castor::messages::computeSHA1Base64(void const* const data,
  const int len){
  // Create a context and hash the data
  EVP_MD_CTX ctx;
  EVP_MD_CTX_init(&ctx);
  EVP_SignInit(&ctx, EVP_sha1());
  if (!EVP_SignUpdate(&ctx,data, len)) {
    EVP_MD_CTX_cleanup(&ctx);
    throw castor::exception::Exception("cant compute SHA1");
  }
  unsigned char md_value[EVP_MAX_MD_SIZE];
  unsigned int md_len;
  EVP_DigestFinal_ex(&ctx, md_value, &md_len);
  // cleanup context
  EVP_MD_CTX_cleanup(&ctx);
  
  // base64 encode 
  BIO *b64 = BIO_new(BIO_f_base64());
  BIO *bmem = BIO_new(BIO_s_mem());
  if (NULL == b64 || NULL == bmem) {
    throw castor::exception::Exception("cant set up the environnement for computing the SHA1 in base64");
  }
  b64 = BIO_push(b64, bmem);
  BIO_write(b64, md_value, md_len);
  (void)BIO_flush(b64);
  BUF_MEM* bptr;
  BIO_get_mem_ptr(b64, &bptr);

  std::string ret(bptr->data,bptr->length);
  BIO_free(bmem);
  BIO_free(b64);
  
  return ret;
}
