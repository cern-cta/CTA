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
#include "strerror_r_wrapper.h"

#include <string.h>
#include <zmq.h>

//------------------------------------------------------------------------------
// sendFrame
//------------------------------------------------------------------------------
void castor::messages::sendFrame(ZmqSocket &socket, const Frame &frame) {
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
castor::messages::Frame castor::messages::recvFrame(ZmqSocket &socket) {
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
// connectZmqSocketToLocalhost
//------------------------------------------------------------------------------
void castor::messages::connectZmqSocketToLocalhost(ZmqSocket &socket,
  const int port) {
  std::string bindingAdress("tcp://127.0.0.1:");
  bindingAdress += castor::utils::toString(port);
  socket.connect(bindingAdress.c_str());
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
std::string castor::messages::computeSHA1Base64(
  const google::protobuf::Message& msg) {
  std::string buffer;
  msg.SerializeToString(&buffer);
  return computeSHA1Base64(buffer.c_str(),buffer.size());
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

//-----------------------------------------------------------------------------
// recvReplyOrEx
//-----------------------------------------------------------------------------
void castor::messages::recvTapeReplyOrEx(ZmqSocket& socket,
  google::protobuf::Message &body) {
  recvReplyOrEx(socket, body, TPMAGIC, PROTOCOL_TYPE_TAPE, PROTOCOL_VERSION_1);
}

//-----------------------------------------------------------------------------
// recvReplyOrEx
//-----------------------------------------------------------------------------
void castor::messages::recvReplyOrEx(ZmqSocket& socket,
  google::protobuf::Message &body,
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

//------------------------------------------------------------------------------
// zmqErrnoToStr
//------------------------------------------------------------------------------
std::string castor::messages::zmqErrnoToStr(const int zmqErrno) {
  switch(zmqErrno) {
  // Translate the values that are specific to ZMQ
  case EFSM:
    return "Operation cannot be accomplished in current state";
  case ENOCOMPATPROTO:
    return "The protocol is not compatible with the socket type";
  case ETERM:
    return "Context was terminated";
  case EMTHREAD:
    return "No thread available";

  // Translate the values that are not specific to ZMQ
  default:
    {
      char errorCStr[100];
      const int rc = strerror_r_wrapper(zmqErrno, errorCStr, sizeof(errorCStr));

      // If strerror_r_wrapper() failed to translate
      if(rc) {
        std::ostringstream oss;
        oss << "Failed to translate ZMQ errno";

        switch(errno) {
        case EINVAL:
          oss << ": Unknown ZMQ errno";
          break;
        case ERANGE:
          oss << ": strerror_r_wrapper() given too small a buffer";
          break;
        default:
          oss << ": Unknown reason";
        }

        oss << ": zmqErrno=" << zmqErrno;
        return oss.str();
      }

      // strerror_r_wrapper() succeeded to translate
      errorCStr[sizeof(errorCStr) -1] = '\0'; // Being paranoid
      return errorCStr;
    }
  }
}
