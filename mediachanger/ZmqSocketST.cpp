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

#include "common/exception/Exception.hpp"
#include "messages.hpp"
#include "ZmqSocketST.hpp"
#include "ZmqSocket.hpp"

namespace cta {
namespace mediachanger {
//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
ZmqSocketST::ZmqSocketST(void *const zmqContext, const int socketType) {
  m_zmqSocket = zmq_socket (zmqContext, socketType);
  if (NULL == m_zmqSocket) {
    const int savedErrno = errno;
    cta::exception::Exception ex;
    ex.getMessage() << "zmq_socket() failed: " << zmqErrnoToStr(savedErrno);
    throw ex;
  }
}
  
//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
ZmqSocketST::~ZmqSocketST() {
  try {
    close();
  } catch(...) {
    // Ignore any exceptions because this is a destructor.
  }
}
  
//------------------------------------------------------------------------------
// close
//------------------------------------------------------------------------------
void ZmqSocketST::close() {
  if(m_zmqSocket == NULL) {
    return;
  }

  if(zmq_close(m_zmqSocket)) {
    const int savedErrno = errno;
    cta::exception::Exception ex;
    ex.getMessage() << "zmq_close() failed: " << zmqErrnoToStr(savedErrno);
    throw ex;
  }
  m_zmqSocket = NULL;
}
    
//------------------------------------------------------------------------------
// bind
//------------------------------------------------------------------------------
void ZmqSocketST::bind (const std::string &endpoint) {
  if(zmq_bind(m_zmqSocket, endpoint.c_str())) {
    const int savedErrno = errno;
    cta::exception::Exception ex;
    ex.getMessage() << "zmq_bind failed(): " << zmqErrnoToStr(savedErrno);
    throw ex;
  }
}
  
//------------------------------------------------------------------------------
// connect
//------------------------------------------------------------------------------
void ZmqSocketST::connect(const std::string &endpoint) {
  if(zmq_connect(m_zmqSocket, endpoint.c_str())) {
    const int savedErrno = errno;
    cta::exception::Exception ex;
    ex.getMessage() << "zmq_connect() failed: " << zmqErrnoToStr(savedErrno);
    throw ex;
  }
}

//------------------------------------------------------------------------------
// send
//------------------------------------------------------------------------------
void ZmqSocketST::send(ZmqMsg &msg, const int flags) {
  send(&msg.getZmqMsg(), flags);
}
  
//------------------------------------------------------------------------------
// send
//------------------------------------------------------------------------------
void ZmqSocketST::send(zmq_msg_t *const msg, const int flags) {
  if(-1 == zmq_msg_send(msg, m_zmqSocket, flags)) {
    const int savedErrno = errno;
    cta::exception::Exception ex;
    ex.getMessage() << "zmq_msg_send() failed: " << zmqErrnoToStr(savedErrno);
    throw ex;
  }
}

//------------------------------------------------------------------------------
// recv
//------------------------------------------------------------------------------
void ZmqSocketST::recv(ZmqMsg &msg, const int flags) {
  recv(&msg.getZmqMsg(), flags);
}

//------------------------------------------------------------------------------
// recv
//------------------------------------------------------------------------------
void ZmqSocketST::recv(zmq_msg_t *const msg, int flags) {
  if(-1 == zmq_msg_recv (msg, m_zmqSocket, flags)) {
    const int savedErrno = errno;
    cta::exception::Exception ex;
    ex.getMessage() << "zmq_msg_recv() failed: " << zmqErrnoToStr(savedErrno);
    throw ex;
  }
}

//------------------------------------------------------------------------------
// getZmqSocket
//------------------------------------------------------------------------------
void *ZmqSocketST::getZmqSocket() const {
  return m_zmqSocket;
}

} // namespace mediachanger
} // namespace cta
