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
#include "castor/messages/messages.hpp"
#include "castor/messages/ZmqSocketST.hpp"
#include "h/serrno.h"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::messages::ZmqSocketST::ZmqSocketST(void *const zmqContext,
  const int socketType) {
  m_zmqSocket = zmq_socket (zmqContext, socketType);
  if (NULL == m_zmqSocket) {
    const int savedErrno = errno;
    castor::exception::Exception ex;
    ex.getMessage() << "zmq_socket() failed: " << zmqErrnoToStr(savedErrno);
    throw ex;
  }
}
  
//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::messages::ZmqSocketST::~ZmqSocketST() throw() {
  try {
    close();
  } catch(...) {
    // Ignore any exceptions because this is a destructor.
  }
}
  
//------------------------------------------------------------------------------
// close
//------------------------------------------------------------------------------
void castor::messages::ZmqSocketST::close() {
  if(m_zmqSocket == NULL) {
    return;
  }

  if(zmq_close(m_zmqSocket)) {
    const int savedErrno = errno;
    castor::exception::Exception ex;
    ex.getMessage() << "zmq_close() failed: " << zmqErrnoToStr(savedErrno);
    throw ex;
  }
  m_zmqSocket = NULL;
}
    
//------------------------------------------------------------------------------
// bind
//------------------------------------------------------------------------------
void castor::messages::ZmqSocketST::bind (const std::string &endpoint) {
  if(zmq_bind(m_zmqSocket, endpoint.c_str())) {
    const int savedErrno = errno;
    castor::exception::Exception ex;
    ex.getMessage() << "zmq_bind failed(): " << zmqErrnoToStr(savedErrno);
    throw ex;
  }
}
  
//------------------------------------------------------------------------------
// connect
//------------------------------------------------------------------------------
void castor::messages::ZmqSocketST::connect(const std::string &endpoint) {
  if(zmq_connect(m_zmqSocket, endpoint.c_str())) {
    const int savedErrno = errno;
    castor::exception::Exception ex;
    ex.getMessage() << "zmq_connect() failed: " << zmqErrnoToStr(savedErrno);
    throw ex;
  }
}

//------------------------------------------------------------------------------
// send
//------------------------------------------------------------------------------
void castor::messages::ZmqSocketST::send(ZmqMsg &msg, const int flags) {
  send(&msg.getZmqMsg(), flags);
}
  
//------------------------------------------------------------------------------
// send
//------------------------------------------------------------------------------
void castor::messages::ZmqSocketST::send(zmq_msg_t *const msg, const int flags) {
  if(-1 == zmq_msg_send(msg, m_zmqSocket, flags)) {
    const int savedErrno = errno;
    castor::exception::Exception ex;
    ex.getMessage() << "zmq_msg_send() failed: " << zmqErrnoToStr(savedErrno);
    throw ex;
  }
}

//------------------------------------------------------------------------------
// recv
//------------------------------------------------------------------------------
void castor::messages::ZmqSocketST::recv(ZmqMsg &msg, const int flags) {
  recv(&msg.getZmqMsg(), flags);
}

//------------------------------------------------------------------------------
// recv
//------------------------------------------------------------------------------
void castor::messages::ZmqSocketST::recv(zmq_msg_t *const msg, int flags) {
  if(-1 == zmq_msg_recv (msg, m_zmqSocket, flags)) {
    const int savedErrno = errno;
    castor::exception::Exception ex;
    ex.getMessage() << "zmq_msg_recv() failed: " << zmqErrnoToStr(savedErrno);
    throw ex;
  }
}

//------------------------------------------------------------------------------
// getZmqSocket
//------------------------------------------------------------------------------
void *castor::messages::ZmqSocketST::getZmqSocket() const throw() {
  return m_zmqSocket;
}
