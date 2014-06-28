/******************************************************************************
 *         castor/tape/utils/castorZmqWrapper.cpp
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
#include "castor/tape/utils/ZmqSocket.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::utils::ZmqSocket::ZmqSocket(void *const zmqContext,
  const int socketType) {
  m_zmqSocket = zmq_socket (zmqContext, socketType);
  if (NULL == m_zmqSocket) {
    char message[100];
    sstrerror_r(errno, message, sizeof(message));
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to create ZMQ socket: "
      << message;
    throw ex;
  }
}
  
//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::utils::ZmqSocket::~ZmqSocket() throw() {
  try {
    close();
  } catch(...) {
    // Ignore any exceptions because this is a destructor.
  }
}
  
//------------------------------------------------------------------------------
// close
//------------------------------------------------------------------------------
void castor::tape::utils::ZmqSocket::close() {
  if(m_zmqSocket == NULL) {
    return;
  }

  if(zmq_close (m_zmqSocket)) {
    char message[100];
    sstrerror_r(errno, message, sizeof(message));
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to close ZMQ socket: "
      << message;
    throw ex;
  }
  m_zmqSocket = NULL;
}
    
//------------------------------------------------------------------------------
// bind
//------------------------------------------------------------------------------
void castor::tape::utils::ZmqSocket::bind (const std::string &endpoint) {
  if(zmq_bind (m_zmqSocket, endpoint.c_str())) {
    char message[100];
    sstrerror_r(errno, message, sizeof(message));
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to bind ZMQ socket: "
      << message;
    throw ex;
  }
}
  
//------------------------------------------------------------------------------
// connect
//------------------------------------------------------------------------------
void castor::tape::utils::ZmqSocket::connect(const std::string &endpoint) {
  if(zmq_connect(m_zmqSocket, endpoint.c_str())) {
    char message[100];
    sstrerror_r(errno, message, sizeof(message));
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to connect ZMQ socket: "
      << message;
    throw ex;
  }
}
  
//------------------------------------------------------------------------------
// send
//------------------------------------------------------------------------------
void castor::tape::utils::ZmqSocket::send(zmq_msg_t *const msg, const int flags) {
  if(-1 == zmq_msg_send (msg, m_zmqSocket, flags)) {
    char message[100];
    sstrerror_r(errno, message, sizeof(message));
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to send ZMQ message: "
      << message;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// recv
//------------------------------------------------------------------------------
void castor::tape::utils::ZmqSocket::recv(zmq_msg_t *const msg, int flags) {
  if(-1 == zmq_msg_recv (msg, m_zmqSocket, flags)) {
    char message[100];
    sstrerror_r(errno, message, sizeof(message));
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to receive ZMQ message: "
      << message;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// getZmqSocket
//------------------------------------------------------------------------------
void *castor::tape::utils::ZmqSocket::getZmqSocket() const throw() {
  return m_zmqSocket;
}
