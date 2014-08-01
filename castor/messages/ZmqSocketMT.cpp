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
#include "castor/messages/ZmqSocketMT.hpp"
#include "castor/utils/ScopedLock.hpp"
#include "h/serrno.h"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::messages::ZmqSocketMT::ZmqSocketMT(void *const zmqContext,
  const int socketType): ZmqSocket(zmqContext, socketType) {
  const int rc = pthread_mutex_init(&m_mutex, NULL);
  if(0 != rc) {
    char message[100];
    sstrerror_r(rc, message, sizeof(message));
    castor::exception::Exception ex;
    ex.getMessage() << "Failed to create ZmqSocketMT: Failed to create mutex: "
      << message;
    throw ex;
  }
}
  
//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::messages::ZmqSocketMT::~ZmqSocketMT() throw() {
  pthread_mutex_destroy(&m_mutex);
}
  
//------------------------------------------------------------------------------
// close
//------------------------------------------------------------------------------
void castor::messages::ZmqSocketMT::close() {
  utils::ScopedLock lock(m_mutex);
  ZmqSocket::close();
}
    
//------------------------------------------------------------------------------
// bind
//------------------------------------------------------------------------------
void castor::messages::ZmqSocketMT::bind (const std::string &endpoint) {
  utils::ScopedLock lock(m_mutex);
  ZmqSocket::bind(endpoint);
}
  
//------------------------------------------------------------------------------
// connect
//------------------------------------------------------------------------------
void castor::messages::ZmqSocketMT::connect(const std::string &endpoint) {
  utils::ScopedLock lock(m_mutex);
  ZmqSocket::connect(endpoint);
}

//------------------------------------------------------------------------------
// send
//------------------------------------------------------------------------------
void castor::messages::ZmqSocketMT::send(ZmqMsg &msg, const int flags) {
  utils::ScopedLock lock(m_mutex);
  ZmqSocket::send(msg, flags);
}
  
//------------------------------------------------------------------------------
// send
//------------------------------------------------------------------------------
void castor::messages::ZmqSocketMT::send(zmq_msg_t *const msg,
  const int flags) {
  utils::ScopedLock lock(m_mutex);
  ZmqSocket::send(msg, flags);
}

//------------------------------------------------------------------------------
// recv
//------------------------------------------------------------------------------
void castor::messages::ZmqSocketMT::recv(ZmqMsg &msg, const int flags) {
  utils::ScopedLock lock(m_mutex);
  ZmqSocket::recv(msg, flags);
}

//------------------------------------------------------------------------------
// recv
//------------------------------------------------------------------------------
void castor::messages::ZmqSocketMT::recv(zmq_msg_t *const msg, int flags) {
  utils::ScopedLock lock(m_mutex);
  ZmqSocket::recv(msg, flags);
}

//------------------------------------------------------------------------------
// getZmqSocket
//------------------------------------------------------------------------------
void *castor::messages::ZmqSocketMT::getZmqSocket() const throw() {
  return ZmqSocket::getZmqSocket();
}
