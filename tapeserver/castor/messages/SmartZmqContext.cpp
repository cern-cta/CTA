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
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "tapeserver/castor/messages/SmartZmqContext.hpp"
#include "tapeserver/castor/utils/utils.hpp"
#include "common/log/Logger.hpp"

#include <errno.h>
#include <unistd.h>
#include <zmq.h>
#include <list>

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::messages::SmartZmqContext::SmartZmqContext() throw() :
  m_zmqContext(NULL) {
}

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::messages::SmartZmqContext::SmartZmqContext(void *const zmqContext)
  throw() : m_zmqContext(zmqContext) {
}

//-----------------------------------------------------------------------------
// reset
//-----------------------------------------------------------------------------
void castor::messages::SmartZmqContext::reset(void *const zmqContext)
  throw() {
  // If the new ZMQ context is not the one already owned
  if(zmqContext != m_zmqContext) {

    // If this smart pointer still owns a ZMQ context, then terminate it
    if(m_zmqContext != NULL) {
      zmq_term(m_zmqContext);
    }

    // Take ownership of the new ZMQ context
    m_zmqContext = zmqContext;
  }
}

//-----------------------------------------------------------------------------
// SmartZmqContext assignment operator
//-----------------------------------------------------------------------------
castor::messages::SmartZmqContext
  &castor::messages::SmartZmqContext::operator=(SmartZmqContext& obj) {
  reset(obj.release());
  return *this;
}

//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::messages::SmartZmqContext::~SmartZmqContext() throw() {
  // ZMQ sends an abort on exit when cleaned up this way under some
  // circumstances, so we purposely do not clean up the context (zmq_term) and
  // leave a resource leak, which in our use case is one-off situation
  // per process, and it gets cleaned up on process termination, which happens
  // very soon after this destructor being called.
  //reset();
}

//-----------------------------------------------------------------------------
// get
//-----------------------------------------------------------------------------
void *castor::messages::SmartZmqContext::get() const throw() {
  return m_zmqContext;
}

//-----------------------------------------------------------------------------
// release
//-----------------------------------------------------------------------------
void *castor::messages::SmartZmqContext::release() {
  // If this smart pointer does not own a ZMQ context
  if(NULL == m_zmqContext) {
    cta::exception::NotAnOwner ex;
    ex.getMessage() << "Smart pointer does not own a ZMQ context";
    throw ex;
  }

  void *const tmp = m_zmqContext;

  // A NULL value indicates this smart pointer does not own a ZMQ context
  m_zmqContext = NULL;

  return tmp;
}

//-----------------------------------------------------------------------------
// release
//-----------------------------------------------------------------------------
void* castor::messages::SmartZmqContext::instantiateZmqContext(const int sizeOfIOThreadPoolForZMQ, cta::log::Logger& log) {
  void *const zmqContext = zmq_init(sizeOfIOThreadPoolForZMQ);
  if(NULL == zmqContext) {
    const std::string message = castor::utils::errnoToString(errno);
    cta::exception::Exception ex;
    ex.getMessage() << "Child of ProcessForker failed to instantiate ZMQ"
      " context: " << message;
    throw ex;
  }

  std::ostringstream contextAddr;
  contextAddr << std::hex << zmqContext;
  std::list<cta::log::Param> params = {cta::log::Param("contextAddr", contextAddr.str())};
  log(cta::log::INFO, "Child of ProcessForker instantiated a ZMQ context", params);

  return zmqContext;
}
