/*
 * The CERN Tape Archive(CTA) project
 * Copyright(C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *(at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "mediachanger/SmartZmqContext.hpp"

#include <errno.h>
#include <unistd.h>
#include <zmq.h>

namespace cta {
namespace mediachanger {

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
SmartZmqContext::SmartZmqContext() throw() :
  m_zmqContext(NULL) {
}

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
SmartZmqContext::SmartZmqContext(void *const zmqContext)
  throw() : m_zmqContext(zmqContext) {
}

//-----------------------------------------------------------------------------
// reset
//-----------------------------------------------------------------------------
void SmartZmqContext::reset(void *const zmqContext)
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
SmartZmqContext
  &SmartZmqContext::operator=(SmartZmqContext& obj) {
  reset(obj.release());
  return *this;
}

//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
SmartZmqContext::~SmartZmqContext() throw() {
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
void *SmartZmqContext::get() const throw() {
  return m_zmqContext;
}

//-----------------------------------------------------------------------------
// release
//-----------------------------------------------------------------------------
void *SmartZmqContext::release() {
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

} // namespace mediachanger
} // namespace cta
