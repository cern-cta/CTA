/******************************************************************************
 *         castor/tape/tapeserver/daemon/SmartCap.cpp
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/exception/Exception.hpp"
#include "castor/tape/tapeserver/daemon/SmartCap.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::SmartCap::SmartCap() throw():
  m_cap(NULL) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::SmartCap::SmartCap(cap_t cap) throw():
  m_cap(cap) {
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void castor::tape::tapeserver::daemon::SmartCap::reset(cap_t cap) throw() {
  // If the new capability state is not the one already owned
  if(cap != m_cap) {

    // If this smart pointer still owns a capability state then free it using
    // cap_free()
    if(NULL != m_cap) {
      cap_free(m_cap);
    }

    // Take ownership of the new capability state
    m_cap = cap;
  }
}

//------------------------------------------------------------------------------
// operator=
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::SmartCap
  &castor::tape::tapeserver::daemon::SmartCap::operator=(SmartCap& obj) {
  reset(obj.release());
  return *this;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::SmartCap::~SmartCap() throw() {
  reset();
}

//------------------------------------------------------------------------------
// get
//------------------------------------------------------------------------------
cap_t castor::tape::tapeserver::daemon::SmartCap::get() const throw() {
  return m_cap;
}

//------------------------------------------------------------------------------
// release
//------------------------------------------------------------------------------
cap_t castor::tape::tapeserver::daemon::SmartCap::release() {
  // If this smart pointer does not own a capbility state
  if(NULL == m_cap) {
    castor::exception::Exception ex;
    ex.getMessage() << "Smart pointer does not own a capbility state";
    throw(ex);
  }

  // Assigning NULL to m_cap indicates this smart pointer does not own a
  // capability state
  cap_t tmpCap = m_cap;
  m_cap = NULL;
  return tmpCap;
}
