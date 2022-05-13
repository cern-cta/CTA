/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "common/exception/Exception.hpp"
#include "common/processCap/SmartCap.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::server::SmartCap::SmartCap() throw():
  m_cap(nullptr) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::server::SmartCap::SmartCap(cap_t cap) throw():
  m_cap(cap) {
}

//------------------------------------------------------------------------------
// reset
//------------------------------------------------------------------------------
void cta::server::SmartCap::reset(cap_t cap) throw() {
  // If the new capability state is not the one already owned
  if(cap != m_cap) {

    // If this smart pointer still owns a capability state then free it using
    // cap_free()
    if(nullptr != m_cap) {
      cap_free(m_cap);
    }

    // Take ownership of the new capability state
    m_cap = cap;
  }
}

//------------------------------------------------------------------------------
// operator=
//------------------------------------------------------------------------------
cta::server::SmartCap &cta::server::SmartCap::operator=(SmartCap& obj) {
  reset(obj.release());
  return *this;
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::server::SmartCap::~SmartCap() throw() {
  reset();
}

//------------------------------------------------------------------------------
// get
//------------------------------------------------------------------------------
cap_t cta::server::SmartCap::get() const throw() {
  return m_cap;
}

//------------------------------------------------------------------------------
// release
//------------------------------------------------------------------------------
cap_t cta::server::SmartCap::release() {
  // If this smart pointer does not own a capbility state
  if(nullptr == m_cap) {
    cta::exception::Exception ex;
    ex.getMessage() << "Smart pointer does not own a capbility state";
    throw(ex);
  }

  // Assigning nullptr to m_cap indicates this smart pointer does not own a
  // capability state
  cap_t tmpCap = m_cap;
  m_cap = nullptr;
  return tmpCap;
}
