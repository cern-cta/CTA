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

#include "common/SmartFILEPtr.hpp"

#include <errno.h>
#include <unistd.h>

namespace cta {

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
SmartFILEPtr::SmartFILEPtr() throw() : m_file(nullptr) {}

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
SmartFILEPtr::SmartFILEPtr(FILE* const file) throw() : m_file(file) {}

//-----------------------------------------------------------------------------
// reset
//-----------------------------------------------------------------------------
void SmartFILEPtr::reset(FILE* const file) throw() {
  // If the new pointer is not the one already owned
  if (file != m_file) {
    // If this smart pointer still owns a pointer, then fclose it
    if (m_file != nullptr) {
      fclose(m_file);
    }

    // Take ownership of the new pointer
    m_file = file;
  }
}

//-----------------------------------------------------------------------------
// SmartFILEPtr assignment operator
//-----------------------------------------------------------------------------
SmartFILEPtr& SmartFILEPtr::operator=(SmartFILEPtr& obj) {
  reset(obj.release());
  return *this;
}

//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
SmartFILEPtr::~SmartFILEPtr() throw() {
  reset();
}

//-----------------------------------------------------------------------------
// get
//-----------------------------------------------------------------------------
FILE* SmartFILEPtr::get() const throw() {
  return m_file;
}

//-----------------------------------------------------------------------------
// release
//-----------------------------------------------------------------------------
FILE* SmartFILEPtr::release() {
  // If this smart pointer does not own a pointer
  if (nullptr == m_file) {
    cta::exception::NotAnOwner ex;
    ex.getMessage() << "Smart pointer does not own a FILE pointer";
    throw ex;
  }

  FILE* const tmp = m_file;

  // A nullptr value indicates this smart pointer does not own a pointer
  m_file = nullptr;

  return tmp;
}

}  // namespace cta
