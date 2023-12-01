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

#include "common/SmartFd.hpp"

#include <errno.h>
#include <unistd.h>

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
cta::SmartFd::SmartFd() noexcept:
  m_fd(-1), m_closedCallback(nullptr) {
}

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
cta::SmartFd::SmartFd(const int fd) noexcept:
  m_fd(fd), m_closedCallback(nullptr) {
}

//-----------------------------------------------------------------------------
// setClosedCallback
//-----------------------------------------------------------------------------
void cta::SmartFd::setClosedCallback(ClosedCallback closedCallback)
  noexcept {
  m_closedCallback = closedCallback;
}

//-----------------------------------------------------------------------------
// reset
//-----------------------------------------------------------------------------
void cta::SmartFd::reset(const int fd = -1) noexcept {
  // If the new file descriptor is not the one already owned
  if(fd != m_fd) {

    // If this SmartFd still owns a file descriptor, then close it
    if(m_fd >= 0) {
      close(m_fd);
      if(m_closedCallback) {
        try {
           (*m_closedCallback)(m_fd);
        } catch(...) {
           // Ignore any exception thrown my the m_closedCallback function
           // because this reset function maybe called by the destructor of
           // SmartFd
        }
      }
    }

    // Take ownership of the new file descriptor
    m_fd = fd;
  }
}

//-----------------------------------------------------------------------------
// SmartFd assignment operator
//-----------------------------------------------------------------------------
cta::SmartFd &cta::SmartFd::operator=(SmartFd& obj) {
  reset(obj.release());
  return *this;
}

//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
cta::SmartFd::~SmartFd() {
  reset();
}

//-----------------------------------------------------------------------------
// get
//-----------------------------------------------------------------------------
int cta::SmartFd::get() const noexcept {
  return m_fd;
}

//-----------------------------------------------------------------------------
// release
//-----------------------------------------------------------------------------
int cta::SmartFd::release()  {
  // If this SmartFd does not own a file descriptor
  if(m_fd < 0) {
    cta::exception::NotAnOwner ex;
    ex.getMessage() << "Smart file-descriptor does not own a file-descriptor";
    throw ex;
  }

  const int tmpFd = m_fd;

  // A negative number indicates this SmartFd does not own a file descriptor
  m_fd = -1;

  return tmpFd;
}
