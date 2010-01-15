/******************************************************************************
 *                      castor/tape/utils/SmartFd.cpp
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
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/tape/utils/SmartFd.hpp"
#include "castor/tape/utils/utils.hpp"

#include <errno.h>
#include <unistd.h>


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::utils::SmartFd::SmartFd() throw():
  m_fd(-1), m_closedCallback(NULL) {
}


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::utils::SmartFd::SmartFd(const int fd) throw():
  m_fd(fd), m_closedCallback(NULL) {
}


//-----------------------------------------------------------------------------
// setClosedCallback
//-----------------------------------------------------------------------------
void castor::tape::utils::SmartFd::setClosedCallback(const int fd,
  ClosedCallback closedCallback) throw() {

  m_closedCallback = closedCallback;
}


//-----------------------------------------------------------------------------
// reset
//-----------------------------------------------------------------------------
void castor::tape::utils::SmartFd::reset(const int fd = -1) throw() {
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
castor::tape::utils::SmartFd
  &castor::tape::utils::SmartFd::operator=(SmartFd& obj) throw() {
  
  reset(obj.release());

  return *this;
}


//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::tape::utils::SmartFd::~SmartFd() {

  reset();
}


//-----------------------------------------------------------------------------
// get
//-----------------------------------------------------------------------------
int castor::tape::utils::SmartFd::get() throw() {

  return m_fd;
}


//-----------------------------------------------------------------------------
// release
//-----------------------------------------------------------------------------
int castor::tape::utils::SmartFd::release()
  throw(castor::exception::Exception) {

  // If this SmartFd does not own a file descriptor
  if(m_fd < 0) {
    TAPE_THROW_CODE(EPERM,
      ": Smart file descriptor does not own a basic file descriptor");
  }


  const int tmpFd = m_fd;

  // A negative number indicates this SmartFd does not own a file descriptor
  m_fd = -1;

  return tmpFd;
}
