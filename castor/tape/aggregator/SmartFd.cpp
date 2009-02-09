/******************************************************************************
 *                      SmartFd.cpp
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

#include "castor/tape/aggregator/SmartFd.hpp"

#include <errno.h>
#include <unistd.h>


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::aggregator::SmartFd::SmartFd(const int fileDescriptor) :
  m_fileDescriptor(fileDescriptor) {
}


//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::tape::aggregator::SmartFd::~SmartFd() {

  // If the smart file descriptor still owns a basic file descriptor
  if(m_fileDescriptor >= 0) {
    close(m_fileDescriptor);
  }

}


//-----------------------------------------------------------------------------
// get
//-----------------------------------------------------------------------------
int castor::tape::aggregator::SmartFd::get()
  throw(castor::exception::Exception) {

  // If the smart file descriptor no longer owns a basic file descriptor
  if(m_fileDescriptor < 0) {
    castor::exception::Exception ex(EPERM);

    ex.getMessage() << "It is not permitted to call get() after release()";

    throw ex;
  }

  return m_fileDescriptor;
}


//-----------------------------------------------------------------------------
// release
//-----------------------------------------------------------------------------
int castor::tape::aggregator::SmartFd::release()
  throw(castor::exception::Exception) {

  // If the smart file descriptor no longer owns a basic file descriptor
  if(m_fileDescriptor < 0) {
    castor::exception::Exception ex(EPERM);

    ex.getMessage() << "It is not permitted to call release more than once";

    throw ex;
  }


  const int tmpFileDescriptor = m_fileDescriptor;

  m_fileDescriptor = -1;

  return tmpFileDescriptor;
}
