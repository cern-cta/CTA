/******************************************************************************
 *                      castor/tape/utils/SmartFdList.cpp
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

#include "castor/exception/InvalidArgument.hpp"
#include "castor/tape/utils/SmartFdList.hpp"

#include <algorithm>
#include <errno.h>
#include <unistd.h>


//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::tape::utils::SmartFdList::~SmartFdList() {

  for(SmartFdList::iterator itor = begin(); itor != end(); itor++) {
    close(*itor);
  }
}


//-----------------------------------------------------------------------------
// push_back
//-----------------------------------------------------------------------------
void castor::tape::utils::SmartFdList::push_back(const int &fd)
  throw(castor::exception::Exception) {

  // Search the list for the file descriptor
  SmartFdList::iterator itor = std::find(begin(), end(), fd);

  // If the file descriptor is already in the list, then throw an exception
  if(itor != end()) {
    castor::exception::InvalidArgument ex;

    ex.getMessage() <<
      "File descriptor is already in the list"
      ": fd=" << fd;

    throw(ex);
  }

  // Append a copy of the file descriptor to the end of the list
  std::list<int>::push_back(fd);
}


//-----------------------------------------------------------------------------
// release
//-----------------------------------------------------------------------------
int castor::tape::utils::SmartFdList::release(const int fd)
  throw(castor::exception::Exception) {

  // Search the list for the file descriptor
  SmartFdList::iterator itor = std::find(begin(), end(), fd);

  // If the smart file descriptor list does not own the file descriptor
  if(itor == end()) {
    castor::exception::Exception ex(EPERM);

    ex.getMessage() <<
      "Smart file-descriptor does not own file descriptor"
      ": fd=" << fd;

    throw(ex);
  }

  // Remove the file descriptor from this list so that the destructor will not
  // close it
  remove(fd);

  return fd;
}
