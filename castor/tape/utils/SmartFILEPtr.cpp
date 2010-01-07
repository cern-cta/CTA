/******************************************************************************
 *                      castor/tape/utils/SmartFILEPtr.cpp
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

#include "castor/tape/utils/SmartFILEPtr.hpp"
#include "castor/tape/utils/utils.hpp"

#include <errno.h>
#include <unistd.h>


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::utils::SmartFILEPtr::SmartFILEPtr() :
  m_file(NULL) {
}


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::utils::SmartFILEPtr::SmartFILEPtr(const FILE *const file) :
  m_file(file) {
}


//-----------------------------------------------------------------------------
// reset
//-----------------------------------------------------------------------------
void castor::tape::utils::SmartFILEPtr::reset(const FILE *const file = NULL)
   throw() {
  // If the new FILE pointer is not the one already owned
  if(file != m_file) {

    // If this SmartFILEPtr still owns a FILE pointer, then close it
    if(m_file != NULL) {
      close(m_file);
    }

    // Take ownership of the new FILE pointer
    m_file = file;
  }
}


//-----------------------------------------------------------------------------
// SmartFILEPtr assignment operator
//-----------------------------------------------------------------------------
castor::tape::utils::SmartFILEPtr
  &castor::tape::utils::SmartFILEPtr::operator=(SmartFILEPtr& obj) throw() {
  
  reset(obj.release());

  return *this;
}


//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::tape::utils::SmartFILEPtr::~SmartFILEPtr() {

  reset();
}


//-----------------------------------------------------------------------------
// get
//-----------------------------------------------------------------------------
int castor::tape::utils::SmartFILEPtr::get()
  throw(castor::exception::Exception) {

  // If this SmartFILEPtr does not own a FILE pointer
  if(m_file == NULL) {
    TAPE_THROW_CODE(EPERM,
      ": Smart FILE pointer does not own a basic FILE pointer");
  }

  return m_file;
}


//-----------------------------------------------------------------------------
// release
//-----------------------------------------------------------------------------
int castor::tape::utils::SmartFILEPtr::release()
  throw(castor::exception::Exception) {

  // If this SmartFILEPtr does not own a FILE pointer
  if(m_file == NULL) {
    TAPE_THROW_CODE(EPERM,
      ": Smart FILE pointer does not own a basic FILE pointer");
  }


  const int tmpFile = m_file;

  // A NULL value indicates this SmartFILEPtr does not own a FILE pointer
  m_file = NULL;

  return tmpFile;
}
