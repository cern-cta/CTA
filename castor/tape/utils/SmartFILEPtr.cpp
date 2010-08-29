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

#include <errno.h>
#include <unistd.h>


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::utils::SmartFILEPtr::SmartFILEPtr() throw() :
  m_file(NULL) {
}


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::utils::SmartFILEPtr::SmartFILEPtr(FILE *const file) throw() :
  m_file(file) {
}


//-----------------------------------------------------------------------------
// copy-constructor
//-----------------------------------------------------------------------------
castor::tape::utils::SmartFILEPtr::SmartFILEPtr(const SmartFILEPtr &) throw() {

  // This code is never executed because the copy-constructor is private and
  // should never be called by another method of this class.

  // Do nothing
}


//-----------------------------------------------------------------------------
// reset
//-----------------------------------------------------------------------------
void castor::tape::utils::SmartFILEPtr::reset(FILE *const file = NULL)
   throw() {
  // If the new pointer is not the one already owned
  if(file != m_file) {

    // If this smart pointer still owns a pointer, then fclose it
    if(m_file != NULL) {
      fclose(m_file);
    }

    // Take ownership of the new pointer
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
FILE *castor::tape::utils::SmartFILEPtr::get() const throw() {

  return m_file;
}


//-----------------------------------------------------------------------------
// release
//-----------------------------------------------------------------------------
FILE *castor::tape::utils::SmartFILEPtr::release()
  throw(castor::exception::Exception) {

  // If this smart pointer does not own a pointer
  if(m_file == NULL) {
    castor::exception::Exception ex(EPERM);

    ex.getMessage() <<
      "Smart pointer does not own a FILE pointer";

    throw(ex);
  }


  FILE *const tmp = m_file;

  // A NULL value indicates this smart pointer does not own a pointer
  m_file = NULL;

  return tmp;
}
