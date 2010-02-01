/******************************************************************************
 *              castor/tape/python/SmartPyObject.cpp
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
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/

// Include Python.h before any standard headers because Python.h may define
// some pre-processor definitions which affect the standard headers
#include <Python.h>

#include "castor/tape/python/SmartPyObjectPtr.hpp"


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::python::SmartPyObjectPtr::SmartPyObjectPtr() throw() :
  m_pyObject(NULL) {
}


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::python::SmartPyObjectPtr::SmartPyObjectPtr(
  PyObject *const pyObject) throw() : m_pyObject(pyObject) {
}


//-----------------------------------------------------------------------------
// reset
//-----------------------------------------------------------------------------
void castor::tape::python::SmartPyObjectPtr::reset(
  PyObject *const pyObject = NULL) throw() {

  // If the new pointer is not the one already owned
  if(pyObject != m_pyObject) {

    // If this smart pointer still owns a pointer, then call Py_XDECREF() on it
    if(m_pyObject != NULL) {
      Py_XDECREF(m_pyObject);
    }

    // Take ownership of the new pointer
    m_pyObject = pyObject;
  }
}


//-----------------------------------------------------------------------------
// SmartPyObjectPtr assignment operator
//-----------------------------------------------------------------------------
castor::tape::python::SmartPyObjectPtr
  &castor::tape::python::SmartPyObjectPtr::operator=(SmartPyObjectPtr& obj)
  throw() {

  reset(obj.release());

  return *this;
}


//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::tape::python::SmartPyObjectPtr::~SmartPyObjectPtr() {

  reset();
}


//-----------------------------------------------------------------------------
// get
//-----------------------------------------------------------------------------
PyObject *castor::tape::python::SmartPyObjectPtr::get() const throw() {

  return m_pyObject;
}


//-----------------------------------------------------------------------------
// release
//-----------------------------------------------------------------------------
PyObject *castor::tape::python::SmartPyObjectPtr::release()
  throw(castor::exception::Exception) {

  // If this smart pointer does not own a pointer
  if(m_pyObject == NULL) {
    castor::exception::Exception ex(EPERM);

    ex.getMessage() <<
      "Smart pointer does not own a PyObject pointer";

    throw(ex);
  }


  PyObject *const tmp = m_pyObject;

  // A NULL value indicates this smart pointer does not own a pointer
  m_pyObject = NULL;

  return tmp;
}
