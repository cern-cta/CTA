/******************************************************************************
 *              castor/tape/python/ScopedPythonLock.hpp
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

#ifndef CASTOR_TAPE_PYTHON_SCOPEPYTHONLOCK_HPP
#define CASTOR_TAPE_PYTHON_SCOPEPYTHONLOCK_HPP 1

// Include Python.h before any standard headers because Python.h may define
// some pre-processor definitions which affect the standard headers
#include <Python.h>


namespace castor    {
namespace tape    {
namespace python {

/**
 * As scoped lock which calls PyCGILState_Ensure() and PyGILState_Release()
 * in order to ensure the current thread is ready to call the Python C API
 * regardless of the current state of the embedded Python interpreter, or the
 * state of the global interpreter lock.
 */
class ScopedPythonLock {

public:

  /**
   * Constructor that immediately calls PyCGILState_Ensure() to ensure the
   * current thread is ready to call the Python C API regardless of the current
   * state of the embedded Python interpreter, or the state of the global
   * interpreter lock.
   */
  ScopedPythonLock() throw();

  /**
   * Destructor that calls PyCGILState_Release() to release the embedded Python
   * interpreter from the current thread and to return the interpreter to the
   * state it was in prior to the execution of this scoped lock's constructor.
   */
  ~ScopedPythonLock() throw();


private:

  /**
   * Opaque handle to the bookkeeping information kept by Python about the
   * thread taking a lock on the embedded Python interpreter.
   */
  const PyGILState_STATE m_gstate;

}; // class ScopedPythonLock

} // namespace python
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_PYTHON_SCOPEPYTHONLOCK_HPP
