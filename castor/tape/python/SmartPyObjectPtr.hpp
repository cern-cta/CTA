/******************************************************************************
 *              castor/rtcopy/mighunter/SmartPyObject.hpp
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

#ifndef CASTOR_TAPE_PYTHON_SMARTPYOBJECTPTR_HPP
#define CASTOR_TAPE_PYTHON_SMARTPYOBJECTPTR_HPP 1

// Include Python.h before any standard headers because Python.h may define
// some pre-processor definitions which affect the standard headers
#include <Python.h>

#include "castor/exception/Exception.hpp"

namespace castor    {
namespace tape    {
namespace python {

/**
 * A smart PyObject pointer that owns a PyObject pointer.  When the smart
 * pointer goes out of scope, it will call Py_XDECREF() on the PyObject
 * pointer.
 */
class SmartPyObjectPtr {
public:

  /**
   * Constructor.
   *
   */
  SmartPyObjectPtr() throw();

  /**
   * Constructor.
   *
   * @param pyObject The PyObject pointer to be owned by the smart pointer.
   */
  SmartPyObjectPtr(PyObject *const pyObject) throw();

  /**
   * Take ownership of the specified PyObject pointer, calling Py_XDECREF() on
   * the previously owned pointer if there is one and it is not the same as the
   * one specified.
   *
   * @param pyObject The PyObject pointer to be owned, defaults to NULL if not
   *                 specified, where NULL means this SmartPyObjectPtr will not
   *                 own a pointer after the reset() method returns.
   */
  void reset(PyObject *const pyObject) throw();

  /**
   * SmartPyObjectPtr assignment operator.
   *
   * This function does the following:
   * <ul>
   * <li> Calls release on the previous owner (obj);
   * <li> Calls Py_XDECREF() on the PyObject pointer of this object if it
   *      already owns one.
   * <li> Makes this object the owner of the PyObject pointer released from the
   *      previous owner (obj).
   * </ul>
   */
  SmartPyObjectPtr &operator=(SmartPyObjectPtr& obj) throw();

  /**
   * Destructor.
   *
   * Calls Py_XDECREF() on the owned PyObject pointer if there is one.
   */
  ~SmartPyObjectPtr();

  /**
   * Returns the owned pointer or NULL if this smart pointer does not own one.
   *
   * @return The owned FILE pointer.
   */
  PyObject *get() const throw();

  /**
   * Releases the owned pointer.
   *
   * @return The released pointer.
   */
  PyObject *release() throw(castor::exception::Exception);

private:

  /**
   * The owned pointer.  A value of NULL means this smart pointer does not own
   * a pointer.
   */
  PyObject *m_pyObject;

}; // class SmartPyObjectPtr

} // namespace python
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_PYTHON_SMARTPYOBJECTPTR_HPP
