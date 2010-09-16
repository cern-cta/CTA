/******************************************************************************
 *                 test/castor/tape/python/TestPythonMemoryMain.cpp
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

// Include python.hpp before any standard headers because python.hpp includes
// Python.h which may define some pre-processor definitions which affect the
// standard headers
#include "castor/tape/python/python.hpp"

#include "castor/tape/python/ScopedPythonLock.hpp"
#include "castor/tape/python/SmartPyObjectPtr.hpp"
#include "castor/tape/utils/utils.hpp"

#include <iostream>
#include <string>
#include <vector>


int exceptionThrowingMain()
  throw(castor::exception::Exception) {

  using namespace castor::tape;

  python::initializePython();

  PyObject *const migrationDict =
    python::importPythonModuleWithLock("migration");
  PyObject *const migrationFunc =
    python::getPythonFunctionWithLock(migrationDict, "defaultMigrationPolicy");

  PyObject *const inspectDict = python::importPythonModuleWithLock("inspect");
  PyObject *const inspectGetargspecFunc = python::getPythonFunctionWithLock(
    inspectDict, "getargspec");

  std::vector<std::string> actualArgumentNames;

  for(;;) { // Forever
    python::getPythonFunctionArgumentNamesWithLock(inspectGetargspecFunc,
      migrationFunc, actualArgumentNames);
/*
    castor::tape::python::SmartPyObjectPtr inputObj(
      Py_BuildValue((char *)"(O)", inspectGetargspecFunc));

     if(inputObj.get() == NULL) {
      // Try to determine the Python exception if there was a Python error
      PyObject *const pyEx = PyErr_Occurred();
      const char *pyExStr = python::stdPythonExceptionToStr(pyEx);

      // Clear the Python error if there was one
      if(pyEx != NULL) {
        PyErr_Clear();
      }

      castor::exception::Exception ex(ECANCELED);
      ex.getMessage() <<
        __FUNCTION__ << "() failed"
        ": Failed to create input Python-object for inspect.getargspec"
        ": Call to Py_BuildValue failed"
        ": pythonException=" << pyExStr;
      throw(ex);
    }

    // Call the inspect.getmembers method on the stream-policy Python-module
    python::SmartPyObjectPtr
      resultObj(PyObject_CallObject(inspectGetargspecFunc, inputObj.get()));

    // Throw an exception if the invocation of the Python-function failed
    if(resultObj.get() == NULL) {
      // Try to determine the Python exception if there was a Python error
      PyObject *const pyEx = PyErr_Occurred();
      const char *pyExStr = python::stdPythonExceptionToStr(pyEx);

      // Clear the Python error if there was one
      if(pyEx != NULL) {
        PyErr_Clear();
      }

      castor::exception::Exception ex(ECANCELED);
      ex.getMessage() <<
        __FUNCTION__ << "() failed"
        ": Failed to get result from Python-function"
        ": moduleName=inspect"
        ", functionName=getargspec"
        ", pythonException=" << pyExStr;
      throw(ex);
    }

    // Throw an exception if the result of inspect.getargsspec is not a
    // Python-sequence
    if(!PySequence_Check(resultObj.get())) {
      castor::exception::Exception ex(ECANCELED);
      ex.getMessage() <<
        __FUNCTION__ << "() failed"
        ": Python-function returned unexpected result-type"
        ": moduleName=inspect"
        ", functionName=getargspec"
        ", expectedResultType=PySequence";
      throw(ex);
    }

    // Get a handle on the array of function argument names
    python::SmartPyObjectPtr
      argumentNamesPySequence(PySequence_GetItem(resultObj.get(), 0));

    // Throw an exception if the handle cound not be obtained
    if(argumentNamesPySequence.get() == NULL) {
      castor::exception::Exception ex(ECANCELED);
      ex.getMessage() <<
        __FUNCTION__ << "() failed"
        ": Failed to get a handle on the array of function argument names"
        ": moduleName=inspect"
        ", functionName=getargspec";
      throw(ex);
    }
*/
  } // Forever

  return 0;
}


int main() {
  std::cout <<
    "\n"
    "Testing castor::tape::python\n"
    "============================\n" <<
    std::endl;

  int rc = 0;

  try {
    rc = exceptionThrowingMain();
  } catch(castor::exception::Exception &ex) {
    std::cerr <<
      "Test failed"
      ": Caught an exception"
      ": code=" << ex.code() <<
      ": message=" << ex.getMessage().str() << std::endl;

    return 1;
  }

  if(rc == 0) {
    std::cout << "Test finished successfully\n" << std::endl;
  } else {
    std::cout << "Test failed" << std::endl;
  }

  return rc;
}
