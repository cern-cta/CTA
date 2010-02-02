/******************************************************************************
 *                      castor/tape/python/python.cpp
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
 * @author Giulia Taurelli, Nicola Bessone and Steven Murray
 *****************************************************************************/

// Include python.hpp before any standard headers because python.hpp includes
// Python.h which may define some pre-processor definitions which affect the
// standard headers
#include "castor/tape/python/python.hpp"

#include "castor/tape/python/Constants.hpp"
#include "castor/tape/python/SmartPyObjectPtr.hpp"
#include "castor/tape/utils/utils.hpp"
#include "h/serrno.h"

#include <errno.h>
#include <pthread.h>
#include <unistd.h>


//---------------------------------------------------------------------------
// initializePython
//---------------------------------------------------------------------------
void castor::tape::python::initializePython()
  throw(castor::exception::Exception) {

  // Append the CASTOR policies directory to the end of the PYTHONPATH
  // environment variable so the PyImport_ImportModule() function can find the
  // stream and migration policy modules
  {
    // Get the current value of PYTHONPATH (there may not be one)
    const char *const currentPath = getenv("PYTHONPATH");

    // Construct the new value of PYTHONPATH
    std::string newPath;
    if(currentPath == NULL) {
      newPath = CASTOR_POLICIES_DIRECTORY;
    } else {
      newPath  = currentPath;
      newPath += ":";
      newPath += CASTOR_POLICIES_DIRECTORY;
    }

    // Set PYTHONPATH to the new value
    const int overwrite = 1;
    setenv("PYTHONPATH", newPath.c_str(), overwrite);
  }

  // Initialize Python throwing an exception if a Python error occurs
  Py_Initialize();
  if(PyErr_Occurred()) {
    castor::exception::Exception ex(ECANCELED);

    ex.getMessage() <<
      "Py_Initialize() call failed"
      ": A Python error occured";

    throw ex;
  }

  // Initialize thread support
  //
  // Please note that PyEval_InitThreads() takes a lock on the global Python
  // interpreter
  PyEval_InitThreads();
  if(PyErr_Occurred()) {
    castor::exception::Exception ex(ECANCELED);

    ex.getMessage() <<
      "PyEval_InitThreads() call failed"
      ": A Python error occured";

    throw ex;
  }

  // Release the lock on the Python global interpreter that was taken by the
  // preceding PyEval_InitThreads() call
  PyEval_ReleaseLock();
  if(PyErr_Occurred()) {
    castor::exception::Exception ex(ECANCELED);

    ex.getMessage() <<
      "PyEval_ReleaseLock() call failed"
      ": A Python error occured";

    throw ex;
  }
}


//---------------------------------------------------------------------------
// finalizePython
//---------------------------------------------------------------------------
void castor::tape::python::finalizePython()
  throw(castor::exception::Exception) {

  Py_Finalize();
}


//---------------------------------------------------------------------------
// importPolicyPythonModule
//---------------------------------------------------------------------------
PyObject * castor::tape::python::importPolicyPythonModule(
  const char *const moduleName) throw(castor::exception::Exception) {

  if(moduleName == NULL) {
    castor::exception::InvalidArgument ex;

    ex.getMessage() <<
      "moduleName parameter is NULL";

    throw(ex);
  }

  // Check the module file exists in the CASTOR_POLICIES_DIRECTORY as it is
  // difficult to obtain errors from the embedded Python interpreter
  {
    std::string fullPathname(CASTOR_POLICIES_DIRECTORY);

    fullPathname += "/";
    fullPathname += moduleName;
    fullPathname += ".py";

    struct stat buf;
    try {
      utils::statFile(fullPathname.c_str(), buf);
    } catch(castor::exception::Exception &ex) {
      castor::exception::Exception ex1(ex.code());

      ex.getMessage() <<
        "Failed to get information about the CASTOR-policy Python-module"
        ": " << ex.getMessage().str();
    }

    // Throw an exception if the module file is not a regular file
    if(!S_ISREG(buf.st_mode)) {
      castor::exception::Exception ex(ECANCELED);

      ex.getMessage() <<
        fullPathname << " is not a regular file";

      throw(ex);
    }
  }

  castor::tape::python::SmartPyObjectPtr
    module(PyImport_ImportModule((char *)moduleName));

  if(module.get() == NULL) {
    castor::exception::Exception ex(ECANCELED);

    ex.getMessage() <<
      "PyImport_ImportModule() call failed"
      ": moduleName=" << moduleName;

    throw(ex);
  }

  return(PyModule_GetDict(module.get()));
}


//---------------------------------------------------------------------------
// getPythonFunction
//---------------------------------------------------------------------------
PyObject * castor::tape::python::getPythonFunction(PyObject *const pyDict,
  const char *const functionName)
  throw(castor::exception::Exception){

  if(pyDict == NULL) {
    castor::exception::InvalidArgument ex;

    ex.getMessage() <<
      "pyDict parameter is NULL";

    throw(ex);
  }

  if(functionName == NULL) {
    castor::exception::InvalidArgument ex;

    ex.getMessage() <<
      "functionName parameter is NULL";

    throw(ex);
  }

  // Get a pointer to the Python-function object
  PyObject *pyFunc = PyDict_GetItemString(pyDict, functionName);

  // Throw an exception if the Python-function object was not found due to a
  // Python error occurring as opposed to function simply not being in the
  // dictionary
  if(pyFunc == NULL && PyErr_Occurred()) {
    castor::exception::Exception ex(ECANCELED);

    ex.getMessage() <<
      "PyDict_GetItemString() call failed"
      ": functionName=" << functionName <<
      ": A Python error occured";

    throw ex;
  }

  // Throw an exception if a non-callable object was found with the same name
  // as the function
  if(pyFunc != NULL && !PyCallable_Check(pyFunc)) {
    castor::exception::InvalidArgument ex;

    ex.getMessage() <<
      "Found non-callable Python object"
      ": functionName=" << functionName;

    throw ex;
  }
  
  return pyFunc;
}
