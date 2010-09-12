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
#include "castor/tape/python/ScopedPythonLock.hpp"
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
  throw() {

  // Get a lock on the global embedded Python interpreter using
  // PyGILState_Ensure() so that the Py_Finalize() function which will be
  // called next, can internally get a hold of the current thread's state via
  // PyThreadState_Get().  A ScopedPythonLock object cannot be used because its
  // destructor would call the PyGILState_Release() function which would fail
  // because it would be called after the Py_Finalize() function.
  PyGILState_Ensure();

  // Finalize the embedded Python interpreter
  Py_Finalize();
}


//---------------------------------------------------------------------------
// importPythonModule
//---------------------------------------------------------------------------
PyObject * castor::tape::python::importPythonModule(
  const char *const moduleName)
  throw(castor::exception::Exception) {

  if(moduleName == NULL) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Failed to import python-module"
      ": moduleName parameter is NULL");
  }

  castor::tape::python::SmartPyObjectPtr
    module(PyImport_ImportModule((char *)moduleName));

  if(module.get() == NULL) {
    // Try to determine the Python exception if there was a Python error
    PyObject *const pyEx = PyErr_Occurred();
    const char *pyExStr = python::stdPythonExceptionToStr(pyEx);

    // Clear the Python error if there was one
    if(pyEx != NULL) {
      PyErr_Clear();
    }

    castor::exception::Exception ex(ECANCELED);

    ex.getMessage() <<
      "Failed to import python-module"
      ": PyImport_ImportModule() call failed"
      ": moduleName=" << moduleName <<
      ", pythonException=" << pyExStr;

    throw(ex);
  }

  PyObject *const dict = PyModule_GetDict(module.get());

  if(dict == NULL) {
    castor::exception::Exception ex(ECANCELED);

    ex.getMessage() <<
      "Failed to import python-module"
      ": moduleName=" << moduleName <<
      "PyModule_GetDict() call failed";

    throw(ex);
  }

  return(dict);
}


//---------------------------------------------------------------------------
// importPythonModuleWithLock
//---------------------------------------------------------------------------
PyObject * castor::tape::python::importPythonModuleWithLock(
  const char *const moduleName)
  throw(castor::exception::Exception) {
  // Get a lock on the embedded Python-interpreter
  ScopedPythonLock scopedLock;

  return(importPythonModule(moduleName));
}


//---------------------------------------------------------------------------
// importPolicyPythonModule
//---------------------------------------------------------------------------
PyObject * castor::tape::python::importPolicyPythonModule(
  const char *const moduleName)
  throw(castor::exception::Exception) {

  if(moduleName == NULL) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Failed to import policy python-module"
      ": moduleName parameter is NULL");
  }

  // Check the module file exists in the CASTOR_POLICIES_DIRECTORY as it is
  // difficult to obtain errors from the embedded Python interpreter
  checkPolicyModuleIsInCastorPoliciesDirectory(moduleName);

  return importPythonModule(moduleName);
}


//---------------------------------------------------------------------------
// checkPolicyModuleIsInCastorPoliciesDirectory
//---------------------------------------------------------------------------
void castor::tape::python::checkPolicyModuleIsInCastorPoliciesDirectory(
  const char *const moduleName)
  throw(castor::exception::Exception) {
  std::string fullPathname(CASTOR_POLICIES_DIRECTORY);

  fullPathname += "/";
  fullPathname += moduleName;
  fullPathname += ".py";

  struct stat buf;
  try {
    utils::statFile(fullPathname.c_str(), buf);
  } catch(castor::exception::Exception &ex) {
    castor::exception::Exception ex2(ex.code());

    ex2.getMessage() <<
      "Failed to import policy python-module"
      ": moduleName=" << moduleName <<
      ": Failed to get information about the CASTOR-policy Python-module file"
      ": " << ex.getMessage().str();

    throw(ex2);
  }

  // Throw an exception if the module file is not a regular file
  if(!S_ISREG(buf.st_mode)) {
    castor::exception::Exception ex(ECANCELED);

    ex.getMessage() <<
      "Failed to import policy python-module"
      ": moduleName=" << moduleName <<
      ": " << fullPathname << " is not a regular file";

    throw(ex);
  }
}


//---------------------------------------------------------------------------
// importPolicyPythonModuleWithLock
//---------------------------------------------------------------------------
PyObject * castor::tape::python::importPolicyPythonModuleWithLock(
  const char *const moduleName)
  throw(castor::exception::Exception) {
  if(moduleName == NULL) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Failed to import policy python-module"
      ": moduleName parameter is NULL");
  }

  // Check the module file exists in the CASTOR_POLICIES_DIRECTORY as it is
  // difficult to obtain errors from the embedded Python interpreter
  checkPolicyModuleIsInCastorPoliciesDirectory(moduleName);

  return importPythonModuleWithLock(moduleName);
}


//---------------------------------------------------------------------------
// getPythonFunction
//---------------------------------------------------------------------------
PyObject *castor::tape::python::getPythonFunction(
  PyObject   *const pyDict,
  const char *const functionName)
  throw(castor::exception::Exception) {

  if(pyDict == NULL) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": pyDict parameter is NULL");
  }

  if(functionName == NULL) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": functionName parameter is NULL");
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


//---------------------------------------------------------------------------
// getPythonFunctionWithLock
//---------------------------------------------------------------------------
PyObject *castor::tape::python::getPythonFunctionWithLock(
  PyObject   *const pyDict,
  const char *const functionName)
  throw(castor::exception::Exception) {
  // Get a lock on the embedded Python-interpreter
  ScopedPythonLock scopedLock;

  return(getPythonFunction(pyDict, functionName));
}


//---------------------------------------------------------------------------
// pythonExceptionToStr
//---------------------------------------------------------------------------
const char *castor::tape::python::stdPythonExceptionToStr(
  PyObject *const pyEx)
  throw() {

  if(pyEx == NULL) {
    return "NULL";
  } else if(PyErr_GivenExceptionMatches(pyEx, PyExc_AssertionError)) {
    return "AssertionError";
  } else if(PyErr_GivenExceptionMatches(pyEx, PyExc_AttributeError)) {
    return "AttributeError";
  } else if(PyErr_GivenExceptionMatches(pyEx, PyExc_EOFError)) {
    return "EOFError";
  } else if(PyErr_GivenExceptionMatches(pyEx, PyExc_FloatingPointError)) {
    return "FloatingPointError";
  } else if(PyErr_GivenExceptionMatches(pyEx, PyExc_IOError)) {
    return "IOError";
  } else if(PyErr_GivenExceptionMatches(pyEx, PyExc_ImportError)) {
    return "ImportError";
  } else if(PyErr_GivenExceptionMatches(pyEx, PyExc_IndexError)) {
    return "IndexError";
  } else if(PyErr_GivenExceptionMatches(pyEx, PyExc_KeyError)) {
    return  "KeyError";
  } else if(PyErr_GivenExceptionMatches(pyEx, PyExc_KeyboardInterrupt)) {
    return "KeyboardInterrupt";
  } else if(PyErr_GivenExceptionMatches(pyEx, PyExc_MemoryError)) {
    return "MemoryError";
  } else if(PyErr_GivenExceptionMatches(pyEx, PyExc_NameError)) {
    return "NameError";
  } else if(PyErr_GivenExceptionMatches(pyEx, PyExc_NotImplementedError)) {
    return "NotImplementedError";
  } else if(PyErr_GivenExceptionMatches(pyEx, PyExc_OSError)) {
    return "OSError";
  } else if(PyErr_GivenExceptionMatches(pyEx, PyExc_OverflowError)) {
    return "OverflowError";
  } else if(PyErr_GivenExceptionMatches(pyEx, PyExc_ReferenceError)) {
    return "ReferenceError";
  } else if(PyErr_GivenExceptionMatches(pyEx, PyExc_RuntimeError)) {
    return "RuntimeError";
  } else if(PyErr_GivenExceptionMatches(pyEx, PyExc_SyntaxError)) {
    return "SyntaxError";
  } else if(PyErr_GivenExceptionMatches(pyEx, PyExc_SystemError)) {
    return "SystemError";
  } else if(PyErr_GivenExceptionMatches(pyEx, PyExc_SystemExit)) {
    return "SystemExit";
  } else if(PyErr_GivenExceptionMatches(pyEx, PyExc_TypeError)) {
    return "TypeError";
  } else if(PyErr_GivenExceptionMatches(pyEx, PyExc_ValueError)) {
    return "ValueError";
  } else if(PyErr_GivenExceptionMatches(pyEx, PyExc_ZeroDivisionError)) {
    return "ZeroDivisionError";
  } else if(PyErr_GivenExceptionMatches(pyEx, PyExc_StandardError)) {
    return "StandardError";
  } else if(PyErr_GivenExceptionMatches(pyEx, PyExc_ArithmeticError)) {
    return "ArithmeticError";
  } else if(PyErr_GivenExceptionMatches(pyEx, PyExc_LookupError)) {
    return "LookupError";
  } else if(PyErr_GivenExceptionMatches(pyEx, PyExc_EnvironmentError)) {
    return "EnvironmentError";
  } else if(PyErr_GivenExceptionMatches(pyEx, PyExc_Exception)) {
    return "Exception";
  } else {
    return "UNKNOWN";
  }
}


//---------------------------------------------------------------------------
// getPythonFunctionArgumentNames
//---------------------------------------------------------------------------
void castor::tape::python::getPythonFunctionArgumentNames(
  PyObject          *const inspectGetargspecFunc,
  PyObject          *const pyFunc,
  std::vector<std::string> &argumentNames) throw(castor::exception::Exception) {

  // Build the input Python-object for the inspect.getargspec Python-function
  castor::tape::python::SmartPyObjectPtr inputObj(
    Py_BuildValue((char *)"(O)", pyFunc));

  // Throw an exception if the creation of the input Python-object failed
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
  SmartPyObjectPtr resultObj(PyObject_CallObject(inspectGetargspecFunc,
    inputObj.get()));

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
  SmartPyObjectPtr
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

  // Throw an exception if the function argument names are not a
  // Python-sequence
  if(!PySequence_Check(argumentNamesPySequence.get())) {
    castor::exception::Exception ex(ECANCELED);
    ex.getMessage() <<
      __FUNCTION__ << "() failed"
      ": Function argument names are not of the expected Python-type"
      ": expectedResultType=PySequence";
    throw(ex);
  }

  const int nbArgumentNames = PySequence_Size(argumentNamesPySequence.get());

  // Throw an exception if the number of function argument names could not be
  // determined
  if(nbArgumentNames == -1) {
    castor::exception::Exception ex(ECANCELED);
    ex.getMessage() <<
      __FUNCTION__ << "() failed"
      ": Failed to determine the number of function argument names"
      ": Call to PySequence_Size() failed";
    throw(ex);
  }

  // Push each function argument name on onto the back of the output list of
  // function argument names
  for(int i=0; i<nbArgumentNames; i++) {
    SmartPyObjectPtr
      argumentNamePyObj(PySequence_GetItem(argumentNamesPySequence.get(),i));

    // Throw an exception if the function argument name could not be retreived
    if(argumentNamePyObj.get() == NULL) {
      castor::exception::Exception ex(ECANCELED);
      ex.getMessage() <<
        __FUNCTION__ << "() failed"
        ": Failed to retreive function argument name"
        ": Call to PySequence_GetItem() failed";
      throw(ex);
    }

    // Throw an exception if the function argument name is not a Python-string
    if(!PyString_Check(argumentNamePyObj.get())) {
      castor::exception::Exception ex(ECANCELED);
      ex.getMessage() <<
        __FUNCTION__ << "() failed"
        ": Function argument name is not a Python-string";
      throw(ex);
    }

    char *const argumentName = PyString_AsString(argumentNamePyObj.get());

    // Throw an exception if the function argument name could not be converted
    // to a C string
    if(argumentName == NULL) {
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
        ": Failed to convert function argument name to a C string"
        ": Call to PyString_AsString() failed"
        ": pythonException=" << pyExStr;
      throw(ex);
    }

    argumentNames.push_back(argumentName);
  }
}


//---------------------------------------------------------------------------
// getPythonFunctionArgumentNamesWithLock
//---------------------------------------------------------------------------
void castor::tape::python::getPythonFunctionArgumentNamesWithLock(
  PyObject          *const inspectGetargspecFunc,
  PyObject          *const pyFunc,
  std::vector<std::string> &argumentNames)
  throw(castor::exception::Exception) {
  // Get a lock on the embedded Python-interpreter
  ScopedPythonLock scopedPythonLock;

  getPythonFunctionArgumentNames(inspectGetargspecFunc, pyFunc, argumentNames);
}
