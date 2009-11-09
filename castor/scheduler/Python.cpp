/******************************************************************************
 *                      Python.cpp
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
 * @(#)$RCSfile: Python.cpp,v $ $Revision: 1.2 $ $Release$ $Date: 2008/03/14 13:49:45 $ $Author: waldron $
 *
 * CPP Wrapper for Python
 *
 * @author Dennis Waldron
 *****************************************************************************/

// Include files
#include "castor/scheduler/Python.hpp"
#include "stdio.h"
#include <string.h>


//-----------------------------------------------------------------------------
// Constructor
//-----------------------------------------------------------------------------
castor::scheduler::Python::Python(std::string module)
  throw(castor::exception::Exception) :
  m_policyFile(module),
  m_init(false),
  m_pyModule(NULL),
  m_pyDict(NULL) {

  // Initialize the embedded python interpreter used for selecting the best
  // filesystem to use in the match phase decided by an external Python
  // module
  Py_Initialize();

  FILE *fp = fopen(m_policyFile.c_str(), "r");
  if (fp == NULL) {
    castor::exception::Exception ex(errno);
    ex.getMessage() << "Failed to open file: " << m_policyFile << std::endl;
    throw ex;
  }

  // Open the python module
  PyRun_SimpleFile(fp, (char *)m_policyFile.c_str());
  if (PyErr_Occurred()) {
    castor::exception::Exception ex(SEINTERNAL);
    ex.getMessage() << "Error in invoking call to PyRun_SimpleFile"
		   << traceback() << std::endl;
    fclose(fp);
    throw ex;
  }
  fclose(fp);

  // Import the pythons modules __main__ namespace
  m_pyModule = PyImport_AddModule((char*)"__main__");
  if (m_pyModule == NULL) {
    castor::exception::Exception ex(SEINTERNAL);
    ex.getMessage() << "Error importing python module with PyImport_AddModule"
		   << traceback() << std::endl;
    throw ex;
  }

  // Get the modules dictionary object
  m_pyDict = PyModule_GetDict(m_pyModule);
  m_init = true;
}


//-----------------------------------------------------------------------------
// Destructor
//-----------------------------------------------------------------------------
castor::scheduler::Python::~Python() {
  Py_Finalize();
}


//-----------------------------------------------------------------------------
// Traceback
//-----------------------------------------------------------------------------
std::string castor::scheduler::Python::traceback() {

  // Check to see if an error occurred
  std::ostringstream error;
  if (!PyErr_Occurred()) {
    error << "Exception: Unknown";
    return error.str();
  }

  // Fetch the error information
  PyObject *pyTracebackMod, *pyString, *pyList = NULL, *pyEmptyString = NULL;
  PyObject *pyType, *pyTraceback, *pyValue;
  PyErr_Fetch(&pyType, &pyValue, &pyTraceback);
  PyErr_NormalizeException(&pyType, &pyValue, &pyTraceback);

  // Append traceback information
  pyTracebackMod = PyImport_ImportModule((char*)"traceback");
  if (pyTracebackMod != NULL) {
    pyList = PyObject_CallMethod(pyTracebackMod,
				 (char*)"format_exception",
				 (char*)"OOO",
				 pyType,
				 pyValue     == NULL ? Py_None : pyValue,
				 pyTraceback == NULL ? Py_None : pyTraceback);
    pyEmptyString = PyString_FromString("");
    pyString = PyObject_CallMethod(pyEmptyString, (char*)"join", (char*)"O", pyList);
    if (pyString) {
      error << PyString_AsString(pyString);
    } else {
      error << "Traceback: Not available";
    }
  } else if (pyValue) {

    // Append type and value
    error << "Exception ";
    if ((pyString = PyObject_Str(pyType))) {
      error << "Type: " << PyString_AsString(pyString) << " - ";
      Py_XDECREF(pyString);
    }
    if ((pyString = PyObject_Str(pyValue))) {
      error << "Value: " << PyString_AsString(pyString);
      Py_XDECREF(pyString);
    }
  }

  // Cleanup
  Py_XDECREF(pyType);
  Py_XDECREF(pyValue);
  Py_XDECREF(pyTraceback);
  Py_XDECREF(pyTracebackMod);
  Py_XDECREF(pyEmptyString);
  Py_XDECREF(pyList);

  PyErr_Clear();

  return error.str();
}


//-----------------------------------------------------------------------------
// version
//-----------------------------------------------------------------------------
std::string castor::scheduler::Python::version() {
  std::string pyVersion(Py_GetVersion());
  return pyVersion.substr(0, pyVersion.find(' ', 0));
}


//-----------------------------------------------------------------------------
// pySet (64bit integers)
//-----------------------------------------------------------------------------
void castor::scheduler::Python::pySet(char *name, u_signed64 value) {
  PyObject *pyValue;
  if (PyObject_HasAttrString(m_pyModule, name)) {
    if ((pyValue = PyLong_FromUnsignedLongLong(value)))
      PyObject_SetAttrString(m_pyModule, name, pyValue);
    Py_XDECREF(pyValue);
  }
}


//-----------------------------------------------------------------------------
// pySet (32bit integers)
//-----------------------------------------------------------------------------
void castor::scheduler::Python::pySet(char *name, int value) {
  PyObject *pyValue;
  if (PyObject_HasAttrString(m_pyModule, name)) {
    if ((pyValue = PyInt_FromLong(value)))
      PyObject_SetAttrString(m_pyModule, name, pyValue);
    Py_XDECREF(pyValue);
  }
}


//-----------------------------------------------------------------------------
// pySet (32bit unsigned integers)
//-----------------------------------------------------------------------------
void castor::scheduler::Python::pySet(char *name, unsigned int value) {
  PyObject *pyValue;
  if (PyObject_HasAttrString(m_pyModule, name)) {
    if ((pyValue = PyInt_FromLong(value)))
      PyObject_SetAttrString(m_pyModule, name, pyValue);
    Py_XDECREF(pyValue);
  }
}


//-----------------------------------------------------------------------------
// pySet (strings)
//-----------------------------------------------------------------------------
void castor::scheduler::Python::pySet(char *name, std::string value) {
  PyObject *pyValue;
  if (PyObject_HasAttrString(m_pyModule, name)) {
    if ((pyValue = PyString_FromString(value.c_str())))
      PyObject_SetAttrString(m_pyModule, name, pyValue);
    Py_XDECREF(pyValue);
  }
}


//-----------------------------------------------------------------------------
// pyExecute
//-----------------------------------------------------------------------------
float castor::scheduler::Python::pyExecute(std::string function)
  throw(castor::exception::Exception) {

  // Python initialised correctly
  if (m_pyDict == NULL) {
    castor::exception::Exception ex(SEINTERNAL);
    ex.getMessage() << "Embedded Python Interpreter is not initialised"
		    << std::endl;
    throw ex;
  }

  // Attempt to find the function in the Python modules dictionary with the
  // function name. If the function cannot be found revert to the default.
  PyObject *pyFunc = PyDict_GetItemString(m_pyDict, (char *)function.c_str());
  if (pyFunc == NULL) {
    pyFunc = PyDict_GetItemString(m_pyDict, "default");
  }

  // Check to make sure the function exists and is callable
  if (!pyFunc || !PyCallable_Check(pyFunc)) {
    castor::exception::Exception ex(EINVAL);
    if (!strcmp(function.c_str(), "default")) {
      ex.getMessage() << "Unable to find 'default' function in Python module, "
		      << "check module syntax - "
		      << traceback() << std::endl;
      throw ex;
    }
    ex.getMessage() << "Unable to find function '" << function
		    << "' or 'default' in Python module, check module syntax - "
		    << traceback() << std::endl;
    throw ex;
  }

  // Call the function from the Python modules namespace
  PyObject *pyValue = PyObject_CallObject(pyFunc, NULL);
  if (pyValue == NULL) {
    castor::exception::Exception ex(SEINTERNAL);
    ex.getMessage() << "Failed to execute Python function " << function
		    << " - " << traceback() << std::endl;
    throw ex;
  }

  float value = (float)PyFloat_AsDouble(pyValue);
  Py_XDECREF(pyValue);

  return value;
}
