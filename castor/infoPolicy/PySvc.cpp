/******************************************************************************
 *                      PySvc.cpp
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
 * @(#)$RCSfile: PySvc.cpp,v $ $Revision: 1.3 $ $Release$ $Date: 2007/11/27 16:12:05 $ $Author: gtaur $
 *
 * CPP Wrapper for Python 
 *
 * @author Giulia Taurelli
 *****************************************************************************/

// Include files

#include "castor/infoPolicy/PySvc.hpp"



//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::infoPolicy::PySvc::PySvc(std::string module) 
  throw(castor::exception::Exception) :
  m_moduleFile(module), 
  m_pyModule(NULL),
  m_pyDict(NULL){

  // Initialize the embedded python interpreter 
  
  //if (!Py_IsInitialized()) 
    Py_Initialize();

    //m_pyTid=Py_NewInterpreter();

  FILE *fp = fopen(m_moduleFile.c_str(), "r");
  if (fp == NULL) {
    castor::exception::Exception ex(errno);
    ex.getMessage() << " Failed to open file: " << m_moduleFile.c_str() << std::endl;
    throw ex;
  }

  // Open the python module
  PyRun_SimpleFile(fp, (char *)m_moduleFile.c_str());
  if (PyErr_Occurred()) {
    castor::exception::Exception ex(SEINTERNAL);
    ex.getMessage() << "Error in invoking call to PyRun_SimpleFile" 
		    << std::endl;
    fclose(fp);
    throw ex;
  }
  fclose(fp);

  // Import the pythons modules __main__ namespace
  
  m_pyModule = PyImport_AddModule("__main__");
   
  if (m_pyModule == NULL) {
    castor::exception::Exception ex(SEINTERNAL);
    ex.getMessage() << "Error importing python module with PyImport_AddModule"
		    << std::endl;
    throw ex;
  }

  // Get the modules dictionary object
  m_pyDict = PyModule_GetDict(m_pyModule);
}


//------------------------------------------------------------------------------// callPolicyFunction
//------------------------------------------------------------------------------
bool castor::infoPolicy::PySvc::callPolicyFunction(std::string functionName, PyObject* inputObj){

  // Attempt to find the function in the Python modules dictionary with the
  // function name. If the function cannot be found revert to the default.

  PyObject *pyFunc = PyDict_GetItemString(m_pyDict,functionName.c_str());
  //  if (pyFunc == NULL) {
  //  pyFunc = PyDict_GetItemString(m_pyDict, "default");
  //  }

  // Check to make sure the function exists and is callable
  if (!pyFunc || !PyCallable_Check(pyFunc)) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage() << "Unable to find function '" << functionName.c_str()
		    << "' or 'default' in Python module, check module syntax - "<< std::endl;
    throw ex;
  } 

  // Call the function from the Python modules namespace
  PyObject *pyValue = PyObject_CallObject(pyFunc,inputObj);
  if (pyValue == NULL) {
    castor::exception::Exception ex(SEINTERNAL);
    ex.getMessage() << "Failed to execute Python function " << functionName.c_str() << std::endl;
    throw ex;
  } 

  bool  ret = PyInt_AsLong(pyValue) !=0 ? true : false;
  Py_XDECREF(pyValue);

  return ret;
}


//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::infoPolicy::PySvc::~PySvc() {
  // Py_EndInterpreter(m_pyTid);
  // if (Py_IsInitialized()) 
    Py_Finalize();
}
