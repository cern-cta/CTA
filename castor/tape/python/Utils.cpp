/******************************************************************************
 *                      castor/tape/python/utils.cpp
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
 * @author Giulia Taurelli
 *****************************************************************************/

#include "castor/tape/python/ScopedLock.hpp"
#include "castor/tape/python/SmartPyObjectPtr.hpp"
#include "castor/tape/python/utils.hpp"
#include "h/serrno.h"

#include <errno.h>
#include <unistd.h>



//------------------------------------------------------------------------------
// appendDirectoryToPYTHONPATH
//------------------------------------------------------------------------------
void castor::tape::python::utils::appendDirectoryToPYTHONPATH(const char *const directory)
  throw() {
  // Get the current value of PYTHONPATH (there may not be one)
  const char *const currentPath = getenv("PYTHONPATH");

  // Construct the new value of PYTHONPATH
  std::string newPath;
  if(currentPath == NULL) {
    newPath = directory;
  } else {
    newPath  = currentPath;
    newPath += ":";
    newPath += directory;
  }

  // Set PYTHONPATH to the new value
  const int overwrite = 1;
  setenv("PYTHONPATH", newPath.c_str(), overwrite);
}


//------------------------------------------------------------------------------
// importPythonModule
//------------------------------------------------------------------------------
PyObject *castor::tape::python::utils::importPythonModule(const char *const moduleName)
  throw(castor::exception::Exception) {

  PyObject *const module = PyImport_ImportModule((char *)moduleName);

  if(module == NULL) {
    castor::exception::Exception ex(ECANCELED);

    ex.getMessage() <<
      "PyImport_ImportModule() call failed"
      ": moduleName=" << moduleName;

    throw(ex);
  }

  return module;
}


//------------------------------------------------------------------------------
// checkPolicyModuleFileExists
//------------------------------------------------------------------------------
void castor::tape::python::utils::checkPolicyModuleFileExists(const char *moduleName)
  throw(castor::exception::Exception) {

  std::string fullPathname(CASTOR_POLICIES_DIRECTORY);

  fullPathname += "/";
  fullPathname += moduleName;
  fullPathname += ".py";

  struct stat buf;
  const int rc = stat(fullPathname.c_str(), &buf);
  const int savedErrno = errno;

  // Throw an exception if the stat() call failed
  if(rc != 0) {
    castor::exception::Exception ex(savedErrno);

    ex.getMessage() <<
      "Failed to stat() policy module-file"
      ": filename=" << fullPathname <<
      ": " << sstrerror(savedErrno);

    throw(ex);
  }

  // Throw an exception if the file is not a regular file
  if(!S_ISREG(buf.st_mode)) {
    castor::exception::Exception ex(savedErrno);

    ex.getMessage() <<
      fullPathname << " is not a regular file";

    throw(ex);
  }
}



//---------------------------------------------------------------------------
// Inizialize Python
//---------------------------------------------------------------------------

void castor::tape::python::utils::startThreadSupportInit(){

  
  // Append the CASTOR policies directory to the end of the PYTHONPATH
  // environment variable so the PyImport_ImportModule() function can find the
  // stream and migration policy modules
  appendDirectoryToPYTHONPATH(CASTOR_POLICIES_DIRECTORY);

  // Initialize Python
  Py_Initialize();


  // Initialize thread support
  //
  // Please note that PyEval_InitThreads() takes a lock on the global Python
  // interpreter
  PyEval_InitThreads();

  // Release the lock on the Python global interpreter that was taken by the
  // preceding PyEval_InitThreads() call
  PyEval_ReleaseLock();


}

//---------------------------------------------------------------------------
// loadPythonCastorPolicy
//---------------------------------------------------------------------------

PyObject * castor::tape::python::utils::loadPythonCastorPolicy(const char *const policyModuleName) throw(castor::exception::Exception){

  // Load in the policy Python module and get a handle on its associated Python dictionary object
  castor::tape::python::utils::SmartPyObjectPtr policyModule;
  PyObject *policyDict = NULL;
  if(policyModuleName != NULL) {
    checkPolicyModuleFileExists(policyModuleName);
    policyModule.reset(importPythonModule(policyModuleName));
    policyDict = PyModule_GetDict(policyModule.get());
  }
  return policyDict;

}

//---------------------------------------------------------------------------
// getPythonFunction
//---------------------------------------------------------------------------


PyObject * castor::tape::python::utils::getPythonFunction(PyObject* pyDict,const char *const functionName)
  throw(castor::exception::Exception){

  if (pyDict==NULL || functionName == NULL)
    return NULL;

  // Get a pointer to the migration-policy Python-function

  PyObject *pyFunc = PyDict_GetItemString(pyDict,functionName);

  // Throw an exception if the migration-policy Python-function does not exist
  if(pyFunc == NULL) {
    castor::exception::InvalidArgument ex;

    ex.getMessage() <<
      "Python function does not exist"
      ": functionName=" << functionName;

    throw ex;
  }

  // Throw an exception if the migration-policy Python-function is not callable
  if (!PyCallable_Check(pyFunc)) {
    castor::exception::InvalidArgument ex;

    ex.getMessage() <<
      "Python function cannot be called"
      ": functionName=" << functionName;

    throw ex;
  }
  
  return pyFunc;
}

