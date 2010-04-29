/******************************************************************************
 *                 test/castor/tape/python/TestPythonMain.cpp
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
#include <pthread.h>


void *startRoutine(void *arg) {
  using namespace castor::tape;

  std::cout << "Entered startRoutine()" << std::endl;

  PyObject *const pyFunc = (PyObject *)arg;

  {
    python::ScopedPythonLock scopedPythonLock;

    python::SmartPyObjectPtr inputObj(Py_BuildValue(
      "(s,s,K,K,K,K,K,K,K,K,K,K)",
      "stevePool", // (elem->tapePoolName()).c_str(),
      "steveFile", // (elem->castorFileName()).c_str(),
      1, // elem->copyNb(),
      1, // elem->fileId(),
      1, // elem->fileSize(),
      1, // elem->fileMode(),
      1, // elem->uid(),
      1, // elem->gid(),
      1, // elem->aTime(),
      1, // elem->mTime(),
      1, // elem->cTime(),
      1  // elem->fileClass()
    ));

    python::SmartPyObjectPtr
      result(PyObject_CallObject(pyFunc, inputObj.get()));

    if(result.get() != NULL) {
      std::cout <<
        "Succeeded to execute Python function" <<
        std::endl;
    } else {
      std::cerr <<
        "Failed to execute Python function" <<
        std::endl;
    }
  }

  std::cout << "Leaving startRoutine()" << std::endl;
  return NULL;
}


int exceptionThrowingMain()
  throw(castor::exception::Exception) {

  using namespace castor::tape;

  python::initializePython();

  // Import a valid module and get a valid function
  PyObject * pyFunc = NULL;
  {
    python::ScopedPythonLock scopedPythonLock;

    const char *const moduleName   = "migration";
    const char *const functionName = "atlasT0MigrPolicy";
    PyObject *const pyDict = python::importPolicyPythonModule(moduleName);
    pyFunc = python::getPythonFunction(pyDict, functionName);

    if(pyFunc != NULL) {
      std::cout << "\"" << functionName << "\" found" << std::endl;
    } else {
      std::cout << "\"" << functionName << "\" NOT found" << std::endl;

      return 1;
    }
  }

  // Import a non-existant module
  try {
    python::ScopedPythonLock scopedPythonLock;

    python::importPolicyPythonModule("doesnotexist");

    std::cerr <<
      "Test failed because the importing of non-existant module apparently"
      " worked" <<
      std::endl;

    return 1;
  } catch(castor::exception::Exception &ex) {

    if(ex.code() == ENOENT) {
      std::cout <<
        "Succeeded to detect non-existant module file" <<
        std::endl;
    } else {
      std::cerr <<
        "Test failed because importPolicyPythonModule() threw the wrong"
        " exception" <<
        std::endl;

      return 1;
    }
  }

  {
    python::ScopedPythonLock scopedPythonLock;

    std::cout <<
      "Inside a new scopedPythonLock" <<
      std::endl;
  }

  {
    python::ScopedPythonLock scopedPythonLock;

    std::cout <<
      "Inside yet another new scopedPythonLock" <<
      std::endl;
  }

  pthread_t thread;

  utils::pthreadCreate(&thread , NULL, startRoutine , pyFunc);

  utils::pthreadJoin(thread, NULL);

  std::cout <<
    "main thread is sleeping for 1 second" <<
    std::endl;
  sleep(1);

  {
    python::ScopedPythonLock scopedPythonLock;

    std::cout <<
      "Inside new scopedPythonLock after utils::pthreadJoin() and sleep(1)" <<
      std::endl;
  }

  python::finalizePython();

  std::cout <<
    "After python::finalizePython()" <<
    std::endl;

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
