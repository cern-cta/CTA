/******************************************************************************
 *         test_multithreaded_cpp_embedded_python_interpreter.cpp
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

#include <iostream>
#include <pthread.h>
#include <sstream>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>

#define SEBASEOFF       1000            /* Base offset for special err. */
#define SEINTERNAL      SEBASEOFF+15    /* Internal error               */

namespace castor {

  namespace exception {

    /**
     * class Exception
     * A simple exception used for error handling in castor
     */
    class Exception {

    public:

      /**
       * Empty Constructor
       * @param serrno the serrno code of the corresponding C error
       */
      Exception(int se);

      /**
       * Copy Constructor
       */
      Exception(Exception& dbex);

      /**
       * Empty Destructor
       */
      virtual ~Exception();

      /**
       * Get the value of m_message
       * A message explaining why this exception was raised
       * @return the value of m_message
       */
      std::ostringstream& getMessage() {
        return m_message;
      }

      /**
       * gets the serrno code of the corresponding C error
       */
      const int code() const { return m_serrno; }

    private:
      /// A message explaining why this exception was raised
      std::ostringstream m_message;

      /**
       * The serrno code of the corresponding C error
       */
      int m_serrno;

    }; // class Exception

    /**
     * Invalid argument exception
     */
    class Internal : public castor::exception::Exception {

    public:

      /**
       * default constructor
       */
      Internal();

    }; // class Internal

    /**
     * Invalid argument exception
     */
    class InvalidArgument : public castor::exception::Exception {

    public:

      /**
       * default constructor
       */
      InvalidArgument();

    }; // class InvalidArgument

  } // end of exception namespace

} // end of castor namespace

castor::exception::Exception::Exception(int se) : m_serrno(se) {}

castor::exception::Exception::Exception(castor::exception::Exception& ex) {
  m_serrno = ex.code();
  m_message << ex.getMessage().str();
}

castor::exception::Exception::~Exception() {}

castor::exception::Internal::Internal() :
  castor::exception::Exception(SEINTERNAL) {}

castor::exception::InvalidArgument::InvalidArgument() :
  castor::exception::Exception(EINVAL) {}

namespace castor    {
namespace rtcopy    {
namespace mighunter {

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
  ScopedPythonLock() throw() : m_gstate(PyGILState_Ensure()) {
  }

  /**
   * Destructor that calls PyCGILState_Release() to release the embedded Python
   * interpreter from the current thread and to return the interpreter to the
   * state it was in prior to the execution of this scoped lock's constructor.
   */
  ~ScopedPythonLock() throw() {
    PyGILState_Release(m_gstate);
  }

private:

  /**
   * Opaque handle to the bookkeeping information kept by Python about the
   * thread taking a lock on the embedded Python interpreter.
   */
  const PyGILState_STATE m_gstate;

}; // class ScopedPythonLock


  /**
   * A smart FILE pointer that owns a basic FILE pointer.  When the smart FILE
   * pointer goes out of scope, it will close the FILE pointer it owns.
   */
  class SmartFILEPtr {
  public:

    /**
     * Constructor.
     *
     */
    SmartFILEPtr();

    /**
     * Constructor.
     *
     * @param file The FILE pointer to be owned by the smart FILE pointer.
     */
    SmartFILEPtr(FILE *const file);

    /**
     * Take ownership of the specified FILE pointer, closing the previously
     * owned FILE pointer if there is one and it is not the same as the one
     * specified.
     *
     * @param file The FILE pointer to be owned, defaults to NULL if not
     *             specified, where NULL means this SmartFILEPtr does not own
     *             anything.
     */
    void reset(FILE *const file) throw();

    /**
     * SmartFILEPtr assignment operator.
     *
     * This function does the following:
     * <ul>
     * <li> Calls release on the previous owner (obj);
     * <li> Closes the FILE pointer of this object if it already owns one.
     * <li> Makes this object the owner of the FILE pointer released from the
     *      previous owner (obj).
     * </ul>
     */
    SmartFILEPtr &operator=(SmartFILEPtr& obj) throw();

    /**
     * Destructor.
     *
     * Closes the owned FILE pointer if there is one.
     */
    ~SmartFILEPtr();

    /**
     * Returns the owned FILE pointer or NULL if this smartFILEPtr does not own
     * FILE pointer.
     *
     * @return The owned FILE pointer.
     */
    FILE *get() throw();

    /**
     * Releases the owned FILE pointer.
     *
     * @return The released FILE pointer.
     */
    FILE *release() throw(castor::exception::Exception);


  private:

    /**
     * The owned FILE pointer.  A value of NULL means this SmartFILEPtr does
     * not own anything.
     */
    FILE *m_file;

  }; // class SmartFILEPtr


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

} // namespace mighunter
} // namespace rtcopy
} // namespace castor


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::rtcopy::mighunter::SmartFILEPtr::SmartFILEPtr() :
  m_file(NULL) {
}


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::rtcopy::mighunter::SmartFILEPtr::SmartFILEPtr(FILE *const file) :
  m_file(file) {
}


//-----------------------------------------------------------------------------
// reset
//-----------------------------------------------------------------------------
void castor::rtcopy::mighunter::SmartFILEPtr::reset(FILE *const file = NULL)
   throw() {
  // If the new FILE pointer is not the one already owned
  if(file != m_file) {

    // If this SmartFILEPtr still owns a FILE pointer, then close it
    if(m_file != NULL) {
      fclose(m_file);
    }

    // Take ownership of the new FILE pointer
    m_file = file;
  }
}


//-----------------------------------------------------------------------------
// SmartFILEPtr assignment operator
//-----------------------------------------------------------------------------
castor::rtcopy::mighunter::SmartFILEPtr
  &castor::rtcopy::mighunter::SmartFILEPtr::operator=(SmartFILEPtr& obj)
  throw() {

  reset(obj.release());

  return *this;
}


//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::rtcopy::mighunter::SmartFILEPtr::~SmartFILEPtr() {

  reset();
}


//-----------------------------------------------------------------------------
// get
//-----------------------------------------------------------------------------
FILE *castor::rtcopy::mighunter::SmartFILEPtr::get() throw() {

  return m_file;
}


//-----------------------------------------------------------------------------
// release
//-----------------------------------------------------------------------------
FILE *castor::rtcopy::mighunter::SmartFILEPtr::release()
  throw(castor::exception::Exception) {

  // If this SmartFILEPtr does not own a FILE pointer
  if(m_file == NULL) {
    castor::exception::Exception ex(EPERM);

    ex.getMessage() <<
      "Smart FILE pointer does not own a FILE pointer";

    throw(ex);
  }


  FILE *const tmpFile = m_file;

  // A NULL value indicates this SmartFILEPtr does not own a FILE pointer
  m_file = NULL;

  return tmpFile;
}


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::rtcopy::mighunter::SmartPyObjectPtr::SmartPyObjectPtr() throw() :
  m_pyObject(NULL) {
}


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::rtcopy::mighunter::SmartPyObjectPtr::SmartPyObjectPtr(
  PyObject *const pyObject) throw() : m_pyObject(pyObject) {
}


//-----------------------------------------------------------------------------
// reset
//-----------------------------------------------------------------------------
void castor::rtcopy::mighunter::SmartPyObjectPtr::reset(
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
castor::rtcopy::mighunter::SmartPyObjectPtr
  &castor::rtcopy::mighunter::SmartPyObjectPtr::operator=(SmartPyObjectPtr& obj)
  throw() {

  reset(obj.release());

  return *this;
}


//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::rtcopy::mighunter::SmartPyObjectPtr::~SmartPyObjectPtr() {

  reset();
}


//-----------------------------------------------------------------------------
// get
//-----------------------------------------------------------------------------
PyObject *castor::rtcopy::mighunter::SmartPyObjectPtr::get() const throw() {

  return m_pyObject;
}


//-----------------------------------------------------------------------------
// release
//-----------------------------------------------------------------------------
PyObject *castor::rtcopy::mighunter::SmartPyObjectPtr::release()
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

/**
 * Loads the specified Python module file into the embedded Python interpreter
 * and returns a pointer to the asscoaited Python module dictionary.
 */
PyObject *loadPythonScript(const char *filename)
  throw(castor::exception::Exception) {
  using namespace castor::rtcopy::mighunter;

  SmartFILEPtr fp(fopen(filename, "r"));
  if (fp.get() == NULL) {
    castor::exception::Exception ex(errno);
    ex.getMessage() <<
      "Failed to open python module-file"
      ": filename= " << filename;
    throw ex;
  }

  // Open the python module
  PyRun_SimpleFile(fp.get(), filename);
  if(PyErr_Occurred()) {
    castor::exception::Internal ex;

    ex.getMessage() << "Error in invoking call to PyRun_SimpleFile"
                    << std::endl;
    throw ex;
  }
  fclose(fp.release());

  // Import the pythons modules __main__ namespace
  PyObject *pyModule = PyImport_AddModule((char*)"__main__");
  if (pyModule == NULL) {
    castor::exception::Exception ex(SEINTERNAL);
    ex.getMessage() << "Error importing python module with PyImport_AddModule"
                    << std::endl;
    throw ex;
  }

  // Get and return a pointer to the module's dictionary object
  return PyModule_GetDict(pyModule);
}


PyObject *callPythonFunction(PyObject *const pyDict, const char *functionName,
  PyObject *const inputObj) throw(castor::exception::Exception) {

  // Get a pointer to the Python function
  PyObject *pyFunc = PyDict_GetItemString(pyDict, functionName);

  // Throw an exception if the Python function does not exist
  if(pyFunc == NULL) {
    castor::exception::InvalidArgument ex;

    ex.getMessage() <<
      "Python function does not exist"
      ": functionName=" << functionName;

    throw ex;
  }

  // Throw an exception if the Python function is not callable
  if (!PyCallable_Check(pyFunc)) {
    castor::exception::InvalidArgument ex;

    ex.getMessage() <<
      "Python function cannot be called"
      ": functionName=" << functionName;

    throw ex;
  }

  PyObject *result = PyObject_CallObject(pyFunc,inputObj);
  if (result == NULL) {
    castor::exception::Internal ex;

    ex.getMessage() <<
      "Failed to execute Python function"
      ": functionName=" << functionName;

    throw ex;
  }

  return result;
}


void *strThreadRoutine(void *arg) {
  using namespace castor::rtcopy::mighunter;

  std::cout << "strThreadRoutine started" << std::endl;

  PyObject        *pyDict       = (PyObject *)arg;
  // const char      *functionName = "steveStreamPolicy";
  const char      *functionName = "firststreamPolicy";

  // SmartPyObjectPtr inputObj;
  SmartPyObjectPtr inputObj(Py_BuildValue(
    "(K,K,K,K,K)",
    1, // elem->runningStream(),
    1, // elem->numFiles(),
    1, // elem->numBytes(),
    1, // elem->maxNumStreams(),
    1  //elem->age());
  ));

  int i=1;
  try {
    for(i=1; i<=10000; i++) {
      ScopedPythonLock scopedPythonLock;

      SmartPyObjectPtr result(callPythonFunction(pyDict, functionName,
        inputObj.get()));
    }
  } catch(castor::exception::Exception &ex) {
    std::cerr <<
      "strThreadRoutine iteration failed"
      ": iteration=" << i <<
      ": code=" << ex.code() <<
      ": message=" << ex.getMessage().str();
  }

  std::cout << "strThreadRoutine ended"   << std::endl;
  return NULL;
}


void *migrThreadRoutine(void *arg) {
  using namespace castor::rtcopy::mighunter;

  std::cout << "migrThreadRoutine started" << std::endl;

  PyObject        *pyDict       = (PyObject *)arg;
  //const char      *functionName = "steveMigrationPolicy";
  const char      *functionName = "atlasT0MigrPolicy";

  SmartPyObjectPtr inputObj(Py_BuildValue(
    "(s,s,K,K,K,K,K,K,K,K,K,K)",
    "nicolasPool", // (elem->tapePoolName()).c_str(),
    "nicolasFile", // (elem->castorFileName()).c_str(),
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

  int i=1;
  try {
    for(i=1; i<=10000; i++) {
      ScopedPythonLock scopedPythonLock;


      SmartPyObjectPtr result(callPythonFunction(pyDict, functionName,
        inputObj.get()));
    }
  } catch(castor::exception::Exception &ex) {
    std::cerr <<
      "strThreadRoutine iteration failed"
      ": iteration=" << i <<
      ": code=" << ex.code() <<
      ": message=" << ex.getMessage().str();
  }

  std::cout << "migrThreadRoutine ended"   << std::endl;

  return NULL;
}


/**
 * C++ wrapper function of the C pthread_create() function.  This wrapper
 * converts the return value indicating an error into the throw of a
 * castor::exception::Exception.
 */
void pthreadCreate(pthread_t *thread, const pthread_attr_t *attr,
  void *(*start_routine)(void*), void *arg)
  throw(castor::exception::Exception) {

  const int rc = pthread_create(thread, attr, start_routine, arg);

  if(rc != 0) {
    castor::exception::Exception ex(rc);

    ex.getMessage() <<
      "Call to pthread_create() failed";

    throw(ex);
  }
}


/**
 * C++ wrapper function of the C pthread_join() function.  This wrapper
 * converts the return value indicating an error into the throw of a
 * castor::exception::Exception.
 */
void pthreadJoin(pthread_t thread, void **value_ptr)
  throw(castor::exception::Exception) {

  const int rc = pthread_join(thread, value_ptr);

  if(rc != 0) {
    castor::exception::Exception ex(rc);

    ex.getMessage() <<
      "Call to pthread_join() failed";

    throw(ex);
  }
}


PyObject *importPythonModule(char *const moduleName)
  throw(castor::exception::Exception) {

  PyObject *const module = PyImport_ImportModule(moduleName);

  if(module == NULL) {
    castor::exception::Exception ex(ECANCELED);

    ex.getMessage() <<
      "PyImport_ImportModule() call failed"
      ": moduleName=" << moduleName;

    throw(ex);
  }

  return module;
}


/**
 * Append the specified directory to the value of the PYTHONPATH environment
 * variable.
 */
void appendDirectoryToPYTHONPATH(const char *const directory) {
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


int exceptionThrowingMain(int argc, char **argv)
  throw(castor::exception::Exception) {
  using namespace castor::rtcopy::mighunter;

  std::cout <<
    "\n"
    "Testing the multi-thread API of embedded Python from C++\n"
    "========================================================\n" <<
    std::endl;

  // Append /etc/castor/policies to the end of the PYTHONPATH environment
  // variable so the PyImport_ImportModule() function can find the stream and
  // migration policy modules
  appendDirectoryToPYTHONPATH(".");

  // Display the result of modifying PYTHONPATH
  {
    const char *value = getenv("PYTHONPATH");

    if(value == NULL) {
      value = "";
    }

    std::cout << "PYTHONPATH=" << value << std::endl;
  }

  // Initialize Python
  Py_Initialize();

  // Initialize thread support
  //
  // Please note that PyEval_InitThreads() takes a lock on the global Python
  // interpreter
  PyEval_InitThreads();

  //std::cout << "Loading in the stream and migration policies" << std::endl;
  SmartPyObjectPtr streamModule(importPythonModule("stream"));
  PyObject *const strDict = PyModule_GetDict(streamModule.get());
  SmartPyObjectPtr migrationModule(importPythonModule("migration"));
  PyObject *const migrDict = PyModule_GetDict(migrationModule.get());

  // Load in the stream and migration policies
  //std::cout << "Loading in the stream and migration policies" << std::endl;
  //PyObject *const strDict  = loadPythonScript("stream.py");
  //PyObject *const migrDict = loadPythonScript("migration.py");

  // Release the lock on the Python global interpreter that was taken by the
  // preceding PyEval_InitThreads() call
  PyEval_ReleaseLock();

  // Display the pointers to the module dictionaries to see if they are infact
  // the same one
  std::cout << std::hex <<
    "strDict =0x" << (uint64_t)strDict  << "\n"
    "migrDict=0x" << (uint64_t)migrDict << "\n" <<
    std::dec << std::endl;

  // Create the stream and migration threads
  std::cout << "Creating the stream and migration threads" << std::endl;
  pthread_t strThread;
  pthread_t migrThread;
  pthreadCreate(&strThread , NULL, strThreadRoutine , strDict );
  pthreadCreate(&migrThread, NULL, migrThreadRoutine, migrDict);

  // Join with the stream and migration threads
  void *strThreadResult  = NULL;
  void *migrThreadResult = NULL;
  std::cout << "Joining with the stream thread"    << std::endl;
  pthreadJoin(strThread , &strThreadResult);
  std::cout << "Joining with the migration thread" << std::endl;
  pthreadJoin(migrThread, &migrThreadResult);

  return 0;
}


int main(int argc, char **argv) {
  try {
    exceptionThrowingMain(argc, argv);
  } catch(castor::exception::Exception &ex) {
    std::cerr <<
      "Test failed"
      ": Caught an exception"
      ": code=" << ex.code() <<
      ": message=" << ex.getMessage().str() << std::endl;

    return 1;
  }

  std::cout << "Test finished successfully\n" << std::endl;

  return 0;
}
