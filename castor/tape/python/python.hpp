/******************************************************************************
 *                      castor/tape/python/python.hpp
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
 * @author Giulia Taurelli, Nicola Bessone and Steven Murray
 *****************************************************************************/

#ifndef CASTOR_TAPE_PYTHON_PYTHON_HPP
#define CASTOR_TAPE_PYTHON_PYTHON_HPP

// Include Python.h before any standard headers because Python.h may define
// some pre-processor definitions which affect the standard headers
#include <Python.h>

#include "castor/exception/Exception.hpp"

#include <string>
#include <vector>

namespace castor {
namespace tape   {
namespace python {

/**
 * Initializes the embedded Python interpreter for multi-threaded use and
 * append the CASTOR_POLICIES_DIRECTORY to the PYTHONPATH environment
 * variable.
 *
 * This function should be once and only once for the entire duration of the
 * calling program.
 *
 * When this function returns there will be no lock taken on the global Python
 * interpeter.  All threads, including the main thread must therefore take a
 * lock using a ScopedPythonLock object before acsessing the API of the
 * embedded Python interpreter.
 */
void initializePython()
  throw(castor::exception::Exception);

/**
 * Finalizes the embedded Python interpreter.
 *
 * Please note that the calling thread must NOT have a lock on the global
 * Python interpreter.
 */
void finalizePython()
  throw();

/**
 * Imports the Python-module with the specified name.
 *
 * Please note that initPython() must be called before this function is called.
 *
 * Please note that the calling thread MUST have a lock on the global Python
 * interpreter through a call to PyGILState_Ensure() or through the strongly
 * recommended use of a ScopedPythonLock object.
 *
 * @param moduleName The name of the Python-module.
 * @return           The Python dictionary object of the imported library.  The
 *                   documentation of the embedded Python-interpreter describes
 *                   the return value as being a "borrowed reference".  This
 *                   means the caller does not need to call Py_XDECREF when the
 *                   dictionary is no longer required.
 */
PyObject* importPythonModule(
  const char *const moduleName)
  throw(castor::exception::Exception);

/**
 * Convenient wrapper method around importPythonModule() that takes a
 * lock on the global Python interpreter and then calls
 * importPythonModule().  The lock is released before this method
 * returns.
 *
 * For further information please see the documentation for the
 * importPythonModule() method.
 *
 * @param moduleName The name of the Python-module.
 * @return           The Python dictionary object of the imported library.  The
 *                   documentation of the embedded Python-interpreter describes
 *                   the return value as being a "borrowed reference".  This
 *                   means the caller does not need to call Py_XDECREF when the
 *                   dictionary is no longer required.
 */
PyObject* importPythonModuleWithLock(
  const char *const moduleName)
  throw(castor::exception::Exception);

/**
 * Imports a CASTOR-policy implemented as a Python module from the
 * Python-module search path which includes the
 * castor::tape::python::CASTOR_POLICIES_DIRECTORY directory.
 *
 * The CASTOR-policy must be implemented by a '*.py' file in the
 * castor::tape::python::CASTOR_POLICIES_DIRECTORY directory.  A '*.pyc' by
 * itself will be rejected.  In this sense the importPolicyPythonModule()
 * function is stricter than the underlying PyImport_ImportModule() method
 * which would work if only a '*.pyc' file was present.  The reason for this
 * enforced strictness is that CASTOR operators should be able to see in a
 * human-readbale form the logic implementing a CASTOR-policy.
 *
 * Please note that initPython() must be called before this function is called.
 *
 * Please note that the calling thread MUST have a lock on the global Python
 * interpreter through a call to PyGILState_Ensure() or through the strongly
 * recommended use of a ScopedPythonLock object.
 *
 * @param moduleName The name of the CASTOR-policy Python-module.
 * @return           The Python dictionary object of the imported library.  The
 *                   documentation of the embedded Python-interpreter describes
 *                   the return value as being a "borrowed reference".  This
 *                   means the caller does not need to call Py_XDECREF when the
 *                   dictionary is no longer required.
 */
PyObject* importPolicyPythonModule(
  const char *const moduleName)
  throw(castor::exception::Exception);

/**
 * Checks that the module file of the specified policy Python-module exists in
 * the CASTOR_POLICIES_DIRECTORY as it is difficult to obtain errors from the
 * embedded Python interpreter.
 *
 * This method raises a castor::exception::Exception if the check fails.
 */
void checkPolicyModuleIsInCastorPoliciesDirectory(
  const char *const moduleName)
  throw(castor::exception::Exception);

/**
 * The same functionality as importPolicyPythonModule but with the additional
 * taking and releasing of the lock on the global Python interpreter.
 *
 * For further information please see the documentation for the
 * importPolicyPythonModule() method.
 *
 * @param moduleName The name of the CASTOR-policy Python-module.
 * @return           The Python dictionary object of the imported library.  The
 *                   documentation of the embedded Python-interpreter describes
 *                   the return value as being a "borrowed reference".  This
 *                   means the caller does not need to call Py_XDECREF when the
 *                   dictionary is no longer required.
 */
PyObject* importPolicyPythonModuleWithLock(
  const char *const moduleName)
  throw(castor::exception::Exception);

/**
 * Get the Python function object for the specified function within the
 * specified Python dictionary.
 *
 * Please note that initPython() must be called before this function is called.
 *
 * Please note that the calling thread MUST have a lock on the global Python
 * interpreter through a call to PyGILState_Ensure() or through the strongly
 * recommended use of a ScopedPythonLock object.
 *
 * @param pyDict       The Python dictionary in which the specified function is
 *                     to be found.
 * @param functionName The name of the Python function.
 * @return             The Python function object representing the specified
 *                     function or NULL if the named function is not in the
 *                     specified dictionary.  The documentation of the embedded
 *                     Python-interpreter describes the return value as being a
 *                     "borrowed reference".  This means the caller does not
 *                     need to call Py_XDECREF when the function is no longer
 *                     required.
 */
PyObject* getPythonFunction(
  PyObject   *const pyDict,
  const char *const functionName)
  throw(castor::exception::Exception);

/**
 * Convenient wrapper method around getPythonFunction() that takes a
 * lock on the global Python interpreter and then calls
 * getPythonFunction().  The lock is released before this method
 * returns.
 *
 * For further information please see the documentation for the
 * getPythonFunction() method.
 *
 * @param pyDict       The Python dictionary in which the specified function is
 *                     to be found.
 * @param functionName The name of the Python function.
 * @return             The Python function object representing the specified
 *                     function or NULL if the named function is not in the
 *                     specified dictionary.  The documentation of the embedded
 *                     Python-interpreter describes the return value as being a
 *                     "borrowed reference".  This means the caller does not
 *                     need to call Py_XDECREF when the function is no longer
 *                     required.
 */
PyObject* getPythonFunctionWithLock(
  PyObject   *const pyDict,
  const char *const functionName)
  throw(castor::exception::Exception);

/**
 * Returns a string representation of the specified Python exception.  This
 * function only knows the standard Python exceptions that were documented at
 * the time the function was written.
 *
 * If a NULL pointer is passed as the Python exception, then this function will
 * return a pointer the string literal "NULL".  If the specified Python
 * exception is unknown, then this function will return a pointer to the string
 * literal "UNKNOWN";
 *
 * The caller of this function must not try to free the memory used by a
 * returned string.  All of the values returned by this function are pointers
 * to string literals.
 *
 * @param pyEx Python exception or NULL if there isn't one.
 * @return     String representation of the specified Python exception.
 */
const char *stdPythonExceptionToStr(
  PyObject *const pyDict)
  throw();

/**
 * Returns the argument names of the specified Python-function.
 *
 * @param inspectGetargspecFunc The inspect.getargspec Python-function.
 * @param pyFunc                The Python-function whose argument names will
 *                              be returned.
 * @param argumentNames         The list of argument names to be filled by this
 *                              function.
 * @return                      The function argument names
 */
std::vector<std::string> &getPythonFunctionArgumentNames(
  PyObject          *const inspectGetargspecFunc,
  PyObject          *const pyFunc,
  std::vector<std::string> &argumentNames)
  throw(castor::exception::Exception);

/**
 * Convenient wrapper method around getPythonFunctionArgumentNames that takes a
 * lock on the global Python interpreter and then calls
 * getPythonFunctionArgumentNames().  The lock is released before this method
 * returns.
 *
 * For further information please see the documentation for the
 * getPythonFunctionArgumentNames() method.
 *
 * @param inspectGetargspecFunc The inspect.getargspec Python-function.
 * @param pyFunc                The Python-function whose argument names will
 *                              be returned.
 * @param argumentNames         The list of argument names to be filled by this
 *                              function.
 * @return                      The function argument names
 */
std::vector<std::string> &getPythonFunctionArgumentNamesWithLock(
  PyObject          *const inspectGetargspecFunc,
  PyObject          *const pyFunc,
  std::vector<std::string> &argumentNames)
  throw(castor::exception::Exception);

} // namespace python
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_PYTHON_PYTHON_HPP
