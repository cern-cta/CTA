/******************************************************************************
 *                      castor/tape/python/PythonUtils.hpp
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
 * @author Giulia Taurelli
 *****************************************************************************/

#ifndef CASTOR_TAPE_PYTHON_PYTHON_UTILS
#define CASTOR_TAPE_PYTHON_PYTHON_UTILS

#include <Python.h>

#include "castor/exception/Exception.hpp"


namespace castor  {
namespace tape    {
namespace python  {

  const char *const CASTOR_POLICIES_DIRECTORY = "/etc/castor/policies";
  
  void startThreadSupportInit();

  PyObject* loadPythonPolicy(const char *const policyModuleName) 
      throw(castor::exception::Exception);

  void endThreadSupportInit();

  PyObject*  getPythonFunction(PyObject* pyDict,const char *const functionName)throw(castor::exception::Exception);

  void appendDirectoryToPYTHONPATH(const char *const directory)throw();
    
  PyObject* importPythonModule(const char *const moduleName)
      throw(castor::exception::Exception);
  
  void checkPolicyModuleFileExists(const char *moduleName)
      throw(castor::exception::Exception);

} // namespace python
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_PYTHON_PYTHON_UTILS
