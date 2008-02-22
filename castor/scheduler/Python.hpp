/******************************************************************************
 *                      Python.hpp
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
 * @(#)$RCSfile: Python.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2008/02/22 08:57:52 $ $Author: waldron $
 *
 * @author Dennis Waldron
 *****************************************************************************/

#ifndef SCHEDULER_PYTHON_HPP
#define SCHEDULER_PYTHON_HPP 1

#ifndef Py_PYTHON_H
  #include "Python.h"
#endif

// Include files
#include "castor/exception/Exception.hpp"
#include <string>


namespace castor {

  namespace scheduler {

    /**
     * Python
     */
    class Python {

    public:

      /**
       * Default constructor
       */
      Python();

      /**
       * Initialize the Python embedded interpreter loading the given module
       * @param module The location of the Python module to load
       * @exception Exception in case of error
       */
      Python(std::string module)
	throw(castor::exception::Exception);

      /**
       * Default destructor
       */
      virtual ~Python();

      /**
       * Return the traceback from a previous Python error
       * @return The last reported traceback
       */
      std::string traceback();

      /**
       * Return the version of the Python library loaded in the constructor
       * @return the version string
       */
      std::string version();

      /**
       * Set a value in the global namespace of the module
       * @param name The name of the value to alter
       * @param value A 64 bit integer
       */
      void pySet(char *name, u_signed64 value);

      /**
       * Set a vlue in the global namespace of the module
       * @param name The name of the value to alter
       * @param value A 32 integer
       */
      void pySet(char *name, int value);

      /**
       * Set a value in the global namespace of the module
       * @param name The name of the value to alter
       * @param value An unsigned integer value
       */
      void pySet(char *name, unsigned int value);

      /**
       * Set a value in the global namespace of the module
       * @param name The name of the value to alter
       * @param value A string to set
       */
      void pySet(char *name, std::string value);

      /**
       * Execute a function in the Python Module
       * @param function The function to call
       * @return The return value of the function represented as a float
       * @exception Exception in case of error
       */
      float pyExecute(std::string function)
	throw(castor::exception::Exception);

    private:

      /// The location of the module loaded by the Python interpreter
      std::string m_policyFile;

      /// Flag to indicate whether the Initialization was successful
      bool m_init;

      /// The Python module handle
      PyObject *m_pyModule;

      /// The Dictionary of the python module
      PyObject *m_pyDict;

    };

  } // End of namespace scheduler

} // End of namespace castor

#endif // SCHEDULER_PYTHON_HPP
