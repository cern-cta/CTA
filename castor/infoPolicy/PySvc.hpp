/******************************************************************************
 *                      PySvc.hpp
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
 * @(#)$RCSfile: PySvc.hpp,v $ $Revision: 1.3 $ $Release$ $Date: 2008/05/28 08:07:46 $ $Author: gtaur $
 *
 * @author Giulia Taurelli
 *****************************************************************************/

#ifndef PYSVC_HPP
#define PYSVC_HPP 

#ifndef Py_PYTHON_H
  #include "Python.h"
#endif

// Include files
#include "castor/infoPolicy/PolicyObj.hpp"
#include "castor/exception/Exception.hpp"


namespace castor {

  namespace infoPolicy {

    /**
     * PySvc
     */
    class PySvc {
      
    public:
      
      /**
       * Default constructor
       */
      PySvc(){};

      /**
       * Initialize the Python embedded interpreter loading the given module
       * @param module The location of the Python module to load
       * @exception Exception in case of error
       */

      PySvc(std::string module)
	throw(castor::exception::Exception);

      virtual int applyPolicy(castor::infoPolicy::PolicyObj* pObj)=0;
      virtual ~PySvc();

    protected:
   
      // internal function used by applyPolicy

      int callPolicyFunction(std::string functionName, PyObject* inputObj);

      /// The location of the module loaded by the Python interpreter
      std::string m_moduleFile;
      
      /// The Python module handle
      PyObject *m_pyModule;

      /// The Dictionary of the python module
      PyObject *m_pyDict;
      

    };

  } // End of namespace infoPolicy

} // End of namespace castor

#endif // PYSVC_HPP
