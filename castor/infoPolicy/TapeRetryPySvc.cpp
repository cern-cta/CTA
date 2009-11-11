
/******************************************************************************
 *                     TapeRetryPySvc.cpp
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
 * @(#)$RCSfile$ $Revision$ $Release$ $Date$ $Author$
 *
 * @author Giulia Taurelli
 *****************************************************************************/


#include "castor/infoPolicy/TapeRetryPySvc.hpp" 

#include "castor/infoPolicy/RetryPolicyElement.hpp"
#include "castor/infoPolicy/PolicyObj.hpp"
#include "castor/stager/TapeCopy.hpp"


int castor::infoPolicy::TapeRetryPySvc::applyPolicy(castor::infoPolicy::PolicyObj* pObj) throw(castor::exception::Exception){
  
  // Python initialised correctly
  if (m_pyDict == NULL) {
    castor::exception::Exception ex(SEINTERNAL);
    ex.getMessage() << "Embedded Python Interpreter is not initialised" 
		    << std::endl;
    throw ex;
  }
  

  
  castor::infoPolicy::RetryPolicyElement* elem = dynamic_cast<castor::infoPolicy::RetryPolicyElement *> (pObj);



  PyObject *inputScript= Py_BuildValue((char*)"(i,i)", (elem->errorCode()), (elem->nbRetry()));

  int ret = 0;

  try {
    ret=castor::infoPolicy::PySvc::callPolicyFunction(m_functionName,inputScript);
  } catch (castor::exception::Exception e){
    if (inputScript) { Py_XDECREF(inputScript); }
    throw e;
  }

  if (inputScript) { Py_XDECREF(inputScript); }

  
  return ret;
  
}

