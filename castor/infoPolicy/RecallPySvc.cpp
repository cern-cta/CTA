
/******************************************************************************
 *                      RecallPySvc.cpp
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
 * @(#)$RCSfile: RecallPySvc.cpp,v $ $Revision: 1.3 $ $Release$ $Date: 2008/05/28 08:07:46 $ $Author: gtaur $
 *
 * @author Giulia Taurelli
 *****************************************************************************/


#include "castor/infoPolicy/RecallPySvc.hpp" 

#include "castor/infoPolicy/RecallPolicyElement.hpp"

#include "castor/infoPolicy/PolicyObj.hpp"


int castor::infoPolicy::RecallPySvc::applyPolicy(castor::infoPolicy::PolicyObj* pObj) throw(castor::exception::Exception){
  
  // Python initialised correctly

  if (m_pyDict == NULL) {
    castor::exception::Exception ex(SEINTERNAL);
    ex.getMessage() << "Embedded Python Interpreter is not initialised" 
		    << std::endl;
    throw ex;
  }


  castor::infoPolicy::RecallPolicyElement* elem= dynamic_cast<castor::infoPolicy::RecallPolicyElement *> (pObj);

  // python-Bugs-1308740  Py_BuildValue (C/API): "K" format
  // K must be used for unsigned (feature not documented at all but available)

  PyObject *inputScript= Py_BuildValue("(s,K,K,K,K)", (elem->vid()).c_str(),elem->numFiles(),elem->numBytes(),elem->oldest(),elem->priority());
  // the policy return -1 as priority if we don't want to recall at all
  int ret=0;
  
  try {
    ret=callPolicyFunction(m_functionName,inputScript);
  
  } catch (castor::exception::Exception e){
    if (inputScript)  Py_XDECREF(inputScript);
    throw e;
  }

  if (inputScript) Py_XDECREF(inputScript);

  return ret;
  
}


