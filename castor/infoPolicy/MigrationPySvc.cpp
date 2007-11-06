
/******************************************************************************
 *                      MigrationPySvc.cpp
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
 * @(#)$RCSfile: MigrationPySvc.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2007/11/06 12:56:28 $ $Author: gtaur $
 *
 * @author Giulia Taurelli
 *****************************************************************************/


#include "castor/infoPolicy/MigrationPySvc.hpp" 

#include "castor/infoPolicy/DbInfoMigrationPolicy.hpp"
#include "castor/infoPolicy/DbInfoPolicy.hpp"
#include "castor/infoPolicy/CnsInfoMigrationPolicy.hpp"
#include "castor/infoPolicy/CnsInfoPolicy.hpp"
#include "castor/infoPolicy/PolicyObj.hpp"


bool castor::infoPolicy::MigrationPySvc::applyPolicy(castor::infoPolicy::PolicyObj* pObj) throw(castor::exception::Exception){
  
  // Python initialised correctly
  if (m_pyDict == NULL) {
    castor::exception::Exception ex(SEINTERNAL);
    ex.getMessage() << "Embedded Python Interpreter is not initialised" 
		    << std::endl;
    throw ex;
  }
  castor::infoPolicy::DbInfoMigrationPolicy* myInfoDb= dynamic_cast<castor::infoPolicy::DbInfoMigrationPolicy *> (pObj->dbInfoPolicy()[0]);
  castor::infoPolicy::CnsInfoMigrationPolicy* myInfoCns= dynamic_cast<castor::infoPolicy::CnsInfoMigrationPolicy *> (pObj->cnsInfoPolicy()[0]);

  // python-Bugs-1308740  Py_BuildValue (C/API): "K" format
  // K must be used for unsigned (feature not documented at all but available)

  PyObject *inputScript= Py_BuildValue("(s,s,K,K,K,K,K,K,K,K,K,l)", (myInfoDb->tapePoolName()).c_str(), (myInfoDb->castorFileName()).c_str(),myInfoDb->copyNb(),myInfoCns->fileId(),myInfoCns->fileSize(),myInfoCns->fileMode(),myInfoCns->uid(),myInfoCns->gid(),myInfoCns->aTime(),myInfoCns->mTime(),myInfoCns->cTime(),myInfoCns->fileClass());
  bool ret = castor::infoPolicy::PySvc::callPolicyFunction(pObj->policyName(),inputScript);

  return ret;
  
}

