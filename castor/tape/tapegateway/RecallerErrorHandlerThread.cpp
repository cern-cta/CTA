/******************************************************************************
 *                      RecallerErrorHandlerThread.cpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2004  CERN
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
 * @(#)$RCSfile: RecallerErrorHandlerThread.cpp,v $ $Author: waldron $
 *
 *
 *
 * @author Giulia Taurelli
 *****************************************************************************/


#include <sys/types.h>
#include <stdlib.h>
#include <unistd.h>
#include <u64subr.h>

#include "castor/IService.hpp"
#include "castor/Constants.hpp"
#include "castor/Services.hpp"

#include "castor/exception/Internal.hpp"
#include "castor/infoPolicy/DbInfoRetryPolicy.hpp"
#include "castor/infoPolicy/PolicyObj.hpp"
#include "castor/stager/TapeCopy.hpp"

#include "castor/tape/tapegateway/DlfCodes.hpp"
#include "castor/tape/tapegateway/ITapeGatewaySvc.hpp"
#include "castor/tape/tapegateway/RecallerErrorHandlerThread.hpp"


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapegateway::RecallerErrorHandlerThread::RecallerErrorHandlerThread(castor::infoPolicy::TapeRetryPySvc* retryPySvc ){
  m_retryPySvc=retryPySvc;
}

//------------------------------------------------------------------------------
// runs the thread
//------------------------------------------------------------------------------
void castor::tape::tapegateway::RecallerErrorHandlerThread::run(void* par)
{
  // get failed recall tapecopies


 // service to access the database
  castor::IService* dbSvc = castor::BaseObject::services()->service("OraTapeGatewaySvc", castor::SVC_ORATAPEGATEWAYSVC);
  castor::tape::tapegateway::ITapeGatewaySvc* oraSvc = dynamic_cast<castor::tape::tapegateway::ITapeGatewaySvc*>(dbSvc);


  if (0 == oraSvc) {
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, FATAL_ERROR, 0, NULL);
    return;
  }

  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, REC_ERROR_GETTING_FILES, 0, NULL);
  std::vector<castor::stager::TapeCopy*> tcList;
  try {
    tcList =  oraSvc->getFailedRecalls();
  }  catch (castor::exception::Exception e){

    castor::dlf::Param params[] =
      {castor::dlf::Param("errorCode",sstrerror(e.code())),
       castor::dlf::Param("errorMessage",e.getMessage().str())
      };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, REC_ERROR_NO_FILE, 2, params);
    return;
  }

  std::vector<u_signed64> tcIdsToRetry;
  std::vector<u_signed64> tcIdsToFail;

  std::vector<castor::stager::TapeCopy*>::iterator tcItem =tcList.begin();
  while (tcItem != tcList.end()){

    // set the policy object
    castor::infoPolicy::DbInfoRetryPolicy* dbInfo = new castor::infoPolicy::DbInfoRetryPolicy();
    dbInfo->setTapecopy(*tcItem);
    castor::infoPolicy::PolicyObj* policyObj = new castor::infoPolicy::PolicyObj();
    policyObj->addDbInfoPolicy(dbInfo);

    //apply the policy

    castor::dlf::Param params[] =
      {castor::dlf::Param("tapecopyId",(*tcItem)->id())
      };
    try {
      if (m_retryPySvc==NULL || m_retryPySvc->applyPolicy(policyObj)) {
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, REC_ERROR_RETRY, 1, params);
	tcIdsToRetry.push_back( (*tcItem)->id());
      }    else  {
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, REC_ERROR_FAILED, 1, params);
	tcIdsToFail.push_back( (*tcItem)->id());
      }
    } catch (castor::exception::Exception e) {
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_DEBUG, REC_ERROR_RETRY_BY_DEFAULT, 1, params);
      tcIdsToRetry.push_back( (*tcItem)->id());
    }

    tcItem++;

  }

  // update the db
 try {
  oraSvc->setRecRetryResult(tcIdsToRetry,tcIdsToFail);
 } catch (castor::exception::Exception e) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("errorCode",sstrerror(e.code())),
       castor::dlf::Param("errorMessage",e.getMessage().str())
      };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, REC_ERROR_CANNOT_UPDATE_DB, 2, params);
  }

 //clean up

 tcItem =tcList.begin();
 while (tcItem != tcList.end()){
   if (*tcItem) delete *tcItem;
   *tcItem=NULL;
   tcItem++;
 }
 tcList.clear();

 tcIdsToRetry.clear();
 tcIdsToFail.clear();

}

