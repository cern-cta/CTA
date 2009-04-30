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
 * @(#)$RCSfile: RecallerErrorHandlerThread.cpp,v $ $Author: gtaur $
 *
 *
 *
 * @author Giulia Taurelli
 *****************************************************************************/
#include "castor/tape/tapegateway/RecallerErrorHandlerThread.hpp"


#include <sys/types.h>
#include <stdlib.h>
#include <unistd.h>

#include "castor/Services.hpp"
#include "castor/Constants.hpp"
#include "castor/IService.hpp"

#include "castor/exception/Internal.hpp"

#include <u64subr.h>

  
#include  "castor/infoPolicy/PolicyObj.hpp"
#include  "castor/infoPolicy/DbInfoRetryPolicy.hpp"
#include  "castor/stager/TapeCopy.hpp"


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapegateway::RecallerErrorHandlerThread::RecallerErrorHandlerThread(castor::tape::tapegateway::ITapeGatewaySvc* svc, castor::infoPolicy::TapeRetryPySvc* retryPySvc ){
  m_dbSvc=svc; 
  m_retryPySvc=retryPySvc;
}

//------------------------------------------------------------------------------
// runs the thread
//------------------------------------------------------------------------------
void castor::tape::tapegateway::RecallerErrorHandlerThread::run(void* par)
{
  // get failed recall tapecopies

  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 27, 0, NULL); 
  std::vector<castor::stager::TapeCopy*> tcList;
  try {
    tcList =  m_dbSvc->inputForRecallRetryPolicy();
  }  catch (castor::exception::Exception e){
 
    castor::dlf::Param params[] =
      {castor::dlf::Param("errorCode",sstrerror(e.code())),
       castor::dlf::Param("errorMessage",e.getMessage().str())
      };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 28, 2, params); 
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
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 29, 1, params); 
	tcIdsToRetry.push_back( (*tcItem)->id());
      }    else  {
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 30, 1, params); 
	tcIdsToFail.push_back( (*tcItem)->id());
      }
    } catch (castor::exception::Exception e) {
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_USAGE, 31, 1, params); 
      tcIdsToRetry.push_back( (*tcItem)->id());
    }

    tcItem++;

  }

  // update the db 
 try {
  m_dbSvc->updateWithRecallRetryPolicyResult(tcIdsToRetry,tcIdsToFail); 
 } catch (castor::exception::Exception e) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("errorCode",sstrerror(e.code())),
       castor::dlf::Param("errorMessage",e.getMessage().str())
      };
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 32, 2, params); 
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

