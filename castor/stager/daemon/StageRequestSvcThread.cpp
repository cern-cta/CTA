/******************************************************************************
*                castor/stager/daemon/StageRequestSvcThread.cpp
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
* @(#)$RCSfile: StageRequestSvcThread.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2007/12/14 16:45:47 $ $Author: itglp $
*
* Service thread for handling stager specific requests
*
* @author castor dev team
*****************************************************************************/

#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"
#include "castor/stager/dbService/StagerRequestHandler.hpp"
#include "castor/stager/dbService/StagerJobRequestHandler.hpp"
#include "castor/stager/dbService/StageRequestSvcThread.hpp"

#include "castor/stager/dbService/StagerGetHandler.hpp"
#include "castor/stager/dbService/StagerRepackHandler.hpp"
#include "castor/stager/dbService/StagerPrepareToGetHandler.hpp"
#include "castor/stager/dbService/StagerPrepareToPutHandler.hpp"
#include "castor/stager/dbService/StagerPutHandler.hpp"
#include "castor/stager/dbService/StagerPutDoneHandler.hpp"
#include "castor/stager/dbService/StagerPrepareToUpdateHandler.hpp"
#include "castor/stager/dbService/StagerUpdateHandler.hpp"
#include "castor/stager/dbService/StagerRmHandler.hpp"
#include "castor/stager/dbService/StagerSetGCHandler.hpp"

#include "castor/BaseObject.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"

#include "stager_constants.h"
#include "Cns_api.h"
#include "expert_api.h"
#include "serrno.h"

#include "dlf_api.h"
#include "castor/dlf/Dlf.hpp"
#include "castor/dlf/Param.hpp"

#include "osdep.h"
#include "Cnetdb.h"
#include "Cpwd.h"
#include "Cgrp.h"
#include "stager_uuid.h"
#include "Cuuid.h"
#include "u64subr.h"
#include "marshall.h"
#include "net.h"

#include "castor/IAddress.hpp"
#include "castor/IObject.hpp"
#include "castor/Services.hpp"
#include "castor/stager/IStagerSvc.hpp"
#include "castor/BaseObject.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/Constants.hpp"

#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"

#include "serrno.h"
#include <errno.h>
#include <iostream>
#include <string>

      
//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::stager::dbService::StageRequestSvcThread::StageRequestSvcThread() throw()
  : BaseRequestSvcThread("StageReqSvc", "DbStagerSvc", castor::SVC_DBSTAGERSVC) {}
      
//-----------------------------------------------------------------------------
// process
//-----------------------------------------------------------------------------
void castor::stager::dbService::StageRequestSvcThread::process(castor::IObject* subRequestToProcess) throw() {
 
  StagerRequestHelper* stgRequestHelper= NULL;
  StagerRequestHandler* stgRequestHandler = NULL;
  
  try {         
    int typeRequest=0;
    stgRequestHelper = new StagerRequestHelper(dynamic_cast<castor::stager::SubRequest*>(subRequestToProcess), typeRequest);
    
    switch(typeRequest){
      
      case OBJ_StagePutDoneRequest:
      stgRequestHandler = new StagerPutDoneHandler(stgRequestHelper);
      break;
      
      case OBJ_StageRmRequest:
      stgRequestHandler = new StagerRmHandler(stgRequestHelper);
      break;
      
      case OBJ_SetFileGCWeight:
      stgRequestHandler = new StagerSetGCHandler(stgRequestHelper);
      break;
      
      default:
        // XXX should never happen, but happens?!
        castor::exception::Internal e;
        e.getMessage() << "Request type " << typeRequest << " not correct for stager svc " << m_name;
        stgRequestHelper->logToDlf(DLF_LVL_ERROR, STAGER_INVALID_TYPE, 0);
        throw e;
    }//end switch(typeRequest)
    
    stgRequestHandler->preHandle();
    stgRequestHandler->handle();
    
    delete stgRequestHandler;
    delete stgRequestHelper;
   
  }
  catch(castor::exception::Exception ex){
    
    handleException(stgRequestHelper, (stgRequestHandler ? stgRequestHandler->getStgCnsHelper() : 0), ex.code(), ex.getMessage().str());
    
    /* we delete our objects */
    if(stgRequestHandler) delete stgRequestHandler;
    if(stgRequestHelper) delete stgRequestHelper;
  }
}
