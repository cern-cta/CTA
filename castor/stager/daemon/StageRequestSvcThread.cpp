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
* @(#)$RCSfile: StageRequestSvcThread.cpp,v $ $Revision: 1.4 $ $Release$ $Date: 2008/01/15 17:41:37 $ $Author: itglp $
*
* Service thread for handling stager specific requests
*
* @author castor dev team
*****************************************************************************/

#include "castor/stager/daemon/RequestHelper.hpp"
#include "castor/stager/daemon/CnsHelper.hpp"
#include "castor/stager/daemon/ReplyHelper.hpp"
#include "castor/stager/daemon/RequestHandler.hpp"
#include "castor/stager/daemon/JobRequestHandler.hpp"
#include "castor/stager/daemon/StageRequestSvcThread.hpp"

#include "castor/stager/daemon/PutDoneHandler.hpp"
#include "castor/stager/daemon/RmHandler.hpp"
#include "castor/stager/daemon/SetGCWeightHandler.hpp"

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
castor::stager::daemon::StageRequestSvcThread::StageRequestSvcThread() throw()
  : BaseRequestSvcThread("StageReqSvc", "DbStagerSvc", castor::SVC_DBSTAGERSVC) {}
      
//-----------------------------------------------------------------------------
// process
//-----------------------------------------------------------------------------
void castor::stager::daemon::StageRequestSvcThread::process(castor::IObject* subRequestToProcess) throw() {
 
  RequestHelper* stgRequestHelper= NULL;
  RequestHandler* stgRequestHandler = NULL;
  
  try {         
    int typeRequest=0;
    stgRequestHelper = new RequestHelper(dynamic_cast<castor::stager::SubRequest*>(subRequestToProcess), typeRequest);
    
    switch(typeRequest){
      
      case OBJ_StagePutDoneRequest:
      stgRequestHandler = new PutDoneHandler(stgRequestHelper);
      break;
      
      case OBJ_StageRmRequest:
      stgRequestHandler = new RmHandler(stgRequestHelper);
      break;
      
      case OBJ_SetFileGCWeight:
      stgRequestHandler = new SetGCWeightHandler(stgRequestHelper);
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
