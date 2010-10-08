/******************************************************************************
*                castor/stager/daemon/JobRequestSvcThread.cpp
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
* @(#)$RCSfile: JobRequestSvcThread.cpp,v $ $Revision: 1.6 $ $Release$ $Date: 2009/07/13 06:22:08 $ $Author: waldron $
*
* Service thread for handling Job oriented requests
*
* @author castor dev team
*****************************************************************************/

#include "castor/stager/daemon/RequestHelper.hpp"
#include "castor/stager/daemon/CnsHelper.hpp"
#include "castor/stager/daemon/ReplyHelper.hpp"
#include "castor/stager/daemon/RequestHandler.hpp"
#include "castor/stager/daemon/JobRequestHandler.hpp"
#include "castor/stager/daemon/JobRequestSvcThread.hpp"

#include "castor/stager/daemon/GetHandler.hpp"
#include "castor/stager/daemon/PutHandler.hpp"
#include "castor/stager/daemon/UpdateHandler.hpp"

#include "castor/BaseObject.hpp"
#include "castor/PortsConfig.hpp"
#include "castor/server/BaseServer.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"

#include "stager_constants.h"
#include "Cns_api.h"
#include "expert_api.h"
#include "serrno.h"

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
castor::stager::daemon::JobRequestSvcThread::JobRequestSvcThread() throw (castor::exception::Exception) :
  BaseRequestSvcThread("JobReqSvc", "DbStagerSvc", castor::SVC_DBSTAGERSVC)
{
  m_jobManagerHost = castor::PortsConfig::getInstance()->getHostName(castor::CASTOR_JOBMANAGER);
  m_jobManagerPort = castor::PortsConfig::getInstance()->getNotifPort(castor::CASTOR_JOBMANAGER);
}

//-----------------------------------------------------------------------------
// process
//-----------------------------------------------------------------------------
void castor::stager::daemon::JobRequestSvcThread::process(castor::IObject* subRequestToProcess) throw () {

  RequestHelper* stgRequestHelper = NULL;
  JobRequestHandler* stgRequestHandler = NULL;

  try {
    int typeRequest=0;
    stgRequestHelper = new RequestHelper(dynamic_cast<castor::stager::SubRequest*>(subRequestToProcess), typeRequest);

    switch(typeRequest){

      case OBJ_StageGetRequest:
        stgRequestHandler = new GetHandler(stgRequestHelper);
        break;

      case OBJ_StagePutRequest:
        stgRequestHandler = new PutHandler(stgRequestHelper);
        break;

      case OBJ_StageUpdateRequest:
        stgRequestHandler = new UpdateHandler(stgRequestHelper);
        break;

      default:
        // XXX should never happen, but happens?!
        castor::exception::Internal e;
        e.getMessage() << "Request type " << typeRequest << " not correct for stager svc " << m_name;
        stgRequestHelper->logToDlf(DLF_LVL_ERROR, STAGER_INVALID_TYPE, 0);
        throw e;
    }

    stgRequestHandler->preHandle();
    stgRequestHandler->handle();

    if (stgRequestHandler->notifyJobManager()) {
      castor::server::BaseServer::sendNotification(m_jobManagerHost, m_jobManagerPort, 'D');
    }

    delete stgRequestHelper;
    delete stgRequestHandler;
  }
  catch(castor::exception::Exception& ex) {

    handleException(stgRequestHelper, (stgRequestHandler ? stgRequestHandler->getStgCnsHelper() : 0), ex.code(), ex.getMessage().str());

    /* we delete our objects */
    if(stgRequestHelper) delete stgRequestHelper;
    if(stgRequestHandler) delete stgRequestHandler;
  }
}
