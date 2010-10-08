/******************************************************************************
 *                castor/stager/daemon/BaseRequestSvcThread.cpp
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
 * @(#)$RCSfile: BaseRequestSvcThread.cpp,v $ $Revision: 1.6 $ $Release$ $Date: 2008/01/18 17:57:28 $ $Author: itglp $
 *
 * Base service thread for handling stager requests
 *
 * @author castor dev team
 *****************************************************************************/


#include "castor/stager/daemon/RequestHelper.hpp"
#include "castor/stager/daemon/CnsHelper.hpp"
#include "castor/stager/daemon/ReplyHelper.hpp"
#include "castor/stager/daemon/BaseRequestSvcThread.hpp"
#include "castor/stager/daemon/DlfMessages.hpp"

#include "castor/stager/IStagerSvc.hpp"
#include "castor/Services.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/FileRequest.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/Constants.hpp"
#include "castor/dlf/Dlf.hpp"
#include "castor/dlf/Param.hpp"


//-----------------------------------------------------------------------------
// select
//-----------------------------------------------------------------------------
castor::IObject* castor::stager::daemon::BaseRequestSvcThread::select() throw() {
  try {
    castor::IService* svc =
      castor::BaseObject::services()->service(m_dbSvcName, m_dbSvcType);
    // we have already initialized the services in the main, so we know the pointer is valid
    castor::IObject* req = 0;
    if(m_dbSvcType == castor::SVC_DBSTAGERSVC) {
      castor::stager::IStagerSvc* stgSvc = dynamic_cast<castor::stager::IStagerSvc*>(svc);
      req = stgSvc->subRequestToDo(m_name);
    }
    else {
      castor::stager::ICommonSvc* cSvc = dynamic_cast<castor::stager::ICommonSvc*>(svc);
      req = cSvc->requestToDo(m_name);
    }
    return req;
  } catch (castor::exception::Exception& e) {
    // "Unexpected exception caught"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Function", "BaseRequestSvcThread::select"),
       castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param("Code", e.code())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, castor::stager::daemon::STAGER_SERVICES_EXCEPTION, 3, params);
    return 0;
  }
}
     

//-----------------------------------------------------------------------------
// handleException
//-----------------------------------------------------------------------------
void castor::stager::daemon::BaseRequestSvcThread::handleException(
  RequestHelper* stgRequestHelper, CnsHelper* stgCnsHelper, int errorCode, std::string errorMessage) throw() {
  if(stgRequestHelper == 0 || stgRequestHelper->dbSvc == 0 || stgRequestHelper->subrequest == 0) {
    // exception thrown before being able to do anything with the db
    // we can't do much here...
    return;        
  }
  stgRequestHelper->subrequest->setStatus(SUBREQUEST_FAILED_FINISHED);

  if(stgRequestHelper->fileRequest != NULL) {
    try {
      // inform the client about the error
      ReplyHelper *stgReplyHelper = new ReplyHelper();
      stgReplyHelper->setAndSendIoResponse(stgRequestHelper, (stgCnsHelper ? &(stgCnsHelper->cnsFileid) : 0), errorCode, errorMessage);
      stgReplyHelper->endReplyToClient(stgRequestHelper);
      delete stgReplyHelper;
    } catch (castor::exception::Exception& ignored) {}
  }
  else {
    // if we didn't get the fileRequest, we probably got a serious failure, and we can't answer the client
    // just try to update the db
    try {
      stgRequestHelper->dbSvc->updateRep(stgRequestHelper->baseAddr, stgRequestHelper->subrequest, true);
    }
    catch (castor::exception::Exception& ignored) {}
  }
}
