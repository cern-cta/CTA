/******************************************************************************
 *                castor/stager/daemon/GcRequestSvcThread.cpp
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
 * @(#)$RCSfile: GcSvcThread.cpp,v $ $Revision: 1.16 $ $Release$ $Date: 2007/10/24 09:17:12 $ $Author: sponcec3 $
 *
 * Service thread for garbage collection related requests
 *
 * @author castor dev team
 *****************************************************************************/

#include "Cthread_api.h"
#include "Cmutex.h"

#include <vector>

#include "castor/Services.hpp"
#include "castor/Constants.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/IClient.hpp"
#include "castor/IService.hpp"
#include "castor/stager/IGCSvc.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/BaseObject.hpp"
#include "castor/stager/Request.hpp"
#include "castor/stager/GCFileList.hpp"
#include "castor/stager/GCFile.hpp"
#include "castor/stager/Files2Delete.hpp"
#include "castor/stager/GCLocalFile.hpp"
#include "castor/rh/GCFilesResponse.hpp"
#include "castor/rh/BasicResponse.hpp"
#include "castor/replier/RequestReplier.hpp"
#include "castor/stager/dbService/StagerDlfMessages.hpp"
#include "castor/stager/dbService/GcSvcThread.hpp"

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::stager::dbService::GcSvcThread::GcSvcThread() throw () {}

//-----------------------------------------------------------------------------
// select
//-----------------------------------------------------------------------------
castor::IObject* castor::stager::dbService::GcSvcThread::select()
  throw() {
  try {
    // get the GcSvc. Note that we cannot cache it since we
    // would not be thread safe
    castor::Services *svcs = castor::BaseObject::services();
    castor::IService *svc = svcs->service("DbGCSvc", castor::SVC_DBGCSVC);
    castor::stager::IGCSvc *gcSvc = dynamic_cast<castor::stager::IGCSvc*>(svc);
    if (0 == gcSvc) {
      // "Could not get GCSvc"
      castor::dlf::Param params[] =
        {castor::dlf::Param("function", "GcSvcThread::select")};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, STAGER_GCSVC_GETSVC, 1, params);
      return 0;
    }
    // actual work
    castor::stager::Request* req = gcSvc->requestToDo();
    gcSvc->release();
    return req;
  } catch (castor::exception::Exception e) {
    // "Unexpected exception caught"
    castor::dlf::Param params[] =
      {castor::dlf::Param("function", "GcSvcThread::select"),
       castor::dlf::Param("message", e.getMessage().str()),
       castor::dlf::Param("code", e.code())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, STAGER_GCSVC_EXCEPT, 1, params);
    return 0;
  }
}

//-----------------------------------------------------------------------------
// handleFilesDeletedOrFailed
//-----------------------------------------------------------------------------
void castor::stager::dbService::GcSvcThread::handleFilesDeletedOrFailed
(castor::stager::Request* req,
 castor::IClient *client,
 castor::Services* svcs,
 castor::stager::IGCSvc* gcSvc,
 castor::BaseAddress &ad,
 Cuuid_t uuid) throw() {
  castor::rh::BasicResponse res;
  try {
    // cannot return 0 since we check the type before calling this method
    castor::stager::GCFileList *uReq =
      dynamic_cast<castor::stager::GCFileList*> (req);
    // Fills it with files to be deleted
    svcs->fillObj(&ad, req, castor::OBJ_GCFile);
    /* Invoking the method                */
    /* ---------------------------------- */
    std::vector<u_signed64*> arg;
    u_signed64* argArray = new u_signed64[uReq->files().size()];
    int i = 0;
    for (std::vector<castor::stager::GCFile*>::iterator it =
           uReq->files().begin();
         it != uReq->files().end();
         it++) {
      argArray[i] = (*it)->diskCopyId();
      arg.push_back(&(argArray[i]));
      i++;
    }
    if (castor::OBJ_FilesDeleted == req->type()) {
      // "Invoking filesDeleted"
      castor::dlf::dlf_writep(uuid, DLF_LVL_USAGE, STAGER_GCSVC_FDELOK);
      gcSvc->filesDeleted(arg);
    } else {
      // "Invoking filesDeletionFailed"
      castor::dlf::dlf_writep(uuid, DLF_LVL_USAGE, STAGER_GCSVC_FDELFAIL);
      gcSvc->filesDeletionFailed(arg);
    }
    delete[] argArray;
  } catch (castor::exception::Exception e) {
    // "Unexpected exception caught"
    castor::dlf::Param params[] =
      {castor::dlf::Param("function", "GcSvcThread::handleFilesDeletedOrFailed"),
       castor::dlf::Param("message", e.getMessage().str()),
       castor::dlf::Param("code", e.code())};
    castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_GCSVC_EXCEPT, 3, params);
    res.setErrorCode(e.code());
    res.setErrorMessage(e.getMessage().str());
  }
  // Reply To Client
  try {
    castor::replier::RequestReplier *rr =
      castor::replier::RequestReplier::getInstance();
    res.setReqAssociated(req->reqId());
    rr->sendResponse(client, &res, true);
  } catch (castor::exception::Exception e) {
    // "Unexpected exception caught"
    castor::dlf::Param params[] =
      {castor::dlf::Param("function", "GcSvcThread::handleFilesDeletedOrFailed.reply"),
       castor::dlf::Param("message", e.getMessage().str()),
       castor::dlf::Param("code", e.code())};
    castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_GCSVC_EXCEPT, 3, params);
  }
}

//-----------------------------------------------------------------------------
// handleFiles2Delete
//-----------------------------------------------------------------------------
void castor::stager::dbService::GcSvcThread::handleFiles2Delete
(castor::stager::Request* req,
 castor::IClient *client,
 castor::Services* svcs,
 castor::stager::IGCSvc* gcSvc,
 castor::BaseAddress &ad,
 Cuuid_t uuid) throw() {
  // Useful Variables
  castor::stager::Files2Delete *uReq;
  std::vector<castor::stager::GCLocalFile*>* result = 0;
  castor::rh::GCFilesResponse res;
  try {
    // get the Files2Delete
    // cannot return 0 since we check the type before calling this method
    uReq = dynamic_cast<castor::stager::Files2Delete*> (req);
    // Invoking the method
    result = gcSvc->selectFiles2Delete(uReq->diskServer());
  } catch (castor::exception::Exception e) {
    // "Unexpected exception caught"
    castor::dlf::Param params[] =
      {castor::dlf::Param("function", "GcSvcThread::handleFiles2Delete"),
       castor::dlf::Param("message", e.getMessage().str()),
       castor::dlf::Param("code", e.code())};
    castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_GCSVC_EXCEPT, 3, params);
    res.setErrorCode(e.code());
    res.setErrorMessage(e.getMessage().str());
  }
  if (0 != result) {
    for(std::vector<castor::stager::GCLocalFile *>::iterator it =
          result->begin();
        it != result->end();
        it++) {
      // Here we transfer the ownership of the GCLocalFiles
      // to res. Result can thus be deleted with no risk
      // of memory leak
      res.addFiles(*it);
    }
  }
  // Reply To Client
  try {
    castor::replier::RequestReplier *rr =
      castor::replier::RequestReplier::getInstance();
    res.setReqAssociated(req->reqId());
    rr->sendResponse(client, &res, true);
  } catch (castor::exception::Exception e) {
    // "Unexpected exception caught"
    castor::dlf::Param params[] =
      {castor::dlf::Param("function", "GcSvcThread::handleFiles2Delete.reply"),
       castor::dlf::Param("message", e.getMessage().str()),
       castor::dlf::Param("code", e.code())};
    castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_GCSVC_EXCEPT, 3, params);
  }
  // Cleanup
  if (result) delete result;
}

//-----------------------------------------------------------------------------
// process
//-----------------------------------------------------------------------------
void castor::stager::dbService::GcSvcThread::process
(castor::IObject *param) throw() {
  // Useful variables
  castor::stager::Request* req = 0;
  castor::Services *svcs = 0;
  castor::stager::IGCSvc *gcSvc = 0;
  Cuuid_t uuid = nullCuuid;
  castor::IClient *client = 0;
  // address to access db
  castor::BaseAddress ad;
  ad.setCnvSvcName("DbCnvSvc");
  ad.setCnvSvcType(castor::SVC_DBCNV);
  try {
    // get the GCSvc. Note that we cannot cache it since we
    // would not be thread safe
    svcs = castor::BaseObject::services();
    castor::IService* svc = svcs->service("DbGCSvc", castor::SVC_DBGCSVC);
    gcSvc = dynamic_cast<castor::stager::IGCSvc*>(svc);
    if (0 == gcSvc) {
      // "Could not get GCSvc"
      castor::dlf::Param params[] =
        {castor::dlf::Param("function", "GcSvcThread::process")};
      castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_GCSVC_GETSVC, 1, params);
      return;
    }
    // Retrieving request and client from the database
    // Note that casting the request will never give 0
    // since select does return a request for sure
    req = dynamic_cast<castor::stager::Request*>(param);
    string2Cuuid(&uuid, (char*)req->reqId().c_str());
    // Getting the client
    svcs->fillObj(&ad, req, castor::OBJ_IClient);
    client = req->client();
    if (0 == client) {
      // "No client associated with request ! Cannot answer !"
      castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_GCSVC_NOCLI);
      delete req;
      gcSvc->release();
      return;
    }
  } catch (castor::exception::Exception e) {
    // If we fail here, we do NOT have enough information
    // to reply to the client ! So we only log something.
    // "Unexpected exception caught"
    castor::dlf::Param params[] =
      {castor::dlf::Param("function", "GcSvcThread::process.1"),
       castor::dlf::Param("message", e.getMessage().str()),
       castor::dlf::Param("code", e.code())};
    castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_GCSVC_EXCEPT, 3, params);
    if (req) delete req;
    if (gcSvc) gcSvc->release();
    return;
  }

  // At this point we are able to reply to the client
  switch (req->type()) {
  case castor::OBJ_FilesDeleted:
  case castor::OBJ_FilesDeletionFailed:
    castor::stager::dbService::GcSvcThread::handleFilesDeletedOrFailed
      (req, client, svcs, gcSvc, ad, uuid);
    break;
  case castor::OBJ_Files2Delete:
    castor::stager::dbService::GcSvcThread::handleFiles2Delete
      (req, client, svcs, gcSvc, ad, uuid);
    break;
  default:
    // "Unknown Request type"
    castor::dlf::Param params[] =
      {castor::dlf::Param("type", req->type())};
    castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_GCSVC_UNKREQ, 1, params);
    if (req) {delete req; req=0;}
    if (gcSvc) gcSvc->release();
    return;
  }
  try {
    // Delete Request From the database
    svcs->deleteRep(&ad, req, true);
  } catch (castor::exception::Exception e) {
    // "Unexpected exception caught"
    castor::dlf::Param params[] =
      {castor::dlf::Param("function", "GcSvcThread::process.2"),
       castor::dlf::Param("message", e.getMessage().str()),
       castor::dlf::Param("code", e.code())};
    castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_GCSVC_EXCEPT, 3, params);
  }
  // Final cleanup
  if (req) { delete req; req=0;}
  if (gcSvc) gcSvc->release();
  return;
}
