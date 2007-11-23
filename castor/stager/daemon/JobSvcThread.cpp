/******************************************************************************
 *                castor/stager/daemon/JobSvcThread.hpp
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
 * @(#)$RCSfile: JobSvcThread.cpp,v $ $Revision: 1.47 $ $Release$ $Date: 2007/11/23 11:27:45 $ $Author: sponcec3 $
 *
 * Service thread for job related requests
 *
 * @author castor dev team
 *****************************************************************************/

#include "Cthread_api.h"
#include "Cmutex.h"

#include <list>
#include <vector>
#include <sstream>

#include "castor/Services.hpp"
#include "castor/Constants.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/IObject.hpp"
#include "castor/IClient.hpp"
#include "castor/IService.hpp"
#include "castor/stager/IJobSvc.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/BaseObject.hpp"
#include "castor/stager/Request.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/DiskCopy.hpp"
#include "castor/stager/DiskCopyInfo.hpp"
#include "castor/stager/DiskServer.hpp"
#include "castor/stager/DiskCopyForRecall.hpp"
#include "castor/stager/FileSystem.hpp"
#include "castor/stager/GetUpdateStartRequest.hpp"
#include "castor/stager/GetUpdateDone.hpp"
#include "castor/stager/GetUpdateFailed.hpp"
#include "castor/stager/PutFailed.hpp"
#include "castor/stager/PutStartRequest.hpp"
#include "castor/stager/PutDoneStart.hpp"
#include "castor/stager/Disk2DiskCopyDoneRequest.hpp"
#include "castor/stager/Disk2DiskCopyStartRequest.hpp"
#include "castor/stager/MoverCloseRequest.hpp"
#include "castor/stager/FirstByteWritten.hpp"
#include "castor/rh/BasicResponse.hpp"
#include "castor/rh/GetUpdateStartResponse.hpp"
#include "castor/rh/Disk2DiskCopyStartResponse.hpp"
#include "castor/replier/RequestReplier.hpp"
#include "castor/stager/dbService/StagerDlfMessages.hpp"
#include "castor/stager/dbService/JobSvcThread.hpp"

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::stager::dbService::JobSvcThread::JobSvcThread() throw () {}

//-----------------------------------------------------------------------------
// select
//-----------------------------------------------------------------------------
castor::IObject* castor::stager::dbService::JobSvcThread::select()
  throw() {
  try {
    // get the JobSvc. Note that we cannot cache it since we
    // would not be thread safe
    castor::Services *svcs = castor::BaseObject::services();
    castor::IService *svc = svcs->service("DbJobSvc", castor::SVC_DBJOBSVC);
    castor::stager::IJobSvc *jobSvc = dynamic_cast<castor::stager::IJobSvc*>(svc);
    if (0 == jobSvc) {
      // "Could not get JobSvc"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Function", "JobSvcThread::select")};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, STAGER_JOBSVC_GETSVC, 1, params);
      return 0;
    }
    // actual work
    castor::stager::Request* req = jobSvc->requestToDo();
    jobSvc->release();
    return req;
  } catch (castor::exception::Exception e) {
    // "Unexpected exception caught"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Function", "JobSvcThread::select"),
       castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param("Code", e.code())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, STAGER_JOBSVC_EXCEPT, 3, params);
    return 0;
  }
}

//-----------------------------------------------------------------------------
// handleStartRequest
//-----------------------------------------------------------------------------
void castor::stager::dbService::JobSvcThread::handleStartRequest
(castor::stager::Request* req,
 castor::IClient *client,
 castor::Services* svcs,
 castor::stager::IJobSvc* jobSvc,
 castor::BaseAddress &ad,
 Cuuid_t uuid) throw() {
  // Useful Variables
  castor::stager::FileSystem *fs = 0;
  castor::stager::SubRequest *subreq = 0;
  castor::stager::DiskCopy *dc = 0;
  std::list<castor::stager::DiskCopyForRecall*> sources;
  castor::stager::StartRequest *sReq;
  bool emptyFile;
  Cuuid_t suuid = nullCuuid;
  castor::rh::GetUpdateStartResponse res;
  bool failed = false;
  try {
    // Retrieving request from the database
    // Note that casting the request will never be
    // null since select returns one for sure
    sReq = dynamic_cast<castor::stager::StartRequest*> (req);
    // Loading the subrequest from db
    ad.setTarget(sReq->subreqId());
    castor::IObject *obj = svcs->createObj(&ad);
    if (0 == obj) {
      // "Could not find subrequest associated to Request"
      castor::dlf::Param params[] =
        {castor::dlf::Param("SubRequest DbId", sReq->subreqId()),
         castor::dlf::Param("Request Type", "StartRequest")};
      castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_JOBSVC_NOSREQ, 2, params);
      delete sReq;
      return;
    }
    subreq = dynamic_cast<castor::stager::SubRequest*>(obj);
    if (0 == subreq) {
      // "Expected SubRequest in Request but found another type"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Type", obj->type()),
         castor::dlf::Param("Request Type", "StartRequest")};
      castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_JOBSVC_BADSRT, 2, params);
      delete sReq;
      delete obj;
      return;
    }
    string2Cuuid(&suuid, (char*)subreq->subreqId().c_str());
    // Getting the FileSystem Object
    fs = jobSvc->selectFileSystem(sReq->fileSystem(),
                                  sReq->diskServer());
    if (0 == fs) {
      // "Could not find suitable filesystem",
      castor::dlf::Param params[] =
        {castor::dlf::Param("DiskServer", sReq->diskServer()),
         castor::dlf::Param("FileSystem", sReq->fileSystem()),
         castor::dlf::Param(suuid)};
      castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_JOBSVC_NOFSOK, 3, params);
      delete sReq;
      delete obj;
      return;
    }
    // Invoking the method
    if (castor::OBJ_GetUpdateStartRequest == sReq->type()) {
      // "Invoking getUpdateStart"
      castor::dlf::Param params[] = {
	castor::dlf::Param(suuid)
      };
      castor::dlf::dlf_writep(uuid, DLF_LVL_USAGE, STAGER_JOBSVC_GETUPDS, 1, params);
      dc = jobSvc->getUpdateStart(subreq, fs, sources, &emptyFile);
    } else {
      // "Invoking PutStart"
      castor::dlf::Param params[] = {
	castor::dlf::Param(suuid)
      };
      castor::dlf::dlf_writep(uuid, DLF_LVL_USAGE, STAGER_JOBSVC_PUTS, 1, params);
      dc = jobSvc->putStart(subreq, fs);
    }
    // Fill DiskCopy with SubRequest
    if (0 != dc) {
      svcs->fillObj(&ad, dc, castor::OBJ_SubRequest);
    }
  } catch (castor::exception::Exception e) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("Function", "JobSvcThread::handleStartRequest"),
       castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param("Code", e.code()),
       castor::dlf::Param(suuid)};
    castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_JOBSVC_EXCEPT, 4, params);
    res.setErrorCode(e.code());
    res.setErrorMessage(e.getMessage().str());
    failed = true;
  }
  // Build the response
  if (!failed) {
    res.setDiskCopy(dc);
    if (castor::OBJ_GetUpdateStartRequest == sReq->type()) {
      for (std::list<castor::stager::DiskCopyForRecall*>::iterator it =
             sources.begin();
           it != sources.end();
           it++) {
        res.addSources(*it);
      }
    }
    res.setEmptyFile(emptyFile);
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
      {castor::dlf::Param("Function", "JobSvcThread::handleStartRequest.reply"),
       castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param("Code", e.code()),
       castor::dlf::Param(suuid)};
    castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_JOBSVC_EXCEPT, 4, params);
  }
  // Cleanup
  if (0 != fs) {
    castor::stager::DiskServer *ds = fs->diskserver();
    if (0 != ds) { delete ds; }
    delete fs;
  }
  if (subreq) delete subreq;
  if (dc) {
    std::vector<castor::stager::SubRequest*> srs = dc->subRequests();
    for (std::vector<castor::stager::SubRequest*>::iterator it =
           srs.begin();
         it != srs.end();
         it++) {
      delete *it;
    }
    delete dc;
  }
  if (castor::OBJ_GetUpdateStartRequest == sReq->type()) {
    for (std::list<castor::stager::DiskCopyForRecall*>::iterator it =
           sources.begin();
         it != sources.end();
         it++) {
      delete *it;
    }
  }
}

//-----------------------------------------------------------------------------
// handleDisk2DiskCopyStartRequest
//-----------------------------------------------------------------------------
void castor::stager::dbService::JobSvcThread::handleDisk2DiskCopyStartRequest
(castor::stager::Request* req,
 castor::IClient *client,
 castor::Services* svcs,
 castor::stager::IJobSvc* jobSvc,
 castor::BaseAddress &ad,
 Cuuid_t uuid) throw() {
  // Useful Variables
  castor::stager::Disk2DiskCopyStartRequest *uReq;
  castor::rh::Disk2DiskCopyStartResponse res;
  castor::stager::DiskCopyInfo *dc = 0;
  castor::stager::DiskCopyInfo *srcDc = 0;
  bool failed = false;
  try {
    // Retrieving request from the database
    // Note that casting the request will never be
    // null since select returns one for sure
    uReq = dynamic_cast<castor::stager::Disk2DiskCopyStartRequest*> (req);
    // "Invoking disk2DiskCopyStart"
    castor::dlf::Param params[] = {
      castor::dlf::Param("DiskCopyId", uReq->diskCopyId()),
      castor::dlf::Param("SourceDcId", uReq->sourceDiskCopyId()),
      castor::dlf::Param("SvcClassName", uReq->destSvcClass()),
      castor::dlf::Param("DiskServer", uReq->diskServer()),
      castor::dlf::Param("FileSystem", uReq->mountPoint())};
    castor::dlf::dlf_writep(uuid, DLF_LVL_USAGE, STAGER_JOBSVC_D2DCS, 5, params);
    jobSvc->disk2DiskCopyStart(uReq->diskCopyId(),
			       uReq->sourceDiskCopyId(),
			       uReq->destSvcClass(),
			       uReq->diskServer(),
			       uReq->mountPoint(), dc, srcDc);
  } catch (castor::exception::Exception e) {
    // "Unexpected exception caught"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Function", "JobSvcThread::handleDisk2DiskCopyStartRequest"),
       castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param("Code", e.code())};
    castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_JOBSVC_EXCEPT, 3, params);
    res.setErrorCode(e.code());
    res.setErrorMessage(e.getMessage().str());
    failed = true;

    // Fail the disk2disk copy transfer automatically on behalf of the client.
    // This allows the client to just exit without having to manually call
    // disk2DiskCopyFailed
    try {
      // "Invoking disk2DiskCopyFailed"
      castor::dlf::Param params[] =
	{castor::dlf::Param("DiskCopyId", uReq->diskCopyId())};
      castor::dlf::dlf_writep(uuid, DLF_LVL_USAGE, STAGER_JOBSVC_D2DCBAD, 1, params);
      jobSvc->disk2DiskCopyFailed(uReq->diskCopyId());
    } catch (castor::exception::Exception e) {

      // "Unexpected exception caught"
      castor::dlf::Param params[] =
	{castor::dlf::Param("Function", "JobSvcThread::handleDisk2DiskCopyStartRequest"),
	 castor::dlf::Param("Message", e.getMessage().str()),
	 castor::dlf::Param("Code", e.code())};
      castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_JOBSVC_EXCEPT, 3, params);
    }
  }
  // Build the response
  if (!failed) {
    res.setDiskCopy(dc);
    res.setSourceDiskCopy(srcDc);
  }
  // Reply To client
  try {
    castor::replier::RequestReplier *rr =
      castor::replier::RequestReplier::getInstance();
    res.setReqAssociated(req->reqId());
    rr->sendResponse(client, &res, true);
  } catch (castor::exception::Exception e) {
    // "Unexpected exception caught"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Function", "JobSvcThread::handleDisk2DiskCopyStartRequest.reply"),
       castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param("Code", e.code())};
    castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_JOBSVC_EXCEPT, 3, params);
  }
  // Cleanup
  if (0 != dc)
    delete dc;
  if (0 != srcDc)
    delete srcDc;
}

//-----------------------------------------------------------------------------
// handleDisk2DiskCopyDoneRequest
//-----------------------------------------------------------------------------
void castor::stager::dbService::JobSvcThread::handleDisk2DiskCopyDoneRequest
(castor::stager::Request* req,
 castor::IClient *client,
 castor::Services* svcs,
 castor::stager::IJobSvc* jobSvc,
 castor::BaseAddress &ad,
 Cuuid_t uuid) throw() {
  // Useful Variables
  castor::stager::Disk2DiskCopyDoneRequest *uReq;
  castor::rh::BasicResponse res;
  try {
    // Retrieving request from the database
    // Note that casting the request will never be
    // null since select returns one for sure
    uReq = dynamic_cast<castor::stager::Disk2DiskCopyDoneRequest*> (req);
    // Invoking the method
    u_signed64 srcDcId = uReq->sourceDiskCopyId();
    if (0 == srcDcId) {
      castor::dlf::Param params[] =
        {castor::dlf::Param("DiskCopyId", uReq->diskCopyId())};
      castor::dlf::dlf_writep(uuid, DLF_LVL_USAGE, STAGER_JOBSVC_D2DCBAD, 1, params);
      jobSvc->disk2DiskCopyFailed(uReq->diskCopyId());
    } else {
      castor::dlf::Param params[] =
        {castor::dlf::Param("DiskCopyId", uReq->diskCopyId()),
         castor::dlf::Param("SourceDcId", srcDcId)};
      castor::dlf::dlf_writep(uuid, DLF_LVL_USAGE, STAGER_JOBSVC_D2DCOK, 2, params);
      jobSvc->disk2DiskCopyDone(uReq->diskCopyId(), srcDcId);
    }
  } catch (castor::exception::Exception e) {
    // "Unexpected exception caught"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Function", "JobSvcThread::handleDisk2DiskCopyDoneRequest"),
       castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param("Code", e.code())};
    castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_JOBSVC_EXCEPT, 3, params);
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
      {castor::dlf::Param("Function", "JobSvcThread::handleDisk2DiskCopyDoneRequest.reply"),
       castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param("Code", e.code())};
    castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_JOBSVC_EXCEPT, 3, params);
  }
}

//-----------------------------------------------------------------------------
// handleMoverCloseRequest
//-----------------------------------------------------------------------------
void castor::stager::dbService::JobSvcThread::handleMoverCloseRequest
(castor::stager::Request* req,
 castor::IClient *client,
 castor::Services* svcs,
 castor::stager::IJobSvc* jobSvc,
 castor::BaseAddress &ad,
 Cuuid_t uuid) throw()  {
  // Useful Variables
  castor::stager::SubRequest *subreq = 0;
  castor::stager::MoverCloseRequest *mcReq;
  castor::IObject *obj = 0;
  Cuuid_t suuid = nullCuuid;
  castor::rh::BasicResponse res;
  try {
    // Retrieving request from the database
    // Note that casting the request will never be
    // null since select returns one for sure
    mcReq = dynamic_cast<castor::stager::MoverCloseRequest*> (req);
    ad.setTarget(mcReq->subReqId());
    obj = svcs->createObj(&ad);
    if (0 == obj) {
      // "Could not find subrequest associated to Request"
      castor::dlf::Param params[] =
        {castor::dlf::Param("SubRequest DbId", mcReq->subReqId()),
         castor::dlf::Param("Request Type", "MoverCloseRequest")};
      castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_JOBSVC_NOSREQ, 2, params);
      delete mcReq;
      return;
    }
    subreq = dynamic_cast<castor::stager::SubRequest*>(obj);
    if (0 == subreq) {
      // "Expected SubRequest in Request but found another type"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Type", obj->type()),
         castor::dlf::Param("Request Type", "MoverCloseRequest")};
      castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_JOBSVC_BADSRT, 2, params);
      delete mcReq;
      delete obj;
      return;
    }
    string2Cuuid(&suuid, (char*)subreq->subreqId().c_str());
    // "Invoking prepareForMigration"
    castor::dlf::Param params[] = {castor::dlf::Param(suuid)};
    castor::dlf::dlf_writep(uuid, DLF_LVL_USAGE, STAGER_JOBSVC_PFMIG, 1, params);
    jobSvc->prepareForMigration(subreq, mcReq->fileSize(), mcReq->timeStamp());
    // Cleaning
    delete obj;
  } catch (castor::exception::Exception e) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("Function", "JobSvcThread::handleMoverCloseRequest"),
       castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param("Code", e.code()),
       castor::dlf::Param(suuid)};
    castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_JOBSVC_EXCEPT, 4, params);
    res.setErrorCode(e.code());
    res.setErrorMessage(e.getMessage().str());
    if (obj != 0) delete obj;
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
      {castor::dlf::Param("Function", "JobSvcThread::handleMoverCloseRequest.reply"),
       castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param("Code", e.code()),
       castor::dlf::Param(suuid)};
    castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_JOBSVC_EXCEPT, 4, params);
  }
}

//-----------------------------------------------------------------------------
// handleGetUpdateDoneRequest
//-----------------------------------------------------------------------------
void castor::stager::dbService::JobSvcThread::handleGetUpdateDoneRequest
(castor::stager::Request* req,
 castor::IClient *client,
 castor::Services* svcs,
 castor::stager::IJobSvc* jobSvc,
 castor::BaseAddress &ad,
 Cuuid_t uuid) throw() {
  // Useful Variables
  castor::stager::GetUpdateDone *uReq;
  castor::rh::BasicResponse res;
  try {
    // Retrieving request from the database
    // Note that casting the request will never be
    // null since select returns one for sure
    uReq = dynamic_cast<castor::stager::GetUpdateDone*> (req);
    // Invoking the method
    castor::dlf::Param params[] =
      {castor::dlf::Param("SubReqId", uReq->subReqId())};
    castor::dlf::dlf_writep(uuid, DLF_LVL_USAGE, STAGER_JOBSVC_GETUPDO, 1, params);
    jobSvc->getUpdateDone(uReq->subReqId());
  } catch (castor::exception::Exception e) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("Function", "JobSvcThread::handleGetUpdateDoneRequest"),
       castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param("Code", e.code())};
    castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_JOBSVC_EXCEPT, 3, params);
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
      {castor::dlf::Param("Function", "JobSvcThread::handleGetUpdateDoneRequest.reply"),
       castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param("Code", e.code())};
    castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_JOBSVC_EXCEPT, 3, params);
  }
}

//-----------------------------------------------------------------------------
// handleGetUpdateFailedRequest
//-----------------------------------------------------------------------------
void castor::stager::dbService::JobSvcThread::handleGetUpdateFailedRequest
(castor::stager::Request* req,
 castor::IClient *client,
 castor::Services* svcs,
 castor::stager::IJobSvc* jobSvc,
 castor::BaseAddress &ad,
 Cuuid_t uuid) throw() {
  // Useful Variables
  castor::stager::GetUpdateFailed *uReq;
  castor::rh::BasicResponse res;
  try {
    // Retrieving request from the database
    // Note that casting the request will never be
    // null since select returns one for sure
    uReq = dynamic_cast<castor::stager::GetUpdateFailed*> (req);
    // Invoking the method
    castor::dlf::Param params[] =
      {castor::dlf::Param("SubReqId", uReq->subReqId())};
    castor::dlf::dlf_writep(uuid, DLF_LVL_USAGE, STAGER_JOBSVC_GETUPFA, 1, params);
    jobSvc->getUpdateFailed(uReq->subReqId());
  } catch (castor::exception::Exception e) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("Function", "JobSvcThread::handleGetUpdateFailedRequest"),
       castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param("Code", e.code())};
    castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_JOBSVC_EXCEPT, 3, params);
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
      {castor::dlf::Param("Function", "JobSvcThread::handleGetUpdateFailedRequest.reply"),
       castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param("Code", e.code())};
    castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_JOBSVC_EXCEPT, 3, params);
  }
}

//-----------------------------------------------------------------------------
// handlePutFailedRequest
//-----------------------------------------------------------------------------
void castor::stager::dbService::JobSvcThread::handlePutFailedRequest
(castor::stager::Request* req,
 castor::IClient *client,
 castor::Services* svcs,
 castor::stager::IJobSvc* jobSvc,
 castor::BaseAddress &ad,
 Cuuid_t uuid) throw() {
  // Useful Variables
  castor::stager::PutFailed *uReq;
  castor::rh::BasicResponse res;
  try {
    // Retrieving request from the database
    // Note that casting the request will never be
    // null since select returns one for sure
    uReq = dynamic_cast<castor::stager::PutFailed*> (req);
    // "Invoking putFailed"
    castor::dlf::Param params[] =
      {castor::dlf::Param("SubReqId", uReq->subReqId())};
    castor::dlf::dlf_writep(uuid, DLF_LVL_USAGE, STAGER_JOBSVC_PUTFAIL, 1, params);
    jobSvc->putFailed(uReq->subReqId());
  } catch (castor::exception::Exception e) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("Function", "JobSvcThread::handlePutFailedRequest"),
       castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param("Code", e.code())};
    castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_JOBSVC_EXCEPT, 3, params);
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
      {castor::dlf::Param("Function", "JobSvcThread::handlePutFailedRequest.reply"),
       castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param("Code", e.code())};
    castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_JOBSVC_EXCEPT, 3, params);
  }
}

//-----------------------------------------------------------------------------
// handleFirstByteWrittenRequest
//-----------------------------------------------------------------------------
void castor::stager::dbService::JobSvcThread::handleFirstByteWrittenRequest
(castor::stager::Request* req,
 castor::IClient *client,
 castor::Services* svcs,
 castor::stager::IJobSvc* jobSvc,
 castor::BaseAddress &ad,
 Cuuid_t uuid) throw() {
  // Useful Variables
  castor::stager::FirstByteWritten *uReq;
  castor::rh::BasicResponse res;
  try {
    // Retrieving request from the database
    // Note that casting the request will never be
    // null since select returns one for sure
    uReq = dynamic_cast<castor::stager::FirstByteWritten*> (req);
    // "Invoking firstByteWritten"
    castor::dlf::Param params[] =
      {castor::dlf::Param("SubReqId", uReq->subReqId())};
    castor::dlf::dlf_writep(uuid, DLF_LVL_USAGE, STAGER_JOBSVC_1STBWR, 1, params);
    jobSvc->firstByteWritten(uReq->subReqId());
  } catch (castor::exception::Exception e) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("function", "JobSvcThread::handleFirstByteWrittenRequest"),
       castor::dlf::Param("message", e.getMessage().str()),
       castor::dlf::Param("code", e.code()),
       castor::dlf::Param("SubReqId", uReq->subReqId())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, STAGER_JOBSVC_EXCEPT, 4, params);
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
      {castor::dlf::Param("function", "JobSvcThread::handleFirstByteWrittenRequest.reply"),
       castor::dlf::Param("message", e.getMessage().str()),
       castor::dlf::Param("code", e.code())};
    castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_JOBSVC_EXCEPT, 3, params);
  }
}

//-----------------------------------------------------------------------------
// process
//-----------------------------------------------------------------------------
void castor::stager::dbService::JobSvcThread::process
(castor::IObject *param) throw() {
  // Useful variables
  castor::stager::Request* req = 0;
  castor::Services *svcs = 0;
  castor::stager::IJobSvc *jobSvc = 0;
  castor::IClient *client = 0;
  Cuuid_t uuid = nullCuuid;
  // address to access db
  castor::BaseAddress ad;
  ad.setCnvSvcName("DbCnvSvc");
  ad.setCnvSvcType(castor::SVC_DBCNV);
  try {
    // get the JobSvc. Note that we cannot cache it since we
    // would not be thread safe
    svcs = castor::BaseObject::services();
    castor::IService* svc = svcs->service("DbJobSvc", castor::SVC_DBJOBSVC);
    jobSvc = dynamic_cast<castor::stager::IJobSvc*>(svc);
    if (0 == jobSvc) {
      // "Could not get JobSvc"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Function", "JobSvcThread::process")};
      castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_JOBSVC_GETSVC, 1, params);
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
      castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_JOBSVC_NOCLI);
      delete req;
      jobSvc->release();
      return;
    }
  } catch (castor::exception::Exception e) {
    // If we fail here, we do NOT have enough information
    // to reply to the client ! So we only log something.
    // "Unexpected exception caught"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Function", "JobSvcThread::process.1"),
       castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param("Code", e.code())};
    castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_JOBSVC_EXCEPT, 3, params);
    if (req) delete req;
    if (jobSvc) jobSvc->release();
    return;
  }

  // At this point we are able to reply to the client
  switch (req->type()) {
  case castor::OBJ_GetUpdateStartRequest:
  case castor::OBJ_PutStartRequest:
    castor::stager::dbService::JobSvcThread::handleStartRequest
      (req, client, svcs, jobSvc, ad, uuid);
    break;
  case castor::OBJ_Disk2DiskCopyStartRequest:
    castor::stager::dbService::JobSvcThread::handleDisk2DiskCopyStartRequest
      (req, client, svcs, jobSvc, ad, uuid);
    break;
  case castor::OBJ_Disk2DiskCopyDoneRequest:
    castor::stager::dbService::JobSvcThread::handleDisk2DiskCopyDoneRequest
      (req, client, svcs, jobSvc, ad, uuid);
    break;
  case castor::OBJ_MoverCloseRequest:
    castor::stager::dbService::JobSvcThread::handleMoverCloseRequest
      (req, client, svcs, jobSvc, ad, uuid);
    break;
  case castor::OBJ_GetUpdateDone:
    castor::stager::dbService::JobSvcThread::handleGetUpdateDoneRequest
      (req, client, svcs, jobSvc, ad, uuid);
    break;
  case castor::OBJ_GetUpdateFailed:
    castor::stager::dbService::JobSvcThread::handleGetUpdateFailedRequest
      (req, client, svcs, jobSvc, ad, uuid);
    break;
  case castor::OBJ_PutFailed:
    castor::stager::dbService::JobSvcThread::handlePutFailedRequest
      (req, client, svcs, jobSvc, ad, uuid);
    break;
  case castor::OBJ_FirstByteWritten:
    castor::stager::dbService::JobSvcThread::handleFirstByteWrittenRequest
      (req, client, svcs, jobSvc, ad, uuid);
    break;
  default:
    // "Unknown Request type"
    castor::dlf::Param params[] =
      {castor::dlf::Param("type", req->type())};
    castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_JOBSVC_UNKREQ, 1, params);
    // Inform client
    castor::rh::BasicResponse res;
    res.setErrorCode(EINVAL);
    res.setErrorMessage("Unknown Request type");
    // Reply To Client
    try {
      castor::replier::RequestReplier *rr =
        castor::replier::RequestReplier::getInstance();
      res.setReqAssociated(req->reqId());
      rr->sendResponse(client, &res, true);
    } catch (castor::exception::Exception e) {
      // "Unexpected exception caught"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Function", "JobSvcThread::process.reply"),
         castor::dlf::Param("Message", e.getMessage().str()),
         castor::dlf::Param("Code", e.code())};
      castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_GCSVC_EXCEPT, 3, params);
    }
  }
  try {
    // Delete Request From the database
    svcs->deleteRep(&ad, req, true);
  } catch (castor::exception::Exception e) {
    // "Unexpected exception caught"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Function", "GcSvcThread::process.2"),
       castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param("Code", e.code())};
    castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_JOBSVC_EXCEPT, 3, params);
  }
  // Final cleanup
  if (req) delete req;
  if (jobSvc) jobSvc->release();
  return;
}
