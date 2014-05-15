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
* Service thread for handling Job oriented requests, i.e. Gets and Puts
*
* @author castor dev team
*****************************************************************************/

#include "castor/stager/daemon/JobRequestSvcThread.hpp"
#include "Cns_api.h"
#include "castor/IService.hpp"
#include "castor/Services.hpp"
#include "castor/rh/Client.hpp"
#include "castor/rh/IOResponse.hpp"
#include "castor/db/DbCnvSvc.hpp"
#include "castor/dlf/Dlf.hpp"
#include "castor/dlf/Param.hpp"
#include "castor/replier/RequestReplier.hpp"
#include "castor/stager/daemon/RequestHelper.hpp"
#include <sys/time.h>

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::stager::daemon::JobRequestSvcThread::JobRequestSvcThread()
  throw (castor::exception::Exception) : SelectProcessThread() {}

//-----------------------------------------------------------------------------
// select
//-----------------------------------------------------------------------------
castor::IObject* castor::stager::daemon::JobRequestSvcThread::select() throw() {
  try {
    // get a subRequest to handle
    return jobSubRequestToDo();
  } catch (castor::exception::Exception &e) {
    // "Unexpected exception caught"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Function", "JobRequestSvcThread::select"),
       castor::dlf::Param("ErrorCode", e.code()),
       castor::dlf::Param("Message", e.getMessageValue()),
       castor::dlf::Param("BackTrace", e.backtrace())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, STAGER_JOBSVC_EXCEPT, 4, params);
    return 0;
  }
}

//-----------------------------------------------------------------------------
// jobSubRequestToDo
//-----------------------------------------------------------------------------
castor::stager::daemon::JobRequest*
castor::stager::daemon::JobRequestSvcThread::jobSubRequestToDo()
  throw(castor::exception::Exception) {
  // get the DbCnvSvc for handling ORACLE statements
  castor::IService *svc = castor::BaseObject::services()->service("DbCnvSvc", castor::SVC_DBCNV);
  castor::db::ora::OraCnvSvc *dbSvc = dynamic_cast<castor::db::ora::OraCnvSvc*>(svc);
  try {
    // retrieve or create statement
    bool wasCreated = false;
    oracle::occi::Statement* jobSubRequestToDoStatement = dbSvc->createOrReuseOraStatement
      ("BEGIN jobSubRequestToDo(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13, :14, :15); END;",
       &wasCreated);
    if (wasCreated) {
      jobSubRequestToDoStatement->registerOutParam(1, oracle::occi::OCCIDOUBLE);
      jobSubRequestToDoStatement->registerOutParam(2, oracle::occi::OCCISTRING, 2048);
      jobSubRequestToDoStatement->registerOutParam(3, oracle::occi::OCCIINT);
      jobSubRequestToDoStatement->registerOutParam(4, oracle::occi::OCCIINT);
      jobSubRequestToDoStatement->registerOutParam(5, oracle::occi::OCCIINT);
      jobSubRequestToDoStatement->registerOutParam(6, oracle::occi::OCCISTRING, 2048);
      jobSubRequestToDoStatement->registerOutParam(7, oracle::occi::OCCISTRING, 2048);
      jobSubRequestToDoStatement->registerOutParam(8, oracle::occi::OCCIDOUBLE);
      jobSubRequestToDoStatement->registerOutParam(9, oracle::occi::OCCIINT);
      jobSubRequestToDoStatement->registerOutParam(10, oracle::occi::OCCIINT);
      jobSubRequestToDoStatement->registerOutParam(11, oracle::occi::OCCIINT);
      jobSubRequestToDoStatement->registerOutParam(12, oracle::occi::OCCIINT);
      jobSubRequestToDoStatement->registerOutParam(13, oracle::occi::OCCIINT);
      jobSubRequestToDoStatement->registerOutParam(14, oracle::occi::OCCIINT);
      jobSubRequestToDoStatement->registerOutParam(15, oracle::occi::OCCISTRING, 2048);
      // also register for associated alert
      oracle::occi::Statement* registerAlertStmt = 
        dbSvc->createOraStatement("BEGIN DBMS_ALERT.REGISTER('wakeUpJobReqSvc'); END;");
      registerAlertStmt->execute();
      dbSvc->terminateStatement(registerAlertStmt);
    }
    // execute the statement and see whether we found something
    unsigned int rc = jobSubRequestToDoStatement->executeUpdate();
    if (0 == rc) {
      castor::exception::Exception e;
      e.getMessage() << "unable to get next SubRequest to process";
      throw e;
    }
    u_signed64 srId = (u_signed64)jobSubRequestToDoStatement->getDouble(1);
    if (0 == srId) {
      // Found no SubRequest to handle
      return 0;
    }
    // Create result
    JobRequest* result = new JobRequest();
    result->requestUuid = nullCuuid;
    std::string strUuid = jobSubRequestToDoStatement->getString(2);
    if (strUuid.length() > 0) {
      string2Cuuid(&result->requestUuid, (char*)strUuid.c_str());
    }
    result->srId = srId;
    result->reqType = jobSubRequestToDoStatement->getInt(3);
    result->euid = jobSubRequestToDoStatement->getInt(4);
    result->egid = jobSubRequestToDoStatement->getInt(5);
    result->fileName = jobSubRequestToDoStatement->getString(6);
    result->svcClassName = jobSubRequestToDoStatement->getString(7);
    result->fileClass = (u_signed64)jobSubRequestToDoStatement->getDouble(8);
    result->flags = jobSubRequestToDoStatement->getInt(9);
    result->modebits = jobSubRequestToDoStatement->getInt(10);
    result->clientIpAddress = jobSubRequestToDoStatement->getInt(11);
    result->clientPort = jobSubRequestToDoStatement->getInt(12);
    result->clientVersion = jobSubRequestToDoStatement->getInt(13);
    // did we get an error ?
    int errorCode = jobSubRequestToDoStatement->getInt(14);
    if (errorCode > 0) {
      // answer client
      bool isLastAnswer = updateAndCheckSubRequest(result->srId, SUBREQUEST_FAILED_FINISHED);
      answerClient(result, 0, SUBREQUEST_FAILED, errorCode,
                   jobSubRequestToDoStatement->getString(15), isLastAnswer);
      return 0;
    }
    // return
    return result;
  } catch (oracle::occi::SQLException &e) {
    dbSvc->handleException(e);
    castor::exception::Exception ex;
    ex.getMessage() << e.getMessage();
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// process
//-----------------------------------------------------------------------------
void castor::stager::daemon::JobRequestSvcThread::process(castor::IObject* requestToProcess) throw () {
  struct Cns_fileid cnsFileid;
  memset(&cnsFileid, 0, sizeof(cnsFileid));
  // for monitoring purposes
  timeval tvStart;
  gettimeofday(&tvStart, NULL);
  // get next request
  JobRequest* sr = dynamic_cast<JobRequest*>(requestToProcess);
  try {
    // check permissions and open the file according to the request type file
    u_signed64 fileSize;
    u_signed64 stagerOpentimeInUsec;
    RequestHelper::openNameServerFile
      (sr->requestUuid, sr->euid, sr->egid, sr->reqType, sr->fileName,
       sr->fileClass, sr->modebits, sr->flags,
       cnsFileid, sr->fileClass, fileSize, stagerOpentimeInUsec);
    // call handleGetOrPut PL/SQL method in the stager DB
    int replyNeeded = handleGetOrPut(sr, cnsFileid, fileSize, stagerOpentimeInUsec);
    // reply to client when needed
    if (replyNeeded) {
      answerClient(sr, cnsFileid.fileid, SUBREQUEST_READY, 0, "", replyNeeded > 1);
    }
  } catch(castor::exception::Exception &ex) {
    try {
      // fail subrequest in the DB
      bool isLastAnswer = updateAndCheckSubRequest(sr->srId, SUBREQUEST_FAILED_FINISHED);
      // inform the client about the error
      answerClient(sr, cnsFileid.fileid, SUBREQUEST_FAILED, ex.code(), ex.getMessage().str(), isLastAnswer);
    } catch (castor::exception::Exception &ex2) {
      // "Unexpected exception caught"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Function", "JobRequestSvcThread::process"),
         castor::dlf::Param("ErrorCode", ex2.code()),
         castor::dlf::Param("ErrorMessage", ex2.getMessageValue()),
         castor::dlf::Param("BackTrace", ex2.backtrace())};
      castor::dlf::dlf_writep(sr->requestUuid, DLF_LVL_ERROR, STAGER_JOBSVC_EXCEPT,
                              4, params, &cnsFileid);
    }
  }
  // Calculate statistics
  timeval tvEnd;
  gettimeofday(&tvEnd, NULL);
  signed64 procTime = ((tvEnd.tv_sec * 1000000) + tvEnd.tv_usec) - ((tvStart.tv_sec * 1000000) + tvStart.tv_usec);
  // "Request processed"
  castor::dlf::Param params[] = {
    castor::dlf::Param(sr->requestUuid),
    castor::dlf::Param("Type", castor::ObjectsIdStrings[sr->reqType]),
    castor::dlf::Param("Filename", sr->fileName),
    castor::dlf::Param("uid", sr->euid),
    castor::dlf::Param("gid", sr->egid),
    castor::dlf::Param("SvcClass", sr->svcClassName),
    castor::dlf::Param("ProcessingTime", procTime * 0.000001)
  };
  castor::dlf::dlf_writep(sr->requestUuid, DLF_LVL_SYSTEM, STAGER_REQ_PROCESSED, 7, params, &cnsFileid);
  // cleanup memory
  delete sr;
}

//-----------------------------------------------------------------------------
// handleGetOrPut
//-----------------------------------------------------------------------------
int castor::stager::daemon::JobRequestSvcThread::handleGetOrPut(const JobRequest *sr,
                                                                struct Cns_fileid &cnsFileid,
                                                                u_signed64 fileSize,
                                                                u_signed64 stagerOpentimeInUsec)
  throw (castor::exception::Exception) {
  // get the DbCnvSvc for handling ORACLE statements
  castor::IService *svc = castor::BaseObject::services()->service("DbCnvSvc", castor::SVC_DBCNV);
  castor::db::ora::OraCnvSvc *dbSvc = dynamic_cast<castor::db::ora::OraCnvSvc*>(svc);
  try {
    // retrieve or create statement
    bool wasCreated = false;
    oracle::occi::Statement* handleGetOrPutStatement = dbSvc->createOrReuseOraStatement
      ("BEGIN :1 := handleGetOrPut(:2, :3, :4, :5, :6, :7, :8, :9); END;", &wasCreated);
    if (wasCreated) {
      handleGetOrPutStatement->registerOutParam(1, oracle::occi::OCCIINT);
    }
    // Execute statement
    handleGetOrPutStatement->setInt(2, sr->reqType);
    handleGetOrPutStatement->setDouble(3, sr->srId);
    handleGetOrPutStatement->setDouble(4, cnsFileid.fileid);
    handleGetOrPutStatement->setString(5, cnsFileid.server);
    handleGetOrPutStatement->setDouble(6, sr->fileClass);
    handleGetOrPutStatement->setString(7, sr->fileName.c_str());
    handleGetOrPutStatement->setDouble(8, fileSize);
    handleGetOrPutStatement->setDouble(9, stagerOpentimeInUsec);
    handleGetOrPutStatement->executeUpdate();
    return handleGetOrPutStatement->getInt(1);
  } catch (oracle::occi::SQLException &e) {
    dbSvc->handleException(e);
    castor::exception::Exception ex;
    ex.getMessage() << "Error caught in handleGetOrPut : " << e.what();
    // log "Exception caught while handling request"
    castor::dlf::Param params[] = {
      castor::dlf::Param(sr->requestUuid),
      castor::dlf::Param("Type", castor::ObjectsIdStrings[sr->reqType]),
      castor::dlf::Param("Filename", sr->fileName),
      castor::dlf::Param("uid", sr->euid),
      castor::dlf::Param("gid", sr->egid),
      castor::dlf::Param("SvcClass", sr->svcClassName),
      castor::dlf::Param("Error", e.what())
    };
    castor::dlf::dlf_writep(sr->requestUuid, DLF_LVL_ERROR, STAGER_JOBSVC_EXCEPT2,
                            7, params, &cnsFileid);
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// updateAndCheckSubRequest
//-----------------------------------------------------------------------------
bool castor::stager::daemon::JobRequestSvcThread::updateAndCheckSubRequest
(const u_signed64 srId, const int status)
  throw (castor::exception::Exception) {
  // get the DbCnvSvc for handling ORACLE statements
  castor::IService *svc = castor::BaseObject::services()->service("DbCnvSvc", castor::SVC_DBCNV);
  castor::db::ora::OraCnvSvc *dbSvc = dynamic_cast<castor::db::ora::OraCnvSvc*>(svc);
  try {
    // retrieve or create statement
    bool wasCreated = false;
    oracle::occi::Statement* updateAndCheckSubReqStatement = dbSvc->createOrReuseOraStatement
      ("BEGIN updateAndCheckSubRequest(:1, :2, :3); END;", &wasCreated);
    if (wasCreated) {
      updateAndCheckSubReqStatement->registerOutParam(3, oracle::occi::OCCIINT);
    }
    // Execute statement
    updateAndCheckSubReqStatement->setDouble(1, srId);
    updateAndCheckSubReqStatement->setInt(2, status);
    updateAndCheckSubReqStatement->executeUpdate();
    return updateAndCheckSubReqStatement->getInt(3) == 0;
  } catch (oracle::occi::SQLException &e) {
    dbSvc->handleException(e);
    castor::exception::Exception ex;
    ex.getMessage() << "Unable to archive subRequest :" << e.what();
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// answerClient
//-----------------------------------------------------------------------------
void castor::stager::daemon::JobRequestSvcThread::answerClient(const JobRequest *sr,
                                                               u_signed64 cnsFileid,
                                                               int status,
                                                               int errorCode,
                                                               const std::string &errorMsg,
                                                               bool isLastAnswer)
  throw (castor::exception::Exception) {
  /* create client object */
  castor::rh::Client cl;
  cl.setIpAddress(sr->clientIpAddress);
  cl.setPort(sr->clientPort);
  cl.setVersion(sr->clientVersion);
  /* create IOResponse object */
  castor::rh::IOResponse ioResponse;
  ioResponse.setId(sr->srId);
  char uuid[CUUID_STRING_LEN+1];
  uuid[CUUID_STRING_LEN] = 0;
  Cuuid2string(uuid, CUUID_STRING_LEN+1, &sr->requestUuid);
  ioResponse.setReqAssociated(uuid);
  ioResponse.setCastorFileName(sr->fileName);
  ioResponse.setFileId(cnsFileid);
  ioResponse.setStatus(status);
  ioResponse.setErrorCode(errorCode);
  ioResponse.setErrorMessage(errorMsg);
  // effectively send the response */
  castor::replier::RequestReplier::getInstance()->sendResponse(&cl, &ioResponse, isLastAnswer);
}
