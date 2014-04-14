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
#include "castor/exception/Internal.hpp"
#include "castor/replier/RequestReplier.hpp"
#include "castor/stager/daemon/RequestHelper.hpp"
#include <sys/time.h>

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::stager::daemon::JobRequestSvcThread::JobRequestSvcThread()
  throw (castor::exception::Exception) :
  SelectProcessThread(), m_jobSubRequestToDoStatement(0),
  m_handleGetOrPutStatement(0), m_archiveSubReqStatement(0) {
  // get the DbCnvSvc for handling ORACLE statements
  castor::IService *svc = castor::BaseObject::services()->service("DbCnvSvc", castor::SVC_DBCNV);
  m_dbSvc = dynamic_cast<castor::db::ora::OraCnvSvc*>(svc);
}

//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::stager::daemon::JobRequestSvcThread::~JobRequestSvcThread() throw () {
 try {
   if (m_jobSubRequestToDoStatement) m_dbSvc->terminateStatement(m_jobSubRequestToDoStatement);
 } catch (castor::exception::Exception &ignored) {};
 try {
   if (m_handleGetOrPutStatement) m_dbSvc->terminateStatement(m_handleGetOrPutStatement);
 } catch (castor::exception::Exception &ignored) {};
 try {
   if (m_archiveSubReqStatement) m_dbSvc->terminateStatement(m_archiveSubReqStatement);
 } catch (castor::exception::Exception &ignored) {};
}

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
  try {
    // Check whether the statements are ok
    if (0 == m_jobSubRequestToDoStatement) {
      m_jobSubRequestToDoStatement = m_dbSvc->createOraStatement
        ("BEGIN jobSubRequestToDo(:1, :2, :3, :4, :5, :6, :7, :8, :9, :10, :11, :12, :13); END;");
      m_jobSubRequestToDoStatement->registerOutParam(1, oracle::occi::OCCIDOUBLE);
      m_jobSubRequestToDoStatement->registerOutParam(2, oracle::occi::OCCISTRING, 2048);
      m_jobSubRequestToDoStatement->registerOutParam(3, oracle::occi::OCCIINT);
      m_jobSubRequestToDoStatement->registerOutParam(4, oracle::occi::OCCIINT);
      m_jobSubRequestToDoStatement->registerOutParam(5, oracle::occi::OCCIINT);
      m_jobSubRequestToDoStatement->registerOutParam(6, oracle::occi::OCCISTRING, 2048);
      m_jobSubRequestToDoStatement->registerOutParam(7, oracle::occi::OCCISTRING, 2048);
      m_jobSubRequestToDoStatement->registerOutParam(8, oracle::occi::OCCIDOUBLE);
      m_jobSubRequestToDoStatement->registerOutParam(9, oracle::occi::OCCIINT);
      m_jobSubRequestToDoStatement->registerOutParam(10, oracle::occi::OCCIINT);
      m_jobSubRequestToDoStatement->registerOutParam(11, oracle::occi::OCCIINT);
      m_jobSubRequestToDoStatement->registerOutParam(12, oracle::occi::OCCIINT);
      m_jobSubRequestToDoStatement->registerOutParam(13, oracle::occi::OCCIINT);
      m_jobSubRequestToDoStatement->setAutoCommit(true);
      // also register for associated alert
      oracle::occi::Statement* registerAlertStmt = 
        m_dbSvc->createOraStatement("BEGIN DBMS_ALERT.REGISTER('wakeUpJobReqSvc'); END;");
      registerAlertStmt->execute();
      m_dbSvc->terminateStatement(registerAlertStmt);
    }
    // execute the statement and see whether we found something
    unsigned int rc = m_jobSubRequestToDoStatement->executeUpdate();
    if (0 == rc) {
      castor::exception::Internal e;
      e.getMessage() << "unable to get next SubRequest to process";
      throw e;
    }
    u_signed64 srId = (u_signed64)m_jobSubRequestToDoStatement->getDouble(1);
    if (0 == srId) {
      // Found no SubRequest to handle
      return 0;
    }
    // Create result
    JobRequest* result = new JobRequest();
    result->requestUuid = nullCuuid;
    std::string strUuid = m_jobSubRequestToDoStatement->getString(2);
    if (strUuid.length() > 0) {
      string2Cuuid(&result->requestUuid, (char*)strUuid.c_str());
    }
    result->srId = srId;
    result->reqType = m_jobSubRequestToDoStatement->getInt(3);
    result->euid = m_jobSubRequestToDoStatement->getInt(4);
    result->egid = m_jobSubRequestToDoStatement->getInt(5);
    result->fileName = m_jobSubRequestToDoStatement->getString(6);
    result->svcClassName = m_jobSubRequestToDoStatement->getString(7);
    result->fileClass = (u_signed64)m_jobSubRequestToDoStatement->getDouble(8);
    result->flags = m_jobSubRequestToDoStatement->getInt(9);
    result->modebits = m_jobSubRequestToDoStatement->getInt(10);
    result->clientIpAddress = m_jobSubRequestToDoStatement->getInt(11);
    result->clientPort = m_jobSubRequestToDoStatement->getInt(12);
    result->clientVersion = m_jobSubRequestToDoStatement->getInt(13);
    // return
    return result;
  } catch (oracle::occi::SQLException &e) {
    m_dbSvc->handleException(e);
    castor::exception::Internal ex;
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
    bool replyNeeded = handleGetOrPut(sr, cnsFileid, fileSize, stagerOpentimeInUsec);
    // reply to client when needed
    if (replyNeeded) {
      answerClient(sr, cnsFileid, SUBREQUEST_READY, 0, "");
    }
  } catch(castor::exception::Exception &ex) {
    try {
      // "Exception caught while handling request"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Function", "JobRequestSvcThread::process"),
         castor::dlf::Param("ErrorCode", ex.code()),
         castor::dlf::Param("ErrorMessage", ex.getMessageValue()),
         castor::dlf::Param("BackTrace", ex.backtrace())};
      castor::dlf::dlf_writep(sr->requestUuid, DLF_LVL_ERROR, STAGER_JOBSVC_EXCEPT2, 4, params, &cnsFileid);
      // fail subrequet in the DB
      archiveSubReq(sr->srId, SUBREQUEST_FAILED_FINISHED);
      // inform the client about the error
      answerClient(sr, cnsFileid, SUBREQUEST_FAILED, ex.code(), ex.getMessage().str());
    } catch (castor::exception::Exception &ex2) {
      // "Unexpected exception caught"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Function", "JobRequestSvcThread::process"),
         castor::dlf::Param("ErrorCode", ex2.code()),
         castor::dlf::Param("ErrorMessage", ex2.getMessageValue()),
         castor::dlf::Param("BackTrace", ex2.backtrace())};
      castor::dlf::dlf_writep(sr->requestUuid, DLF_LVL_ERROR, STAGER_JOBSVC_EXCEPT, 4, params, &cnsFileid);
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
bool castor::stager::daemon::JobRequestSvcThread::handleGetOrPut(const JobRequest *sr,
                                                                 const struct Cns_fileid &cnsFileid,
                                                                 u_signed64 fileSize,
                                                                 u_signed64 stagerOpentimeInUsec)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statement was created
    if (0 == m_handleGetOrPutStatement) {
      m_handleGetOrPutStatement = m_dbSvc->createOraStatement
        ("BEGIN :1 := handleGetOrPut(:2, :3, :4, :5, :6, :7, :8, :9); END;");
      m_handleGetOrPutStatement->registerOutParam(1, oracle::occi::OCCIINT);
      m_handleGetOrPutStatement->setAutoCommit(true);
    }
    // Execute statement
    m_handleGetOrPutStatement->setInt(2, sr->reqType);
    m_handleGetOrPutStatement->setDouble(3, sr->srId);
    m_handleGetOrPutStatement->setDouble(4, cnsFileid.fileid);
    m_handleGetOrPutStatement->setString(5, cnsFileid.server);
    m_handleGetOrPutStatement->setDouble(6, sr->fileClass);
    m_handleGetOrPutStatement->setString(7, sr->fileName.c_str());
    m_handleGetOrPutStatement->setDouble(8, fileSize);
    m_handleGetOrPutStatement->setDouble(9, stagerOpentimeInUsec);
    m_handleGetOrPutStatement->executeUpdate();
    return m_handleGetOrPutStatement->getInt(1) != 0;
  } catch (oracle::occi::SQLException &e) {
    m_dbSvc->handleException(e);
    castor::exception::Internal ex;
    ex.getMessage() << "Error caught in handleGetOrPut : " << e.what();
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// archiveSubReq
//-----------------------------------------------------------------------------
void castor::stager::daemon::JobRequestSvcThread::archiveSubReq(const u_signed64 srId, const int status)
  throw (castor::exception::Exception) {
  try {
    // Check whether the statement was created
    if (0 == m_archiveSubReqStatement) {
      m_archiveSubReqStatement = m_dbSvc->createOraStatement("BEGIN archiveSubReq(:1, :2); END;");
      m_archiveSubReqStatement->setAutoCommit(true);
    }
    // Execute statement
    m_archiveSubReqStatement->setDouble(1, srId);
    m_archiveSubReqStatement->setInt(2, status);
    m_archiveSubReqStatement->executeUpdate();
  } catch (oracle::occi::SQLException &e) {
    m_dbSvc->handleException(e);
    castor::exception::Internal ex;
    ex.getMessage() << "Unable to archive subRequest :" << e.what();
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// answerClient
//-----------------------------------------------------------------------------
void castor::stager::daemon::JobRequestSvcThread::answerClient(const JobRequest *sr,
                                                               const struct Cns_fileid &cnsFileid,
                                                               const int status,
                                                               const int errorCode,
                                                               const std::string &errorMsg)
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
  ioResponse.setFileId((u_signed64) cnsFileid.fileid);
  ioResponse.setStatus(status);
  ioResponse.setErrorCode(errorCode);
  ioResponse.setErrorMessage(errorMsg);
  // effectively send the response */
  castor::replier::RequestReplier::getInstance()->sendResponse(&cl, &ioResponse);
}
