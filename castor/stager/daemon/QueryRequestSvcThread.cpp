/******************************************************************************
 *                castor/stager/daemon/QueryRequestSvcThread.cpp
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
 * @(#)$RCSfile: QueryRequestSvcThread.cpp,v $ $Revision: 1.56 $ $Release$ $Date: 2007/10/23 14:01:36 $ $Author: sponcec3 $
 *
 * Service thread for StageQueryRequest requests
 *
 * @author castor dev team
 *****************************************************************************/

#include "Cthread_api.h"
#include "Cmutex.h"

#include <list>
#include <vector>
#include <sys/stat.h>

#include "castor/Services.hpp"
#include "castor/Constants.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/IObject.hpp"
#include "castor/IClient.hpp"
#include "castor/IService.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/NoEntry.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/BaseObject.hpp"
#include "castor/replier/RequestReplier.hpp"
#include "castor/query/VersionQuery.hpp"
#include "castor/query/VersionResponse.hpp"
#include "castor/stager/StageFileQueryRequest.hpp"
#include "castor/stager/StageFindRequestRequest.hpp"
#include "castor/stager/StageRequestQueryRequest.hpp"
#include "castor/query/DiskPoolQuery.hpp"
#include "castor/query/DiskPoolQueryResponse.hpp"
#include "castor/query/DiskServerDescription.hpp"
#include "castor/stager/RequestQueryType.hpp"
#include "castor/stager/QueryParameter.hpp"
#include "castor/stager/DiskCopyInfo.hpp"
#include "castor/stager/DiskCopyStatusCodes.hpp"
#include "castor/stager/SegmentStatusCodes.hpp"
#include "castor/stager/TapeCopyStatusCodes.hpp"
#include "castor/stager/SvcClass.hpp"
#include "castor/rh/BasicResponse.hpp"
#include "castor/rh/FileQryResponse.hpp"
#include "castor/query/IQuerySvc.hpp"
#include "castor/stager/dbService/QueryRequestSvcThread.hpp"
#include "castor/stager/dbService/StagerDlfMessages.hpp"
#include "stager_client_api.h"
#include "Cns_api.h"
#include "u64subr.h"

// All this to get the default version. The defines
// are here to be able to compile in case the patchlevel
// file was not properly filled with proper numbers
// since this is done only by maketar
#include "patchlevel.h"
#define __MAJORVERSION__ -1
#define __MINORVERSION__ -1
#define __MAJORRELEASE__ -1
#define __MINORRELEASE__ -1

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::stager::dbService::QueryRequestSvcThread::QueryRequestSvcThread()
  throw () {}

//-----------------------------------------------------------------------------
// select
//-----------------------------------------------------------------------------
castor::IObject* castor::stager::dbService::QueryRequestSvcThread::select()
  throw() {
  try {
    // get the QuerySvc. Note that we cannot cache it since we
    // would not be thread safe
    castor::Services *svcs = castor::BaseObject::services();
    castor::IService *svc = svcs->service("DbQuerySvc", castor::SVC_DBQUERYSVC);
    castor::query::IQuerySvc *qrySvc = dynamic_cast<castor::query::IQuerySvc*>(svc);
    if (0 == qrySvc) {
      // "Could not get QuerySvc"
      castor::dlf::Param params[] =
        {castor::dlf::Param("function", "QueryRequestSvcThread::select")};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, STAGER_QRYSVC_GETSVC, 1, params);
      return 0;
    }
    // actual work
    castor::IObject* obj = qrySvc->requestToDo();
    qrySvc->release();
    return obj;
  } catch (castor::exception::Exception e) {
    // "Unexpected exception caught"
    castor::dlf::Param params[] =
      {castor::dlf::Param("function", "QueryRequestSvcThread::select"),
       castor::dlf::Param("message", e.getMessage().str()),
       castor::dlf::Param("code", e.code())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, STAGER_QRYSVC_EXCEPT, 1, params);
    return 0;
  }
}

//-----------------------------------------------------------------------------
// setFileResponseStatus
//-----------------------------------------------------------------------------
void castor::stager::dbService::QueryRequestSvcThread::setFileResponseStatus
(castor::rh::FileQryResponse* fr,
 castor::stager::DiskCopyInfo* dc,
 bool& foundDiskCopy) throw() {

  // 1. Mapping diskcopy status to file status
  stage_fileStatus st = FILE_INVALID_STATUS;
  std::string diskServer = "";

  switch(dc->diskCopyStatus()) {
  case DISKCOPY_GCCANDIDATE:
  case DISKCOPY_BEINGDELETED:
  case DISKCOPY_DELETED:
    // just IGNORE the discopies in those statuses
    return;

  case DISKCOPY_INVALID:
  case DISKCOPY_FAILED:
    // don't ignore here, but return INVALID
    break;

  case DISKCOPY_WAITDISK2DISKCOPY:
  case DISKCOPY_WAITTAPERECALL:
    st = FILE_STAGEIN;
    break;

  case  DISKCOPY_STAGED:
    st = FILE_STAGED;
    diskServer = dc->diskServer();
    break;

  case DISKCOPY_WAITFS:
  case DISKCOPY_WAITFS_SCHEDULING:
  case DISKCOPY_STAGEOUT:
    st = FILE_STAGEOUT;
    diskServer = dc->diskServer();
    break;

  case DISKCOPY_CANBEMIGR:
    st = FILE_CANBEMIGR;
    diskServer = dc->diskServer();
    break;
  }

  // 2. Aggregate status for the various diskcopies
  if (!foundDiskCopy) {
    fr->setStatus(st);
    fr->setDiskServer(diskServer);
    fr->setNbAccesses(dc->nbAccesses());
    foundDiskCopy = true;
  } else {
    // If there are several diskcopies for the file,
    // we apply some precedence rules :
    // STAGED, CANBEMIGR > STAGEOUT, STAGEIN > INVALID
    if (((FILE_STAGEOUT == st || FILE_STAGEIN == st) &&
         (FILE_INVALID_STATUS == fr->status())) ||
        ((FILE_STAGED == st || FILE_CANBEMIGR == st))) {
      fr->setStatus(st);
      fr->setDiskServer(diskServer);
      fr->setNbAccesses(dc->nbAccesses());
    }
  }

  // 3. Set other common attributes from the castorFile
  fr->setCastorFileName(dc->lastKnownFileName());
  fr->setSize(dc->size());
}

//-----------------------------------------------------------------------------
// handleFileQueryRequestByFileName
//-----------------------------------------------------------------------------
void
castor::stager::dbService::QueryRequestSvcThread::handleFileQueryRequestByFileName
(castor::query::IQuerySvc* qrySvc,
 castor::IClient *client,
 std::string& fileName,
 u_signed64 svcClassId,
 std::string reqId,
 Cuuid_t uuid)
  throw (castor::exception::Exception) {
  // Processing File Query by fileName
  castor::dlf::Param params[] =
    {castor::dlf::Param("fileName", fileName)};
  castor::dlf::dlf_writep(uuid, DLF_LVL_SYSTEM, STAGER_QRYSVC_FQUERY, 1, params);
  // List diskCopies
  std::list<castor::stager::DiskCopyInfo*>* result =
    qrySvc->diskCopies4FileName(fileName, svcClassId);
  if(result == 0 || result->size() == 0) {   // sanity check, result always is != 0
    castor::exception::Exception e(ENOENT);
    if(svcClassId > 0)
      e.getMessage() << "File " << fileName << " not in given svcClass";
    else
      e.getMessage() << "File " << fileName << " not in stager";
    if(result != 0)
      delete result;
    throw e;
  }
  // Iterates over the list of disk copies, and returns one result
  // per fileid to the client. It needs the SQL statement to return the diskcopies
  // sorted by fileid/nshost.
  u_signed64 fileid = 0;
  std::string nshost = "";
  castor::rh::FileQryResponse res;
  res.setReqAssociated(reqId);
  bool foundDiskCopy = false;

  for(std::list<castor::stager::DiskCopyInfo*>::iterator dcit
        = result->begin();
      dcit != result->end();
      ++dcit) {
    castor::stager::DiskCopyInfo* diskcopy = *dcit;
    if (diskcopy->fileId() != fileid
        || diskcopy->nsHost() != nshost) {
      // Sending the response for the previous
      // castor file being processed
      if (foundDiskCopy) {
        castor::replier::RequestReplier::getInstance()->
          sendResponse(client, &res);
      }
      // Now processing a new file !
      foundDiskCopy = false;
      fileid = diskcopy->fileId();
      nshost = diskcopy->nsHost();
      std::ostringstream sst;
      sst << diskcopy->fileId() << "@" <<  diskcopy->nsHost();
      res.setFileName(sst.str());
      res.setFileId(fileid);
    }
    // Preparing the response
    setFileResponseStatus(&res, diskcopy, foundDiskCopy);
  }
  // Send the last response if necessary
  if (foundDiskCopy) {
    castor::replier::RequestReplier::getInstance()->
      sendResponse(client, &res);
  }
  // Cleanup
  for(std::list<castor::stager::DiskCopyInfo*>::iterator dcit
        = result->begin();
      dcit != result->end();
      ++dcit) {
    delete *dcit;
  }
  delete result;
}

//-----------------------------------------------------------------------------
// handleFileQueryRequestByFileId
//-----------------------------------------------------------------------------
void
castor::stager::dbService::QueryRequestSvcThread::handleFileQueryRequestByFileId
(castor::query::IQuerySvc* qrySvc,
 castor::IClient *client,
 std::string &fid,
 std::string &nshost,
 u_signed64 svcClassId,
 std::string reqId,
 Cuuid_t uuid)
  throw (castor::exception::Exception) {
  // Processing File Query by fileId
  castor::dlf::Param params[] =
    {castor::dlf::Param("fileId", fid.c_str()),
     castor::dlf::Param("nsHost", nshost.c_str())};
  castor::dlf::dlf_writep(uuid, DLF_LVL_SYSTEM, STAGER_QRYSVC_IQUERY, 1, params);
  std::list<castor::stager::DiskCopyInfo*>* result =
    qrySvc->diskCopies4File(fid, nshost, svcClassId);
  if(result == 0 || result->size() != 1) {   // sanity check, result.size() must be == 1
    castor::exception::Exception e(ENOENT);
    if(svcClassId > 0)
      e.getMessage() << "File " << fid << "@" << nshost << " not in given svcClass";
    else
      e.getMessage() << "File " << fid << "@" << nshost << " not in stager";
    if(result != 0)
      delete result;
    throw e;
  }
  castor::stager::DiskCopyInfo* diskcopy = *result->begin();
  bool foundDiskCopy = false;
  // Preparing the response
  castor::rh::FileQryResponse res;
  res.setReqAssociated(reqId);
  std::ostringstream sst;
  sst << fid << "@" << nshost;
  res.setFileName(sst.str());
  res.setFileId(diskcopy->fileId());
  // compute the status; INVALID is taken into account too
  setFileResponseStatus(&res, diskcopy, foundDiskCopy);
  // Sending the response
  castor::replier::RequestReplier::getInstance()->
    sendResponse(client, &res);
  // Cleanup
  delete diskcopy;
  delete result;
}

//-----------------------------------------------------------------------------
// handleFileQueryRequestByRequest
//-----------------------------------------------------------------------------
void
castor::stager::dbService::QueryRequestSvcThread::handleFileQueryRequestByRequest
(castor::query::IQuerySvc* qrySvc,
 castor::IClient *client,
 castor::stager::RequestQueryType reqType,
 std::string &val,
 u_signed64 svcClassId,
 std::string reqId,
 Cuuid_t uuid)
  throw (castor::exception::Exception) {
  // Processing File Query by Request
  castor::dlf::Param params[] =
    {castor::dlf::Param("ReqId", val.c_str())};
  castor::dlf::dlf_writep(uuid, DLF_LVL_SYSTEM, STAGER_QRYSVC_RQUERY, 1, params);
  std::list<castor::stager::DiskCopyInfo*>* result;
  result = qrySvc->diskCopies4Request(reqType, val, svcClassId);
  if (result == 0) {
    castor::exception::Exception e(EINVAL);
    e.getMessage() << "Unknown "
                   << ((reqType == REQUESTQUERYTYPE_USERTAG ||
                        reqType == REQUESTQUERYTYPE_USERTAG_GETNEXT) ?
                       "user tag " : "request id ") << val;
    throw e;
  }
  if (result->size() == 0) {
    castor::exception::Exception e(ENOENT);
    e.getMessage() << "Could not find results for "
                   << ((reqType == REQUESTQUERYTYPE_USERTAG ||
                        reqType == REQUESTQUERYTYPE_USERTAG_GETNEXT) ?
                       "user tag " : "request id ") << val;
    delete result;
    throw e;
  }
  // Iterates over the list of disk copies, and returns one result
  // per fileid to the client. It needs the SQL statement to return the diskcopies
  // sorted by fileid/nshost.
  u_signed64 fileid = 0;
  std::string nshost = "";
  castor::rh::FileQryResponse res;
  res.setReqAssociated(reqId);
  bool foundDiskCopy = false;
  for(std::list<castor::stager::DiskCopyInfo*>::iterator dcit
        = result->begin();
      dcit != result->end();
      ++dcit) {
    castor::stager::DiskCopyInfo* diskcopy = *dcit;
    if (diskcopy->fileId() != fileid
        || diskcopy->nsHost() != nshost) {
      // Sending the response for the previous
      // castor file being processed
      if (foundDiskCopy) {
        castor::replier::RequestReplier::getInstance()->
          sendResponse(client, &res);
      }
      // Now processing a new file !
      foundDiskCopy = false;
      fileid = diskcopy->fileId();
      nshost = diskcopy->nsHost();
      std::ostringstream sst;
      sst << diskcopy->fileId() << "@" <<  diskcopy->nsHost();
      res.setFileName(sst.str());
      res.setFileId(diskcopy->fileId());
    }
    /* Preparing the response */
    /* ---------------------- */
    setFileResponseStatus(&res, diskcopy, foundDiskCopy);
  }
  // Send the last response if necessary
  if (foundDiskCopy) {
    castor::replier::RequestReplier::getInstance()->
      sendResponse(client, &res);
  }
  /* Cleanup */
  /* ------- */
  for(std::list<castor::stager::DiskCopyInfo*>::iterator dcit
        = result->begin();
      dcit != result->end();
      ++dcit) {
    delete *dcit;
  }
  delete result;
}

//-----------------------------------------------------------------------------
// handleFileQueryRequest
//-----------------------------------------------------------------------------
void
castor::stager::dbService::QueryRequestSvcThread::handleFileQueryRequest
(castor::stager::Request* req,
 castor::IClient *client,
 castor::Services* svcs,
 castor::query::IQuerySvc* qrySvc,
 castor::BaseAddress &ad,
 Cuuid_t uuid)
  throw (castor::exception::Exception) {
  // Usefull Variables
  castor::stager::StageFileQueryRequest *uReq;
  try {
    // get the StageFileQueryRequest
    // cannot return 0 since we check the type before calling this method
    uReq = dynamic_cast<castor::stager::StageFileQueryRequest*> (req);
    // Iterating on the parameters to reply to each qry
    std::vector<castor::stager::QueryParameter*> params =
      uReq->parameters();
    // Log an error in case of empty query
    if (0 ==  uReq->parameters().size()) {
      castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_QRYSVC_FQNOPAR);
    }
    for(std::vector<QueryParameter*>::iterator it = params.begin();
        it != params.end();
        ++it) {
      castor::stager::RequestQueryType ptype = (*it)->queryType();
      std::string pval = (*it)->value();
      try {
        std::string fid, nshost, reqidtag;
        bool queryOk = false;
        switch(ptype) {
        case REQUESTQUERYTYPE_FILEID:
          {
            std::string::size_type idx = pval.find('@');
            if (idx == std::string::npos) {
              fid = pval;
              nshost = '%';
            } else {
              fid = pval.substr(0, idx);
              nshost = pval.substr(idx + 1);
            }
            queryOk = true;
          }
          break;
        case REQUESTQUERYTYPE_FILENAME:
        case REQUESTQUERYTYPE_REQID:
        case REQUESTQUERYTYPE_USERTAG:
        case REQUESTQUERYTYPE_REQID_GETNEXT:
        case REQUESTQUERYTYPE_USERTAG_GETNEXT:
          queryOk = true;
          break;
        }
        if (!queryOk) {
          castor::exception::Exception e(serrno);
          e.getMessage() << "Could not parse parameter: "
                         << ptype << "/"
                         << pval;
          throw e;
        }
        // Verify whether we are querying a directory (if not regexp)
        if(ptype == REQUESTQUERYTYPE_FILEID ||
           (ptype == REQUESTQUERYTYPE_FILENAME && 0 != pval.compare(0, 7, "regexp:"))) {
          // Get PATH for queries by fileId
          if(ptype == REQUESTQUERYTYPE_FILEID) {
            char cfn[CA_MAXPATHLEN+1];     // XXX unchecked string length in Cns_getpath() call
            if (Cns_getpath((char*)nshost.c_str(),
                            strtou64(fid.c_str()), cfn) < 0) {
              castor::exception::Exception e(serrno);
              e.getMessage() << "fileid " << fid;
              throw e;
            }
            pval = cfn;
          }
          struct Cns_filestat Cnsfilestat;
          if (Cns_stat(pval.c_str(), &Cnsfilestat) < 0) {
            castor::exception::Exception e(serrno);
            e.getMessage() << "file " << pval;
            throw e;
          }
          if((Cnsfilestat.filemode & S_IFDIR) == S_IFDIR) {
            // it is a directory, query for the content (don't perform a query by ID)
            ptype = REQUESTQUERYTYPE_FILENAME;
            if(pval[pval.length()-1] != '/')
              pval = pval + "/";
          }
        }
        // Get the SvcClass associated to the request
        u_signed64 svcClassId = 0;
        castor::stager::SvcClass* svcClass = uReq->svcClass();
        if (0 != svcClass) {
          svcClassId = svcClass->id();
        }
        // call the proper handling request
        if(ptype == REQUESTQUERYTYPE_FILENAME) {
          handleFileQueryRequestByFileName(qrySvc,
                                           client,
                                           pval,
                                           svcClassId,
                                           req->reqId(),
                                           uuid);
        } else if (ptype == REQUESTQUERYTYPE_FILEID) {
          handleFileQueryRequestByFileId(qrySvc,
                                         client,
                                         fid,
                                         nshost,
                                         svcClassId,
                                         req->reqId(),
                                         uuid);
        } else {
          handleFileQueryRequestByRequest(qrySvc,
                                          client,
                                          ptype,
                                          pval,
                                          svcClassId,
                                          req->reqId(),
                                          uuid);
        }
      } catch (castor::exception::Exception e) {
        // In case the file did not exist, we don't consider
        // it as an error from the server point of view
        castor::dlf::Param params[] =
          {castor::dlf::Param("function", "QueryRequestSvcThread::handleFileQueryRequest"),
           castor::dlf::Param("code", e.code()),
           castor::dlf::Param("message", e.getMessage().str())};
        castor::dlf::dlf_writep(uuid,
                                e.code() == ENOENT ? DLF_LVL_SYSTEM : DLF_LVL_ERROR,
                                STAGER_QRYSVC_UNKREQ, 3, params);
        // Send the exception to the client
        castor::rh::FileQryResponse res;
        res.setReqAssociated(req->reqId());
        res.setStatus(10000); // Dummy status code for non existing files
        res.setFileName(pval);
        res.setErrorCode(e.code());
        res.setErrorMessage(e.getMessage().str());
        // Reply To Client
        castor::replier::RequestReplier::getInstance()->
          sendResponse(client, &res);
      } // End catch
    } // End loop on all diskcopies
  } catch (castor::exception::Exception e) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("function", "QueryRequestSvcThread::handleFileQueryRequest"),
       castor::dlf::Param("code", e.code()),
       castor::dlf::Param("message", e.getMessage().str())};
    castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_QRYSVC_UNKREQ, 3, params);
    // try/catch this as well ?
    // Send the exception to the client
    castor::rh::FileQryResponse res;
    res.setReqAssociated(req->reqId());
    res.setStatus(10000); // Dummy status code for non existing files
    res.setErrorCode(e.code());
    res.setErrorMessage(e.getMessage().str());
    // Reply To Client
    castor::replier::RequestReplier::getInstance()->
      sendResponse(client, &res);
  }
  castor::replier::RequestReplier::getInstance()->
    sendEndResponse(client, req->reqId());
}

//-----------------------------------------------------------------------------
// handleDiskPoolQuery
//-----------------------------------------------------------------------------
void castor::stager::dbService::QueryRequestSvcThread::handleDiskPoolQuery
(castor::stager::Request* req,
 castor::IClient *client,
 castor::Services* svcs,
 castor::query::IQuerySvc* qrySvc,
 castor::BaseAddress &ad,
 Cuuid_t uuid)
  throw (castor::exception::Exception) {
  castor::query::DiskPoolQuery *uReq;
  try {
    // get the StageFileQueryRequest
    // cannot return 0 since we check the type before calling this method
    uReq = dynamic_cast<castor::query::DiskPoolQuery*> (req);
    if ("" == uReq->diskPoolName()) { // list all DiskPools for a given svcclass
      // Get the SvcClass associated to the request
      std::string svcClassName;
      castor::stager::SvcClass* svcClass = uReq->svcClass();
      if (0 != svcClass) {
        svcClassName = svcClass->name();
      }
      // Invoking the method
      std::vector<castor::query::DiskPoolQueryResponse*>* result =
        qrySvc->describeDiskPools(svcClassName);
      if(result == 0) {   // sanity check, result always is != 0
        castor::exception::NoEntry e;
        e.getMessage() << " describeDiskPools returned NULL";
        throw e;
      }
      for (std::vector<castor::query::DiskPoolQueryResponse*>::const_iterator it =
             result->begin();
           it != result->end();
           it++) {
        (*it)->setReqAssociated(req->reqId());
        castor::replier::RequestReplier::getInstance()->
          sendResponse(client, *it);
      }
      // Send the last response if necessary
      castor::replier::RequestReplier::getInstance()->
        sendEndResponse(client, req->reqId());
      // Cleanup
      delete result;
    } else { // list all DiskPools for a given svcclass

      // Invoking the method
      castor::query::DiskPoolQueryResponse* result =
        qrySvc->describeDiskPool(uReq->diskPoolName());
      if(result == 0) {   // sanity check, result always is != 0
        castor::exception::NoEntry e;
        e.getMessage() << " describeDiskPool returned NULL";
        throw e;
      }
      result->setReqAssociated(req->reqId());
      castor::replier::RequestReplier::getInstance()->
        sendResponse(client, result);
      castor::replier::RequestReplier::getInstance()->
        sendEndResponse(client, req->reqId());
    }
  } catch (castor::exception::Exception e) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("function", "QueryRequestSvcThread::handleDiskPoolQuery"),
       castor::dlf::Param("code", e.code()),
       castor::dlf::Param("message", e.getMessage().str())};
    castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_QRYSVC_UNKREQ, 3, params);
    // try/catch this as well ?
    // Send the exception to the client
    castor::query::DiskPoolQueryResponse res;
    res.setReqAssociated(req->reqId());
    res.setErrorCode(e.code());
    res.setErrorMessage(e.getMessage().str());
    // Reply To Client
    castor::replier::RequestReplier::getInstance()->
      sendResponse(client, &res);
  }
  castor::replier::RequestReplier::getInstance()->
    sendEndResponse(client, req->reqId());
}

//-----------------------------------------------------------------------------
// handleVersionQuery
//-----------------------------------------------------------------------------
void castor::stager::dbService::QueryRequestSvcThread::handleVersionQuery
(castor::stager::Request* req, castor::IClient *client)
  throw (castor::exception::Exception) {
  try {
    castor::query::VersionResponse result;
    result.setMajorVersion(MAJORVERSION);
    result.setMinorVersion(MINORVERSION);
    result.setMajorRelease(MAJORRELEASE);
    result.setMinorRelease(MINORRELEASE);
    castor::replier::RequestReplier *rr =
      castor::replier::RequestReplier::getInstance();
    rr->sendResponse(client, &result, true);
  } catch (castor::exception::Exception e) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("function", "QueryRequestSvcThread::handleVersionQuery"),
       castor::dlf::Param("code", e.code()),
       castor::dlf::Param("message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, STAGER_QRYSVC_UNKREQ, 3, params);
  }
}

//-----------------------------------------------------------------------------
// cleanup
//-----------------------------------------------------------------------------
void castor::stager::dbService::QueryRequestSvcThread::cleanup
(castor::stager::Request* req, castor::IService *svc) throw() {
  if (0 != req) {
    castor::stager::SvcClass *svcClass = req->svcClass();
    if (0 != svcClass) {
      delete svcClass;
    }
    delete req;
  }
  if (svc) svc->release();
}

//-----------------------------------------------------------------------------
// process
//-----------------------------------------------------------------------------
void castor::stager::dbService::QueryRequestSvcThread::process
(castor::IObject *param) throw() {
  castor::stager::Request* req = 0;
  castor::query::IQuerySvc *qrySvc = 0;
  castor::Services *svcs = 0;
  Cuuid_t uuid = nullCuuid;
  castor::BaseAddress ad;
  ad.setCnvSvcName("DbCnvSvc");
  ad.setCnvSvcType(castor::SVC_DBCNV);
  castor::IClient *client = 0;
  try {
    // get the QuerySvc. Note that we cannot cache it since we
    // would not be thread safe
    svcs = castor::BaseObject::services();
    castor::IService *svc = svcs->service("DbQuerySvc", castor::SVC_DBQUERYSVC);
    qrySvc = dynamic_cast<castor::query::IQuerySvc*>(svc);
    if (0 == qrySvc) {
      // "Could not get QuerySvc"
      castor::dlf::Param params[] =
        {castor::dlf::Param("function", "QueryRequestSvcThread::process")};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, STAGER_QRYSVC_GETSVC, 1, params);
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
      castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_QRYSVC_NOCLI);
      cleanup(req, qrySvc);
      return;
    }
  } catch (castor::exception::Exception e) {
    // If we fail here, we do NOT have enough information
    // to reply to the client ! So we only log something.
    castor::dlf::Param params[] =
      {castor::dlf::Param("function", "QueryRequestSvcThread::process.1"),
       castor::dlf::Param("message", e.getMessage().str()),
       castor::dlf::Param("code", e.code())};
    castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_QRYSVC_EXCEPT, 3, params);
    cleanup(req, qrySvc);
    return;
  }

  // At this point we are able to reply to the client
  bool failed = false;
  try {
    // Getting the parameters
    if (req->type() != castor::OBJ_DiskPoolQuery) {
      svcs->fillObj(&ad, req, castor::OBJ_QueryParameter);
    }
    // Getting the svcClass
    // We take an empty svcClass or '*' as a wildcard
    std::string className = req->svcClassName();
    if ("" != className && "*" != className) {
      castor::stager::SvcClass* svcClass = qrySvc->selectSvcClass(className);
      if (0 == svcClass) {
        // "Invalid ServiceClass name"
        castor::dlf::Param params[] =
          {castor::dlf::Param("svcClassName", className)};
        castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_QRYSVC_INVSC, 1, params);
        castor::exception::NoEntry e;
        e.getMessage() << "Invalid ServiceClass name : '" << className << "'";
        failed = true;
        throw e;
      }
      // Filling SvcClass in the DataBase
      req->setSvcClass(svcClass);
      svcs->fillRep(&ad, req, castor::OBJ_SvcClass, true);
    }
  } catch (castor::exception::Exception e) {
    if (!failed) {
      // "Unexpected exception caught"
      castor::dlf::Param params[] =
        {castor::dlf::Param("function", "QueryRequestSvcThread::process.2"),
         castor::dlf::Param("message", e.getMessage().str()),
         castor::dlf::Param("code", e.code())};
      castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_QRYSVC_EXCEPT, 3, params);
    }
    // In case of failure, we answer first to the client,
    // and there is some cleanup to do before we exit
    castor::rh::FileQryResponse res;
    res.setReqAssociated(req->reqId());
    res.setStatus(1);
    res.setErrorCode(e.code());
    res.setErrorMessage(e.getMessage().str());
    castor::replier::RequestReplier::getInstance()->
      sendResponse(client, &res);
    castor::replier::RequestReplier::getInstance()->
      sendEndResponse(client, req->reqId());
    // Cleanup
    cleanup(req, qrySvc);
    return;
  }

  // We call the adequate method
  switch (req->type()) {
  case castor::OBJ_StageFileQueryRequest:
    castor::stager::dbService::QueryRequestSvcThread::handleFileQueryRequest
      (req, client, svcs, qrySvc, ad, uuid);
    break;
  case castor::OBJ_StageFindRequestRequest:
    //castor::stager::dbService::QueryRequestSvcThread::handle_findRequestRequest
    //  (req, client, svcs, qrySvc, ad, uuid);
    break;
  case castor::OBJ_StageRequestQueryRequest:
    //castor::stager::dbService::QueryRequestSvcThread::handle_requestQueryRequest
    //  (req, client, svcs, qrySvc, ad, uuid);
    break;
  case castor::OBJ_DiskPoolQuery:
    castor::stager::dbService::QueryRequestSvcThread::handleDiskPoolQuery
      (req, client, svcs, qrySvc, ad, uuid);
    break;
  case castor::OBJ_VersionQuery:
    castor::stager::dbService::QueryRequestSvcThread::handleVersionQuery
      (req, client);
    break;
  default:
    // "Unknown Request type"
    castor::dlf::Param params[] =
      {castor::dlf::Param("type", req->type())};
    castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_QRYSVC_EXCEPT, 1, params);
    cleanup(req, qrySvc);
    return;
  }

  try {
    // Delete Request From the database
    svcs->deleteRep(&ad, req, true);
  } catch (castor::exception::Exception e) {
    // "Unexpected exception caught"
    castor::dlf::Param params[] =
      {castor::dlf::Param("function", "QueryRequestSvcThread::process.3"),
       castor::dlf::Param("message", e.getMessage().str()),
       castor::dlf::Param("code", e.code())};
    castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_QRYSVC_EXCEPT, 3, params);
  }

  // final cleanup
  cleanup(req, qrySvc);
  return;
}
