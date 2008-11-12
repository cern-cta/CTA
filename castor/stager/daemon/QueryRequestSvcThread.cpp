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
 * @(#)$RCSfile: QueryRequestSvcThread.cpp,v $ $Revision: 1.95 $ $Release$ $Date: 2008/11/12 12:49:02 $ $Author: waldron $
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

#include "castor/System.hpp"
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
#include "castor/exception/PermissionDenied.hpp"
#include "castor/BaseObject.hpp"
#include "castor/replier/RequestReplier.hpp"
#include "castor/query/VersionQuery.hpp"
#include "castor/query/VersionResponse.hpp"
#include "castor/stager/StageFileQueryRequest.hpp"
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
#include "castor/rh/IRHSvc.hpp"
#include "castor/query/IQuerySvc.hpp"
#include "castor/stager/daemon/QueryRequestSvcThread.hpp"
#include "castor/stager/daemon/DlfMessages.hpp"
#include "castor/bwlist/ChangePrivilege.hpp"
#include "castor/bwlist/ListPrivileges.hpp"
#include "castor/bwlist/ListPrivilegesResponse.hpp"
#include "stager_client_api.h"
#include "Cns_api.h"
#include "Cupv_api.h"
#include "u64subr.h"
#include "patchlevel.h"    // to get the default version
#include "getconfent.h"

//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::stager::daemon::QueryRequestSvcThread::QueryRequestSvcThread()
  throw () :
  BaseRequestSvcThread("QueryReqSvc", "DbQuerySvc", castor::SVC_DBQUERYSVC) {}

//-----------------------------------------------------------------------------
// setFileResponseStatus
//-----------------------------------------------------------------------------
void castor::stager::daemon::QueryRequestSvcThread::setFileResponseStatus
(castor::rh::FileQryResponse* fr,
 castor::stager::DiskCopyInfo* dc,
 bool& foundDiskCopy) throw() {

  // 1. Mapping diskcopy status to file status
  stage_fileStatus st = FILE_INVALID_STATUS;
  std::string diskServer = "";

  switch(dc->diskCopyStatus()) {
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
    }
    fr->setNbAccesses(fr->nbAccesses()+dc->nbAccesses());
  }

  // 3. Set other common attributes from the castorFile
  fr->setCastorFileName(dc->lastKnownFileName());
  fr->setSize(dc->size());
}

//-----------------------------------------------------------------------------
// handleFileQueryRequestByFileName
//-----------------------------------------------------------------------------
void
castor::stager::daemon::QueryRequestSvcThread::handleFileQueryRequestByFileName
(castor::query::IQuerySvc* qrySvc,
 castor::IClient *client,
 std::string& fileName,
 u_signed64 svcClassId,
 std::string reqId,
 Cuuid_t uuid)
  throw (castor::exception::Exception) {
  // "Processing File Query by Filename"
  castor::dlf::Param params[] =
    {castor::dlf::Param("Filename", fileName)};
  castor::dlf::dlf_writep(uuid, DLF_LVL_SYSTEM, STAGER_QRYSVC_FQUERY, 1, params);
  // Regular expression supported is no longer supported
  if (fileName.compare(0, 7, "regexp:") == 0) {
    castor::exception::Exception ex(SEOPNOTSUP);
    ex.getMessage() << "Operation not supported for regular expressions";
    throw ex;
  }
  // List diskCopies
  std::list<castor::stager::DiskCopyInfo*>* result =
    qrySvc->diskCopies4FileName(fileName, svcClassId);
  if(result == 0 || result->size() == 0) {   // sanity check, result always is != 0
    castor::exception::Exception e(ENOENT);
    if(svcClassId > 0)
      e.getMessage() << "File " << fileName << " not on this service class";
    else
      e.getMessage() << "File " << fileName << " not on disk cache";
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
castor::stager::daemon::QueryRequestSvcThread::handleFileQueryRequestByFileId
(castor::query::IQuerySvc* qrySvc,
 castor::IClient *client,
 u_signed64 fid,
 std::string &nshost,
 std::string &fileName,
 u_signed64 svcClassId,
 std::string reqId,
 Cuuid_t uuid)
  throw (castor::exception::Exception) {
  // Processing File Query by fileid
  std::ostringstream sst;
  sst << fid;
  castor::dlf::Param params[] =
    {castor::dlf::Param("FileId", fid),
     castor::dlf::Param("NsHost", nshost)};
  castor::dlf::dlf_writep(uuid, DLF_LVL_SYSTEM, STAGER_QRYSVC_IQUERY, 2, params);
  std::list<castor::stager::DiskCopyInfo*>* result =
    qrySvc->diskCopies4File(fid, nshost, svcClassId, fileName);
  if(result == 0 || result->size() == 0) {   // sanity check, result.size() must be == 1
    castor::exception::Exception e(ENOENT);
    if(svcClassId > 0)
      e.getMessage() << "File " << fileName << " (" << fid << "@" << nshost << ") not on this service class";
    else
      e.getMessage() << "File " << fileName << " (" << fid << "@" << nshost << ") not on disk cache";
    if(result != 0)
      delete result;
    throw e;
  }
  bool foundDiskCopy = false;
  // Preparing the response
  castor::rh::FileQryResponse res;
  res.setReqAssociated(reqId);
  sst << "@" << nshost;
  res.setFileName(sst.str());
  res.setFileId(fid);
  // Iterates over the list of disk copies, and computes status
  for(std::list<castor::stager::DiskCopyInfo*>::iterator dcit
        = result->begin();
      dcit != result->end();
      ++dcit) {
    setFileResponseStatus(&res, *dcit, foundDiskCopy);
  }
  // Sending the response
  castor::replier::RequestReplier::getInstance()->
    sendResponse(client, &res);
  // Cleanup
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
// handleFileQueryRequestByRequest
//-----------------------------------------------------------------------------
void
castor::stager::daemon::QueryRequestSvcThread::handleFileQueryRequestByRequest
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
    {castor::dlf::Param("ReqId", val)};
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
castor::stager::daemon::QueryRequestSvcThread::handleFileQueryRequest
(castor::stager::Request* req,
 castor::IClient *client,
 castor::query::IQuerySvc* qrySvc,
 Cuuid_t uuid)
  throw (castor::exception::Exception) {
  // Useful Variables
  castor::stager::StageFileQueryRequest *uReq;
  // get the StageFileQueryRequest
  // cannot return 0 since we check the type before calling this method
  uReq = dynamic_cast<castor::stager::StageFileQueryRequest*> (req);
  // Iterating on the parameters to reply to each qry
  std::vector<castor::stager::QueryParameter*> params =
    uReq->parameters();
  // Log an error in case of empty query
  if (0 ==  uReq->parameters().size()) {
    castor::dlf::dlf_writep(uuid, DLF_LVL_USER_ERROR, STAGER_QRYSVC_FQNOPAR);
  }
  for(std::vector<QueryParameter*>::iterator it = params.begin();
      it != params.end();
      ++it) {
    castor::stager::RequestQueryType ptype = (*it)->queryType();
    std::string pval = (*it)->value();
    try {
      std::string nshost, reqidtag;
      u_signed64 fid = 0;
      bool queryOk = false;
      switch(ptype) {
      case REQUESTQUERYTYPE_FILEID:
        {
          std::string::size_type idx = pval.find('@');
          if (idx == std::string::npos) {
            fid = strtou64(pval.c_str());
            nshost = '%';
          } else {
            fid = strtou64(pval.substr(0, idx).c_str());
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
        castor::exception::Exception e(EINVAL);
        e.getMessage() << "Could not parse parameter: "
                       << ptype << "/"
                       << pval;
        throw e;
      }

      // Verify whether we are querying a directory (if not regexp)
      if (ptype == REQUESTQUERYTYPE_FILEID ||
         (ptype == REQUESTQUERYTYPE_FILENAME && (0 != pval.compare(0, 7, "regexp:")))) {
        // Get PATH for queries by fileId
        if (ptype == REQUESTQUERYTYPE_FILEID) {
          char cfn[CA_MAXPATHLEN+1];     // XXX unchecked string length in Cns_getpath() call
          if (Cns_getpath((char*)nshost.c_str(), fid, cfn) < 0) {
            castor::exception::Exception e(serrno);
            e.getMessage() << "Fileid " << fid;
            throw e;
          }
          pval = cfn;
        }
        // check if the filename is valid (it has to start with /)
	if (pval.empty() || (pval.at(0) != '/')) {
	  castor::exception::Exception ex(EINVAL);
	  ex.getMessage() << "Invalid file path";
	  throw ex;
	}
        // stat the file in the nameserver
        struct Cns_filestat Cnsfilestat;
        struct Cns_fileid Cnsfileid;
        Cnsfileid.server[0] = 0;
        if (Cns_statx(pval.c_str(), &Cnsfileid, &Cnsfilestat) < 0) {
          castor::exception::Exception e(serrno);
          e.getMessage() << "Filename " << pval;
          throw e;
        }
        // deal with directories and with pure file request
        if ((Cnsfilestat.filemode & S_IFDIR) == S_IFDIR) {
          // it is a directory, query for the content (don't perform a query by ID)
          ptype = REQUESTQUERYTYPE_FILENAME;
          if (pval[pval.length()-1] != '/')
            pval = pval + "/";
        } else {
          // prefer the querying by file id for real files
          // in order to be immune to file renames
          ptype = REQUESTQUERYTYPE_FILEID;
          if (0 == fid) {
            fid = Cnsfileid.fileid;
            nshost = Cnsfileid.server;
          }
        }
      }
      // Get the SvcClass associated to the request
      u_signed64 svcClassId = 0;
      castor::stager::SvcClass* svcClass = uReq->svcClass();
      if (0 != svcClass) {
        svcClassId = svcClass->id();
      }
      // call the proper handling request
      if (ptype == REQUESTQUERYTYPE_FILENAME) {
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
                                       pval,
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
      // In case the file and/or the request did not exist, we don't consider
      // it as an error from the server point of view
      castor::dlf::Param params[] =
        {castor::dlf::Param("Function", "QueryRequestSvcThread::handleFileQueryRequest"),
         castor::dlf::Param("Code", e.code()),
         castor::dlf::Param("Message", e.getMessage().str())};
      switch(e.code()) {
        case ENOENT:
          castor::dlf::dlf_writep(uuid, DLF_LVL_USER_ERROR, STAGER_USER_NONFILE, 3, params);
          break;
        case EINVAL:
          castor::dlf::dlf_writep(uuid, DLF_LVL_USER_ERROR, STAGER_QRYSVC_INVARG, 3, params);
          break;
        default:
          castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_QRYSVC_EXCEPT, 3, params);
          break;
      }
      // Send the exception to the client
      castor::rh::FileQryResponse res;
      res.setReqAssociated(req->reqId());
      res.setStatus(10000); // Dummy status code for non existing files
      res.setCastorFileName(pval);
      res.setFileName(pval);
      res.setErrorCode(e.code());
      res.setErrorMessage(e.getMessage().str());
      // Reply To Client
      castor::replier::RequestReplier::getInstance()->
	sendResponse(client, &res);
    } // End catch
  } // End loop on all diskcopies
  castor::replier::RequestReplier::getInstance()->
    sendEndResponse(client, req->reqId());
}

//-----------------------------------------------------------------------------
// handleDiskPoolQuery
//-----------------------------------------------------------------------------
void castor::stager::daemon::QueryRequestSvcThread::handleDiskPoolQuery
(castor::stager::Request* req,
 castor::IClient *client,
 castor::query::IQuerySvc* qrySvc,
 Cuuid_t uuid)
  throw (castor::exception::Exception) {
  castor::query::DiskPoolQuery *uReq;
  try {
    // Get the StageFileQueryRequest
    // cannot return 0 since we check the type before calling this method
    uReq = dynamic_cast<castor::query::DiskPoolQuery*> (req);
    // Get the SvcClass associated to the request
    castor::stager::SvcClass* svcClass = uReq->svcClass();
    std::string svcClassName;
    if (0 != svcClass) {
      svcClassName = svcClass->name();
    }

    // Get the name of the client hostname to pass into the Cupv interface.
    const castor::rh::Client *c =
      dynamic_cast<const castor::rh::Client*>(client);
    std::string srcHostName =
      castor::System::ipAddressToHostname(c->ipAddress());

    // Check if the user has ADMIN privileges so that they can see detailed
    // information about the diskservers and filesystems within a given pool
    // and/or svcclass
    bool detailed = false;
    int rc = Cupv_check(req->euid(), req->egid(),
			srcHostName.c_str(), "", P_ADMIN);
    if ((rc < 0) && (serrno != EACCES)) {
      castor::exception::Exception e(serrno);
      e.getMessage() << "Failed Cupv_check call for "
		     << req->euid() << ":" << req->egid() << " (ADMIN)";
      throw e;
    } else if (rc == 0) {
      detailed = true;
    }

    // List all DiskPools for a given svcclass
    if ("" == uReq->diskPoolName()) {

      // "Processing DiskPoolQuery by SvcClass"
      castor::dlf::Param params[] =
	{castor::dlf::Param("SvcClass", svcClassName)};
      castor::dlf::dlf_writep(uuid, DLF_LVL_SYSTEM, STAGER_QRYSVC_DSQUERY, 1, params);

      // Invoking the method
      std::vector<castor::query::DiskPoolQueryResponse*>* result =
        qrySvc->describeDiskPools(svcClassName, req->euid(), req->egid(), detailed);
      if (result == 0) {
        castor::exception::NoEntry e;
        e.getMessage() << " describeDiskPools returned NULL";
        throw e;
      }
      for (std::vector<castor::query::DiskPoolQueryResponse*>::iterator it =
             result->begin();
           it != result->end();
           it++) {
        (*it)->setReqAssociated(req->reqId());
        castor::replier::RequestReplier::getInstance()->
          sendResponse(client, *it);
        delete (*it);
      }
      delete result;
    }

    // List all DiskPools for a given svcclass
    else {
      // "Processing DiskPoolQuery by DiskPool"
      castor::dlf::Param params[] =
	{castor::dlf::Param("DiskPool", uReq->diskPoolName()),
	 castor::dlf::Param("SvcClass", svcClassName)};
      castor::dlf::dlf_writep(uuid, DLF_LVL_SYSTEM, STAGER_QRYSVC_DDQUERY, 2, params);

      // Invoking the method
      castor::query::DiskPoolQueryResponse* result =
        qrySvc->describeDiskPool(uReq->diskPoolName(), svcClassName, detailed);
      if (result == 0) {
        castor::exception::NoEntry e;
        e.getMessage() << " describeDiskPool returned NULL";
        throw e;
      }
      result->setReqAssociated(req->reqId());
      castor::replier::RequestReplier::getInstance()->
        sendResponse(client, result);
      delete result;
    }
  } catch (castor::exception::Exception e) {
    if (e.code() == EINVAL) {
      // "Failed to process DiskPoolQuery"
      castor::dlf::Param params[] =
	{castor::dlf::Param("Message", e.getMessage().str())};
      castor::dlf::dlf_writep(uuid, DLF_LVL_USER_ERROR, STAGER_QRYSVC_DFAILED, 1, params);
    } else {
      // "Unexpected exception caught"
      castor::dlf::Param params[] =
	{castor::dlf::Param("Function", "QueryRequestSvcThread::handleDiskPoolQuery"),
	 castor::dlf::Param("Code", e.code()),
	 castor::dlf::Param("Message", e.getMessage().str())};
      castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_QRYSVC_EXCEPT, 3, params);
    }

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
// handleChangePrivilege
//-----------------------------------------------------------------------------
void castor::stager::daemon::QueryRequestSvcThread::handleChangePrivilege
(castor::stager::Request* req,
 castor::IClient *client,
 castor::rh::IRHSvc* rhSvc,
 Cuuid_t uuid)
  throw (castor::exception::Exception) {
  castor::bwlist::ChangePrivilege *uReq;
  castor::rh::BasicResponse res;
  try {
    // prepare response first in case we fail
    res.setReqAssociated(req->reqId());
    // Get the name of the client hostname to pass into the Cupv interface.
    const castor::rh::Client *c =
      dynamic_cast<const castor::rh::Client*>(client);
    std::string srcHostName =
      castor::System::ipAddressToHostname(c->ipAddress());
    // check privileges of the caller. He must be ADMIN in general
    // or GROUP_ADMIN for user operations
    int rc = Cupv_check(req->euid(), req->egid(),
                        srcHostName.c_str(),
			"", P_GRP_ADMIN);
    if ((rc < 0) && (serrno != EACCES)) {
      castor::exception::Exception e(serrno);
      e.getMessage() << "Failed Cupv_check call for "
		     << req->euid() << ":" << req->egid() << " (GRP_ADMIN)";
      throw e;
    } else if (rc < 0) {
      // Check for GRP_ADMIN failed so check if the user is an ADMIN
      rc = Cupv_check(req->euid(), req->egid(),
		      srcHostName.c_str(),
		      "", P_ADMIN);
      if ((rc < 0) && (serrno != EACCES)){
	castor::exception::Exception e(serrno);
	e.getMessage() << "Failed Cupv_check call for "
		       << req->euid() << ":" << req->egid() << " (ADMIN)";
	throw e;
      } else if (rc < 0) {
	castor::exception::PermissionDenied e;
	e.getMessage() << "Not authorized to change permissions. Please ask your group admin";
	throw e;
      }
    }
    // Get the ChangePrivilege
    // cannot return 0 since we check the type before calling this method
    uReq = dynamic_cast<castor::bwlist::ChangePrivilege*> (req);
    // call method
    rhSvc->changePrivilege(uReq->svcClassName(), uReq->users(),
			   uReq->requestTypes(),uReq->isGranted());
    // Reply To Client
    castor::replier::RequestReplier::getInstance()->
      sendResponse(client, &res, true);
  } catch (castor::exception::Exception e) {
    // "Unexpected exception caught"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Function", "QueryRequestSvcThread::handleChangePrivilege"),
       castor::dlf::Param("Code", e.code()),
       castor::dlf::Param("Message", e.getMessage().str())};
    castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_QRYSVC_EXCEPT, 3, params);
    // Fill the reponse with the error
    res.setErrorCode(e.code());
    res.setErrorMessage(e.getMessage().str());
    // and try to answer the client
    try {
      castor::replier::RequestReplier::getInstance()->
	sendResponse(client, &res, true);
    } catch (castor::exception::Exception e) {
      // nothing that can really be done here
    }
  }
}

//-----------------------------------------------------------------------------
// handleListPrivileges
//-----------------------------------------------------------------------------
void castor::stager::daemon::QueryRequestSvcThread::handleListPrivileges
(castor::stager::Request* req,
 castor::IClient *client,
 castor::rh::IRHSvc* rhSvc,
 Cuuid_t uuid)
  throw (castor::exception::Exception) {
  castor::bwlist::ListPrivileges *uReq;
  castor::bwlist::ListPrivilegesResponse res;
  try {
    // prepare response first in case we fail
    res.setReqAssociated(req->reqId());
    // Get the ListPrivileges
    // cannot return 0 since we check the type before calling this method
    uReq = dynamic_cast<castor::bwlist::ListPrivileges*> (req);
    // call method
    std::vector<castor::bwlist::Privilege*> privList =
      rhSvc->listPrivileges(uReq->svcClassName(), uReq->userId(),
			    uReq->groupId(), uReq->requestType());
    // fill reponse with white list part
    for (std::vector<castor::bwlist::Privilege*>::const_iterator it =
	   privList.begin();
	 it != privList.end();
	 it++) {
      res.addPrivileges(*it);
    }
    // Reply To Client
    castor::replier::RequestReplier::getInstance()->
      sendResponse(client, &res, true);
  } catch (castor::exception::Exception e) {
    // "Unexpected exception caught"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Function", "QueryRequestSvcThread::handleListPrivilege"),
       castor::dlf::Param("Code", e.code()),
       castor::dlf::Param("Message", e.getMessage().str())};
    castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_QRYSVC_EXCEPT, 3, params);
    // Fill the reponse with the error
    res.setErrorCode(e.code());
    res.setErrorMessage(e.getMessage().str());
    // and try to answer the client
    try {
      castor::replier::RequestReplier::getInstance()->
	sendResponse(client, &res, true);
    } catch (castor::exception::Exception e) {
      // nothing that can really be done here
    }
  }
}

//-----------------------------------------------------------------------------
// handleVersionQuery
//-----------------------------------------------------------------------------
void castor::stager::daemon::QueryRequestSvcThread::handleVersionQuery
(castor::stager::Request* req, castor::IClient *client)
  throw (castor::exception::Exception) {
  castor::query::VersionResponse result;
  result.setMajorVersion(MAJORVERSION);
  result.setMinorVersion(MINORVERSION);
  result.setMajorRelease(MAJORRELEASE);
  result.setMinorRelease(MINORRELEASE);
  castor::replier::RequestReplier *rr =
    castor::replier::RequestReplier::getInstance();
  rr->sendResponse(client, &result, true);
}

//-----------------------------------------------------------------------------
// cleanup
//-----------------------------------------------------------------------------
void castor::stager::daemon::QueryRequestSvcThread::cleanup
(castor::stager::Request* req,
 castor::IService *svc, castor::IService *extraSvc) throw() {
  if (0 != req) {
    castor::stager::SvcClass *svcClass = req->svcClass();
    if (0 != svcClass) {
      delete svcClass;
    }
    delete req;
  }
  if (svc) svc->release();
  if (extraSvc) extraSvc->release();
}

//-----------------------------------------------------------------------------
// process
//-----------------------------------------------------------------------------
void castor::stager::daemon::QueryRequestSvcThread::process
(castor::IObject *param) throw() {
  // Useful variables
  castor::stager::Request* req = 0;
  castor::query::IQuerySvc *qrySvc = 0;
  castor::rh::IRHSvc *rhSvc = 0;
  castor::Services *svcs = 0;
  Cuuid_t uuid = nullCuuid;
  castor::IClient *client = 0;
  // address to access db
  castor::BaseAddress ad;
  ad.setCnvSvcName("DbCnvSvc");
  ad.setCnvSvcType(castor::SVC_DBCNV);
  try {
    // get the QuerySvc. Note that we cannot cache it since we
    // would not be thread safe
    svcs = castor::BaseObject::services();
    castor::IService *svc = svcs->service("DbQuerySvc", castor::SVC_DBQUERYSVC);
    qrySvc = dynamic_cast<castor::query::IQuerySvc*>(svc);
    svc = svcs->service("DbRHSvc", castor::SVC_DBRHSVC);
    rhSvc = dynamic_cast<castor::rh::IRHSvc*>(svc);
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
      cleanup(req, qrySvc, rhSvc);
      return;
    }
  } catch (castor::exception::Exception e) {
    // If we fail here, we do NOT have enough information
    // to reply to the client ! So we only log something.
    // "Unexpected exception caught"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Function", "QueryRequestSvcThread::process.1"),
       castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param("Code", e.code())};
    castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_QRYSVC_EXCEPT, 3, params);
    cleanup(req, qrySvc, rhSvc);
    return;
  }

  // At this point we are able to reply to the client
  bool failed = false;
  try {
    // Getting the parameters
    if (req->type() == OBJ_StageFileQueryRequest ||
        req->type() == OBJ_VersionQuery) {
      svcs->fillObj(&ad, req, castor::OBJ_QueryParameter);
    }
    // Getting users and requesttypes
    if (req->type() == OBJ_ChangePrivilege) {
      svcs->fillObj(&ad, req, castor::OBJ_BWUser);
      svcs->fillObj(&ad, req, castor::OBJ_RequestType);
    }
    // Getting the svcClass
    // We take an empty svcClass or '*' as a wildcard
    if (req->type() == OBJ_StageFileQueryRequest ||
        req->type() == OBJ_DiskPoolQuery) {
      std::string className = req->svcClassName();
      if ("" != className && "*" != className) {
        castor::stager::SvcClass* svcClass = qrySvc->selectSvcClass(className);
        if (0 == svcClass) {
          // "Invalid ServiceClass name"
          castor::dlf::Param params[] =
            {castor::dlf::Param("SvcClass", className)};
          castor::dlf::dlf_writep(uuid, DLF_LVL_USER_ERROR, STAGER_QRYSVC_INVSC, 1, params);
          castor::exception::NoEntry e;
          e.getMessage() << "Invalid ServiceClass name : '" << className << "'";
          failed = true;
          throw e;
        }
        // Filling SvcClass in the DataBase
        req->setSvcClass(svcClass);
        svcs->fillRep(&ad, req, castor::OBJ_SvcClass, false);
      }
    }
  } catch (castor::exception::Exception e) {
    if (!failed) {
      // "Unexpected exception caught"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Function", "QueryRequestSvcThread::process.2"),
         castor::dlf::Param("Message", e.getMessage().str()),
         castor::dlf::Param("Code", e.code())};
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
    cleanup(req, qrySvc, rhSvc);
    return;
  }

  try {
    // We call the adequate method
    switch (req->type()) {
    case castor::OBJ_StageFileQueryRequest:
      castor::stager::daemon::QueryRequestSvcThread::handleFileQueryRequest
        (req, client, qrySvc, uuid);
      break;
    case castor::OBJ_DiskPoolQuery:
      castor::stager::daemon::QueryRequestSvcThread::handleDiskPoolQuery
        (req, client, qrySvc, uuid);
      break;
    case castor::OBJ_VersionQuery:
      castor::stager::daemon::QueryRequestSvcThread::handleVersionQuery
        (req, client);
      break;
    case OBJ_ChangePrivilege:
      castor::stager::daemon::QueryRequestSvcThread::handleChangePrivilege
        (req, client, rhSvc, uuid);
      break;
    case OBJ_ListPrivileges:
      castor::stager::daemon::QueryRequestSvcThread::handleListPrivileges
        (req, client, rhSvc, uuid);
      break;
    default:
      // "Unknown Request type"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Type", req->type())};
      castor::dlf::dlf_writep(uuid, DLF_LVL_USER_ERROR, STAGER_QRYSVC_UNKREQ, 1, params);
    }

    // Delete Request From the database
    svcs->deleteRep(&ad, req, true);
  } catch (castor::exception::Exception e) {
    // "Unexpected exception caught"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Function", "QueryRequestSvcThread::process.3"),
       castor::dlf::Param("Message", e.getMessage().str()),
       castor::dlf::Param("Code", e.code())};
    castor::dlf::dlf_writep(uuid, DLF_LVL_ERROR, STAGER_QRYSVC_EXCEPT, 3, params);
  }

  // final cleanup
  cleanup(req, qrySvc, rhSvc);
  return;
}
