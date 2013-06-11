/*******************************************************************************
 *                      XrdxCastor2Stager.cc
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2012  CERN
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
 *
 * @author Elvin Sindrilaru & Andreas Peters - CERN
 *
 ******************************************************************************/


/*-----------------------------------------------------------------------------*/
#include <sys/types.h>
#include <iostream>
/*-----------------------------------------------------------------------------*/
#include "castor/BaseObject.hpp"
#include "castor/Constants.hpp"
#include "castor/client/VectorResponseHandler.hpp"
#include "castor/client/BaseClient.hpp"
#include "castor/stager/Request.hpp"
#include "castor/Services.hpp"
#include "castor/IService.hpp"
#include "castor/stager/IJobSvc.hpp"
#include "castor/stager/StagePrepareToGetRequest.hpp"
#include "castor/stager/StagePrepareToPutRequest.hpp"
#include "castor/stager/StagePrepareToUpdateRequest.hpp"
#include "castor/stager/StagePutRequest.hpp"
#include "castor/stager/StageGetRequest.hpp"
#include "castor/stager/StageUpdateRequest.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/rh/IOResponse.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/Communication.hpp"
#include "stager_client_api_common.hpp"
#include "stager_api.h"
/*-----------------------------------------------------------------------------*/
#include "XrdxCastor2Stager.hpp"
#include "XrdxCastor2Fs.hpp"
/*-----------------------------------------------------------------------------*/


extern XrdxCastor2Fs* XrdxCastor2FS;  ///< defined in XrdxCastor2Fs.cpp
XrdSysRWLock XrdxCastor2Stager::msLockDelay;  ///< lock for delay map


//------------------------------------------------------------------------------
// Delete the request and response objects
//------------------------------------------------------------------------------
void
XrdxCastor2Stager::DeleteReqResp(castor::stager::FileRequest* req,
                                 castor::client::IResponseHandler* resp,
                                 std::vector<castor::rh::Response*>* respvec)
{
  // Delete request
  delete req;
  // Delete the responses and the vector
  delete resp;
  castor::rh::Response* ptr = 0;

  for (unsigned int i = 0; i < respvec->size(); i++)
  {
    ptr = respvec->at(i);
    delete ptr;
  }

  respvec->clear();
  delete respvec;
}



//------------------------------------------------------------------------------
// Prepare to get
//------------------------------------------------------------------------------
bool
XrdxCastor2Stager::Prepare2Get(XrdOucErrInfo& error,
                               uid_t          uid,
                               gid_t          gid,
                               const char*    path,
                               const char*    stagehost,
                               const char*    serviceclass,
                               struct RespInfo& respInfo)
{
  //const char* tident = error.getErrUser();
  struct stage_options Opts;
  xcastor_static_debug("uid=%i, gid=%i, path=%s, stagehost=%s, sericeclass=%s",
                       uid, gid, path, stagehost, serviceclass);
  // Construct the request and subrequest objects
  castor::stager::StagePrepareToGetRequest  getReq_ro;
  castor::stager::SubRequest* subreq = new castor::stager::SubRequest();
  std::vector<castor::rh::Response*>respvec;
  castor::client::VectorResponseHandler rh(&respvec);
  int mask = umask(0);
  umask(mask);
  getReq_ro.setUserTag("xCastor2P2Get");
  getReq_ro.setMask(mask);
  getReq_ro.addSubRequests(subreq);
  subreq->setRequest(&getReq_ro);
  subreq->setProtocol(std::string("xroot"));
  subreq->setFileName(std::string(path));
  subreq->setModeBits(0744);
  castor::client::BaseClient cs2client(stage_getClientTimeout(), -1);
  Opts.stage_host    = (char*)stagehost;
  Opts.service_class = (char*)serviceclass;
  Opts.stage_version = 2;
  Opts.stage_port    = 0;
  cs2client.setOptions(&Opts);
  cs2client.setAuthorizationId(uid, gid);

  try
  {
    xcastor_static_debug("Sending Prepare2Get path=%s, uid=%i, gid=%i, stagehost=%s, svc=%s",
                         path, uid, gid, stagehost, serviceclass);
    std::string reqid = cs2client.sendRequest(&getReq_ro, &rh);
  }
  catch (castor::exception::Communication e)
  {
    xcastor_static_debug("Communication error: %s", e.getMessage().str().c_str());
    error.setErrInfo(ECOMM, e.getMessage().str().c_str());
    delete subreq;
    return false;
  }
  catch (castor::exception::Exception e)
  {
    xcastor_static_debug("sendRequest exception: %s", e.getMessage().str().c_str());
    error.setErrInfo(ECOMM, e.getMessage().str().c_str());
    delete subreq;
    return false;
  }

  if (respvec.size() <= 0)
  {
    xcastor_static_debug("No response for prepare2get for: %s", path);
    error.setErrInfo(ECOMM, "No response");
    delete subreq;
    return false;
  }

  // Proccess the response.
  castor::rh::IOResponse* fr =
    dynamic_cast<castor::rh::IOResponse*>(respvec[0]);

  if (0 == fr)
  {
    xcastor_static_debug("Invalid response object for prepare2get: %s ", path);
    error.setErrInfo(ECOMM, "Invalid response object for prepare2get");
    delete subreq;
    return false;
  }

  if (fr->errorCode())
  {
    std::stringstream sstr;
    sstr << "uid=" << (int)uid << " gid=" << (int)gid << " path=" << path
         << " stagehost=" << stagehost << " serviceclass=" << serviceclass;
    XrdOucString ErrPrefix = sstr.str().c_str();
    xcastor_static_debug("received error errc=%i, errmsg=%s\%i, subreqid=%s, reqid=%s",
                         fr->errorCode(), fr->errorMessage().c_str(), ErrPrefix.c_str(),
                         fr->subreqId().c_str(), fr->reqAssociated().c_str());
    XrdOucString emsg = "received error errc=";
    emsg += (int)fr->errorCode();
    emsg += " errmsg=\"";
    emsg += fr->errorMessage().c_str();
    emsg += "\" ";
    emsg += ErrPrefix;
    emsg += " subreqid=";
    emsg += fr->subreqId().c_str();
    emsg += " reqid=";
    emsg += fr->reqAssociated().c_str();
    error.setErrInfo(fr->errorCode(), emsg.c_str());
    delete subreq;
    delete respvec[0];
    return false;
  }
  else
  {
    xcastor_static_debug("%s status=%u, path=%s:%s ( %s ) id=%i",
                         stage_requestStatusName(fr->status()), fr->status(),
                         fr->server().c_str(), fr->fileName().c_str(),
                         fr->castorFileName().c_str(), fr->id());
  }

  respInfo.mRedirectionHost = fr->server().c_str();
  respInfo.mRedirectionPfn1 = fr->fileName().c_str();
  respInfo.mStageStatus = stage_requestStatusName(fr->status());
  delete subreq;
  delete respvec[0];
  return true;
}


//------------------------------------------------------------------------------
// Send an async request to the stager. This request can be a GET or a PUT
// or an UPDATE.
//------------------------------------------------------------------------------
int 
XrdxCastor2Stager::DoAsyncReq(XrdOucErrInfo& error,
                              const std::string& opType,
                              struct ReqInfo* reqInfo,
                              struct RespInfo& respInfo)
{
 xcastor_static_debug("op=%s uid=%i, gid=%i, path=%s, stagehost=%s, serviceclass=%s",
                      opType.c_str(), reqInfo->mUid, reqInfo->mGid, reqInfo->mPath, 
                      reqInfo->mStageHost, reqInfo->mServiceClass);
  // Build the user id
  std::string user_tag;
  const char* tident = error.getErrUser();
  std::stringstream sstr;
  sstr << tident << ":" << reqInfo->mPath << ":" << opType;
  std::string user_id = sstr.str();

  // Check if we are coming back for an old request
  bool found = false;
  struct xcastor::XrdxCastorClient::ReqElement* elem = 0;
  elem = XrdxCastor2FS->msCastorClient->GetResponse(user_id, found);
  
  // We are looking for a previous request 
  if (found) 
  {
    if (elem)
    {
      // Response was received, we just need to process it 
      xcastor_static_debug("found response for previous request");
      DropDelayTag(user_id.c_str());
      return ProcessResponse(error, elem, opType, reqInfo, respInfo);     
    }
    else 
    {
      // Response not received, we need to stall the client some more
      xcastor_static_debug("response for previous request hasn't arrived yer");
      return SFS_STALL;
    }
  }
  
  // Build the request and response objects
  castor::stager::FileRequest* request;
  
  if (opType == "get")
  {
    user_tag = "xCasto2Get";
    request = new castor::stager::StageGetRequest();
  }
  else if (opType == "put")
  {
    user_tag = "xCastor2Put";
    request = new castor::stager::StagePutRequest();
    
  }
  else if (opType == "update")
  {
    user_tag = "xCastor2Update";
    request = new castor::stager::StageUpdateRequest();
  }
  else 
  {
    xcastor_static_err("unknown operation type: %s", opType.c_str());
    return SFS_ERROR;      
  }

  castor::stager::SubRequest* subreq = new castor::stager::SubRequest();
  std::vector<castor::rh::Response*>* respvec =  new std::vector<castor::rh::Response*>();
  castor::client::VectorResponseHandler* rh = new castor::client::VectorResponseHandler(respvec);
  int mask = umask(0);
  umask(mask);
  request->setUserTag(user_tag);
  request->setMask(mask);
  request->addSubRequests(subreq);
  request->setEuid(reqInfo->mUid);
  request->setEgid(reqInfo->mGid);
  request->setUserName(std::string(tident));
  request->setPid(getpid());
  request->setSvcClassName(std::string(reqInfo->mServiceClass));
  subreq->setRequest(request);
  subreq->setProtocol(std::string("xroot"));
  subreq->setFileName(std::string(reqInfo->mPath));
  subreq->setModeBits(0744);

  try
  {
    // Sending asynchronous get requests
    xcastor_static_debug("op=%s path=%s, uid=%i, gid=%i, stagehost=%s, svc=%s",
                         opType.c_str(), reqInfo->mPath, reqInfo->mUid, reqInfo->mGid, 
                         reqInfo->mStageHost, reqInfo->mServiceClass);

    int retc = XrdxCastor2FS->msCastorClient->SendAsyncRequest( user_id, 
                    std::string(reqInfo->mStageHost), 0, request, rh, respvec);

    if (retc == SFS_ERROR)
    {
      // Error occured, free memory
      xcastor_static_err("error while sending the async request");
      DeleteReqResp(request, rh, respvec);
      return retc;
    }
    else if (retc > SFS_OK)
    {      
      // Free memory and stall the client 
      DeleteReqResp(request, rh, respvec);
      return SFS_STALL;
    }
  }
  catch (castor::exception::Communication e)
  {
    xcastor_static_debug("Communications error: %s", e.getMessage().str().c_str());
    error.setErrInfo(ECOMM, e.getMessage().str().c_str());
    return SFS_ERROR;
  }
  catch (castor::exception::Exception e)
  {
    xcastor_static_debug("% request exception: %s", opType.c_str(),
                         e.getMessage().str().c_str());
    error.setErrInfo(ECOMM, e.getMessage().str().c_str());
    return SFS_ERROR;
  }

  // Try to get the response, maybe we are lucky ...
  elem = 0;
  found = false;
  elem = XrdxCastor2FS->msCastorClient->GetResponse(user_id, found);

  if (elem)
  {
    // Response found, process it
    return ProcessResponse(error, elem, opType, reqInfo, respInfo);
  }
  else
  {
    if (found)
    {
      // Request found, but response not yet ready => stall the client
      xcastor_static_debug("%s request found, but response hasn't arrived", opType.c_str());
      return SFS_STALL;
    }
    else
    {
      // Request not found => return error to the client
      xcastor_static_err("%s request not found, although we were expecting it", opType.c_str());
      return SFS_ERROR;
    }
  }
}


//------------------------------------------------------------------------------
// Remove
//------------------------------------------------------------------------------
bool
XrdxCastor2Stager::Rm(XrdOucErrInfo& error,
                      uid_t          uid,
                      gid_t          gid,
                      const char*    path,
                      const char*    stagehost,
                      const char*    serviceclass)
{
  //const char* tident = error.getErrUser();
  XrdOucString ErrPrefix = "uid=";
  ErrPrefix += (int)uid;
  ErrPrefix += " gid=";
  ErrPrefix += (int)gid;
  ErrPrefix += " path=";
  ErrPrefix += path;
  ErrPrefix += " stagehost=";
  ErrPrefix += stagehost;
  ErrPrefix += " serviceclass=";
  ErrPrefix += serviceclass;
  struct stage_filereq requests[1];
  struct stage_fileresp* resp;
  struct stage_options Opts;
  int i;
  int nbresps;
  char* reqid;
  char errbuf[1024];
  Opts.stage_host    = (char*)stagehost;
  Opts.service_class = (char*)serviceclass;
  Opts.stage_version = 2;
  Opts.stage_port    = 0;
  requests[0].filename = (char*) path;
  stage_setid(uid, gid);
  stager_seterrbuf(errbuf, sizeof(errbuf));

  if (stage_rm(requests, 1, &resp, &nbresps, &reqid, &Opts) < 0)
  {
    if (serrno != 0)
    {
      xcastor_static_debug("error: %s", sstrerror(serrno));
      error.setErrInfo(ECOMM, sstrerror(serrno));
    }

    if (*errbuf)
    {
      xcastor_static_debug("err buffer: %s", errbuf);
      error.setErrInfo(ECOMM, errbuf);
    }

    return false;
  }
  else
  {
    xcastor_static_debug("Received %i stage_rm() responses.", nbresps);

    for (i = 0; i < nbresps; i++)
    {
      xcastor_static_debug("reqid=%s, rc=%i, msg=%s", reqid,
                           resp[i].errorCode, resp[i].errorMessage);

      if (resp[i].errorCode)
      {
        xcastor_static_debug("path=%s, reqid=%s, rc=%i, msg=%s",
                             path, reqid, resp[i].errorCode, resp[i].errorMessage);
        error.setErrInfo(EINVAL, resp[i].errorMessage);
        free_fileresp(resp, nbresps);
        return false;
      }
    }

    free_fileresp(resp, nbresps);
  }

  return true;
}


//------------------------------------------------------------------------------
// Update done
//------------------------------------------------------------------------------
bool
XrdxCastor2Stager::UpdateDone(XrdOucErrInfo& error,
                              const char*    path,
                              const char*    reqid,
                              const char*    fileid,
                              const char*    nameserver,
                              const char*    stagehost,
                              const char*    serviceclass)
{
  //const char* tident = error.getErrUser();
  xcastor_static_debug("path=%s, stagehost=%s, sericeclass=%s",
                       path, stagehost, serviceclass);
  castor::stager::SubRequest subReq;
  subReq.setId(atoll(reqid));
  castor::Services* cs2service = castor::BaseObject::services();

  if (!cs2service)
  {
    error.setErrInfo(ENOMEM, "get castor2 baseobject services");
    return false;
  }

  castor::IService* cs2iservice = cs2service->service("RemoteJobSvc", castor::SVC_REMOTEJOBSVC);
  castor::stager::IJobSvc* jobSvc;
  jobSvc = dynamic_cast<castor::stager::IJobSvc*>(cs2iservice);

  if (!cs2iservice)
  {
    error.setErrInfo(ENOMEM, "get castor2 remote job service");
    return false;
  }

  try
  {
    xcastor_static_debug("Calling getUpdateDone( %s ) for: %s ", reqid, path);
    jobSvc->getUpdateDone((unsigned long long) atoll(reqid),
                          (unsigned long long) atoll(fileid),
                          nameserver);
  }
  catch (castor::exception::Communication e)
  {
    xcastor_static_debug("Communications error: %s", e.getMessage().str().c_str());
    error.setErrInfo(ECOMM, e.getMessage().str().c_str());
    // delete cs2iservice;
    // delete cs2service;
    return false;
  }
  catch (castor::exception::Exception e)
  {
    xcastor_static_debug("Fatal exception: %s", e.getMessage().str().c_str());
    error.setErrInfo(ECOMM, e.getMessage().str().c_str());
    // delete cs2iservice;
    // delete cs2service;
    return false;
  }

  // delete cs2iservice;
  // delete cs2service;
  return true;
}


//------------------------------------------------------------------------------
// First write
//------------------------------------------------------------------------------
bool
XrdxCastor2Stager::FirstWrite(XrdOucErrInfo& error,
                              const char*    path,
                              const char*    reqid,
                              const char*    fileid,
                              const char*    nameserver,
                              const char*    stagehost,
                              const char*    serviceclass)
{
  //const char* tident = error.getErrUser();
  xcastor_static_debug("path=%s, stagehost=%s, sericeclass=%s",
                       path, stagehost, serviceclass);
  castor::stager::SubRequest subReq;
  subReq.setId(atoll(reqid));
  castor::Services* cs2service = castor::BaseObject::services();

  if (!cs2service)
  {
    error.setErrInfo(ENOMEM, "get castor2 baseobject services");
    return false;
  }

  castor::IService* cs2iservice = cs2service->service("RemoteJobSvc",
                                  castor::SVC_REMOTEJOBSVC);
  castor::stager::IJobSvc* jobSvc;
  jobSvc = dynamic_cast<castor::stager::IJobSvc*>(cs2iservice);

  if (!cs2iservice)
  {
    error.setErrInfo(ENOMEM, "get castor2 remote job service");
    return false;
  }

  try
  {
    xcastor_static_debug("Calling firstByteWritten( %s ) for: %s ", reqid, path);
    jobSvc->firstByteWritten((unsigned long long) atoll(reqid),
                             (unsigned long long) atoll(fileid),
                             nameserver);
  }
  catch (castor::exception::Communication e)
  {
    xcastor_static_debug("Communications error: %s", e.getMessage().str().c_str());
    error.setErrInfo(ECOMM, e.getMessage().str().c_str());
    return false;
  }
  catch (castor::exception::Exception e)
  {
    xcastor_static_debug("Fatal exception: %s", e.getMessage().str().c_str());
    error.setErrInfo(ECOMM, e.getMessage().str().c_str());
    return false;
  }

  return true;
}


//------------------------------------------------------------------------------
// Stager query
//------------------------------------------------------------------------------
bool
XrdxCastor2Stager::StagerQuery(XrdOucErrInfo& error,
                               uid_t          uid,
                               gid_t          gid,
                               const char*    path,
                               const char*    stagehost,
                               const char*    serviceclass,
                               XrdOucString&  status)
{
  //const char* tident = error.getErrUser();
  xcastor_static_debug("uid=%i, gid=%i, path=%s, stagehost=%s, sericeclass=%s",
                       uid, gid, path, stagehost, serviceclass);
  XrdOucString ErrPrefix = "uid=";
  ErrPrefix += (int)uid;
  ErrPrefix += " gid=";
  ErrPrefix += (int)gid;
  ErrPrefix += " path=";
  ErrPrefix += path;
  ErrPrefix += " stagehost=";
  ErrPrefix += stagehost;
  ErrPrefix += " serviceclass=";
  ErrPrefix += serviceclass;
  struct stage_query_req requests[1];
  struct stage_filequery_resp* resp;
  int  nbresps, i;
  char errbuf[1024];
  struct stage_options Opts;
  errbuf[0] = 0;
  Opts.stage_host    = (char*)stagehost;
  Opts.service_class = (char*)serviceclass;
  Opts.stage_version = 2;
  Opts.stage_port    = 0;
  requests[0].type  = BY_FILENAME;
  requests[0].param = (char*) path;
  stager_seterrbuf(errbuf, sizeof(errbuf));
  stage_setid(uid, gid);

  if (stage_filequery(requests, 1, &resp, &nbresps, &Opts) < 0)
  {
    if (serrno != 0)
    {
      xcastor_static_debug("error: %s", sstrerror(serrno));
      error.setErrInfo(ECOMM, sstrerror(serrno));
    }

    if (*errbuf)
    {
      xcastor_static_debug("error buff: %s", errbuf);
      error.setErrInfo(ECOMM, errbuf);
    }

    return false;
  }
  else
  {
    xcastor_static_debug("Received %i stage_filequery() responses", nbresps);

    for (i = 0; i < nbresps; i++)
    {
      xcastor_static_debug("status=%s rc=%i, path=%s:%s ( %s ) id=%i",
                           stage_requestStatusName(resp[i].status), resp[i].errorCode,
                           resp[i].diskserver, resp[i].filename,
                           resp[i].castorfilename);
      status = stage_fileStatusName(resp[i].status);

      if (*resp[i].castorfilename)
      {
        if (!resp[i].errorCode)
        {
          xcastor_static_debug("File status: %s for path: %s", status.c_str(), path);
        }
        else
        {
          xcastor_static_debug("Error for path: %s", path);
          free_filequery_resp(resp, nbresps);
          error.setErrInfo(EBUSY, "file is not staged in requested stager");
          return false;
        }
      }
      else
      {
        status = "NA";
      }
    }

    free_filequery_resp(resp, nbresps);
  }

  return true;
}


//--------------------------------------------------------------------------
// Process response received form the stager
//--------------------------------------------------------------------------
int 
XrdxCastor2Stager::ProcessResponse(XrdOucErrInfo& error,
                                   struct xcastor::XrdxCastorClient::ReqElement*& respElem,
                                   const std::string& opType,
                                   struct ReqInfo* reqInfo,
                                   struct RespInfo& respInfo)
{
  if (respElem->mRespVec->size() <= 0)
  {
    xcastor_static_debug("No response for %s for path %s", 
                         opType.c_str(), reqInfo->mPath);
    error.setErrInfo(ECOMM, "No response from stager ");
    delete respElem;
    return SFS_ERROR;
  }

  // Proccess the response
  castor::rh::IOResponse* fr =
    dynamic_cast<castor::rh::IOResponse*>(respElem->mRespVec->at(0));

  if (0 == fr)
  {
    xcastor_static_debug("Invalid response object for %s for path %s", 
                         opType.c_str(), reqInfo->mPath);
    error.setErrInfo(ECOMM, "Invalid response object");
    delete respElem;
    return SFS_ERROR;
  }

  if (fr->errorCode())
  {
    std::stringstream sstr;
    sstr.str(std::string());
    sstr << "uid=" << (int)reqInfo->mUid << " gid=" << (int)reqInfo->mGid 
         << " path=" << reqInfo->mPath << " stagehost=" << reqInfo->mStageHost 
         << " serviceclass=" << reqInfo->mServiceClass;
    XrdOucString ErrPrefix = sstr.str().c_str();

    xcastor_static_debug("received error errc=%i, errmsg=%s\%i, subreqid=%s, reqid=%s",
                         fr->errorCode(), fr->errorMessage().c_str(),
                         ErrPrefix.c_str(), fr->subreqId().c_str(),
                         fr->reqAssociated().c_str());

    sstr.str(std::string());
    sstr << "received error errc=" << (int)fr->errorCode() << " errmsg=\""
         << fr->errorMessage() << "\" " << ErrPrefix.c_str() << "subreqid="
         << fr->subreqId() << " reqid=" << fr->reqAssociated();
    XrdOucString emsg = sstr.str().c_str();
    error.setErrInfo(fr->errorCode(), emsg.c_str());
    delete respElem;
    return SFS_ERROR;
  }
  else
  {
    xcastor_static_debug("%s rc=%i, path=%s:%s ( %s ) id=%i",
                         stage_requestStatusName(fr->status()), fr->errorCode(),
                         fr->server().c_str(), fr->fileName().c_str(),
                         fr->castorFileName().c_str(), fr->id());
  }

  respInfo.mStageStatus = stage_requestStatusName(fr->status());
  respInfo.mRedirectionHost = fr->server().c_str();
  respInfo.mRedirectionPfn1 = fr->fileName().c_str();
 
  char sid[4096];
  sprintf(sid, "%llu", fr->id());
  respInfo.mRedirectionPfn2 = sid;  // request id
  respInfo.mRedirectionPfn2 += ":";
  respInfo.mRedirectionPfn2 += reqInfo->mStageHost;
  respInfo.mRedirectionPfn2 += ":";
  respInfo.mRedirectionPfn2 += reqInfo->mServiceClass;
  // Attach the port for the local host connection on a diskserver to talk with stagerJob
  respInfo.mRedirectionPfn2 += ":";
  respInfo.mRedirectionPfn2 += fr->port();
  // Attach the sub req ID needed to identity the local host connection
  respInfo.mRedirectionPfn2 += ":";
  respInfo.mRedirectionPfn2 += fr->subreqId().c_str();

  delete respElem;
  return SFS_OK;
}


//------------------------------------------------------------------------------
// Get delay value
//------------------------------------------------------------------------------
int
XrdxCastor2Stager::GetDelayValue(const char* tag)
{
  XrdOucString* delayval;
  XrdSysRWLockHelper rd_lock(msLockDelay);

  if ((delayval = msDelayStore->Find(tag)))
  {
    float oldval = atoi(delayval->c_str());
    oldval *= 1.8;

    if (oldval > 3600)
    {
      // More than 1 hour, doesn't make sense
      oldval = 3600;
    }

    // We double always the delay value
    (*delayval) = "";
    *(delayval) += (int) oldval;
  }
  else
  {
    delayval = new XrdOucString();
    *delayval = 5 + (rand() % 5);
    msDelayStore->Add(tag, delayval, 3600);
  }

  return atoi(delayval->c_str());
}


//------------------------------------------------------------------------------
// Drop delay tag from mapping
//------------------------------------------------------------------------------
void
XrdxCastor2Stager::DropDelayTag(const char* tag)
{
  XrdSysRWLockHelper wr_lock(msLockDelay, 0);
  msDelayStore->Del(tag);
}

