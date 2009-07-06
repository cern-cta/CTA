//          $Id: XrdxCastor2Stager.cc,v 1.9 2009/07/06 08:27:11 apeters Exp $

#ifndef __XCASTOR2FS__STAGER__HH
#define __XCASTOR2FS__STAGER__HH

#include <sys/types.h>
#include <iostream>

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

#include <sys/types.h>
#include <XrdxCastor2Fs/XrdxCastor2Stager.hh>

#include <XrdxCastor2Fs/XrdxCastor2Trace.hh>

#include <XrdOuc/XrdOucTrace.hh>


extern XrdOucTrace xCastor2FsTrace;

bool
XrdxCastor2Stager::Prepare2Get(XrdOucErrInfo &error, uid_t uid, gid_t gid, const char* path, const char* stagehost, const char* serviceclass, XrdOucString &redirectionhost, XrdOucString &redirectionpfn, XrdOucString &status) 
{
  EPNAME("Prepare2Get");
  const char* tident = error.getErrUser();
  struct stage_options                  Opts;
  ZTRACE(stager,"uid=" << uid << " gid=" << gid << " path=" << path << " stagehost=" << stagehost << " serviceclass=" << serviceclass);
  
  const char* stageTag = "xCastor2P2Get";
  
  castor::stager::StagePrepareToGetRequest  getReq_ro;
  castor::stager::SubRequest      *subreq = new castor::stager::SubRequest();
  std::vector<castor::rh::Response *>respvec;
  castor::client::VectorResponseHandler rh(&respvec);
  int mask = umask(0);
  umask(mask);
  
  getReq_ro.setUserTag(stageTag);
  getReq_ro.setMask(mask);
  getReq_ro.addSubRequests(subreq);
  subreq->setRequest(&getReq_ro);
  subreq->setProtocol(std::string("xroot"));
  subreq->setFileName(std::string(path));
  subreq->setModeBits(0744);

  castor::client::BaseClient       cs2client(stage_getClientTimeout(),-1);

  Opts.stage_host    = (char*)stagehost;
  Opts.service_class = (char*)serviceclass;
  Opts.stage_version = 2;
  Opts.stage_port    = 0;

  cs2client.setOptions(&Opts);
  cs2client.setAuthorizationId(uid,gid);
  
  try   {
    ZTRACE(stager,"Sending Prepare2Get path=" <<path << " uid=" << uid << " gid=" << gid << " stagehost=" << stagehost << " svc=" <<serviceclass);
    std::string reqid = cs2client.sendRequest(&getReq_ro, &rh);
  }
  catch (castor::exception::Communication e) {
    TRACES("Communications error:" << e.getMessage().str().c_str());
    error.setErrInfo(ECOMM,e.getMessage().str().c_str());
    delete subreq;
    return false;
  }
  catch (castor::exception::Exception e) {
    TRACES("sendRequest exception:"<< e.getMessage().str().c_str());
    error.setErrInfo(ECOMM,e.getMessage().str().c_str());
    delete subreq;
    return false;
  }
  
  if (respvec.size() <= 0){
    TRACES("No response for prepare2get " << path );
    error.setErrInfo(ECOMM,"No response");
    delete subreq;
    return false;
  }
    
  // Proccess the response.
  //
  castor::rh::IOResponse* fr =
    dynamic_cast<castor::rh::IOResponse*>(respvec[0]);
  if (0 == fr) {
    TRACES("Invalid response object for prepare2get " << path);
    error.setErrInfo(ECOMM,"Invalid response object for prepare2get");
    delete subreq;
    return false;
  }
  
  if (fr->errorCode()) {
    TRACES("Got error code err=" << fr->errorCode());
    XrdOucString emsg = "received error code errc=";
    emsg += (int)fr->errorCode();
    error.setErrInfo(fr->errorCode(),emsg.c_str());
    delete subreq;
    delete respvec[0];
    return false;
  }  else {
    ZTRACE(stager, stage_requestStatusName(fr->status())<< "status=" << fr->status() <<" rc=" << fr->errorCode()
	   <<" path=" <<fr->server().c_str() <<':' <<fr->fileName().c_str()
	   <<" (" <<fr->castorFileName().c_str() <<") id=" <<fr->id() );
  }
  
  redirectionhost = fr->server().c_str();
  redirectionpfn  = fr->fileName().c_str();
  status          = stage_requestStatusName(fr->status());
  
  delete subreq;
  delete respvec[0];

  return true;
}


bool
XrdxCastor2Stager::Get(XrdOucErrInfo &error, uid_t uid, gid_t gid,const char* path, const char* stagehost, const char* serviceclass, XrdOucString &redirectionhost, XrdOucString &redirectionpfn, XrdOucString &redirectionpfn2, XrdOucString &status) 
{
  EPNAME("Get");
  const char* tident = error.getErrUser();
  struct stage_options                  Opts;

  ZTRACE(stager,"uid=" << uid << " gid=" << gid << " path=" << path << " stagehost=" << stagehost << " serviceclass=" << serviceclass);

  const char* stageTag = "xCastor2Get";
  
  castor::stager::StageGetRequest  getReq_ro;
  castor::stager::SubRequest      *subreq = new castor::stager::SubRequest();
  std::vector<castor::rh::Response *>respvec;
  castor::client::VectorResponseHandler rh(&respvec);
  int mask = umask(0);
  umask(mask);
  
  getReq_ro.setUserTag(stageTag);
  getReq_ro.setMask(mask);
  getReq_ro.addSubRequests(subreq);
  subreq->setRequest(&getReq_ro);
  subreq->setProtocol(std::string("xroot"));
  subreq->setFileName(std::string(path));
  subreq->setModeBits(0744);

  castor::client::BaseClient       cs2client(stage_getClientTimeout(),-1);
  Opts.stage_host    = (char*)stagehost;
  Opts.service_class = (char*)serviceclass;
  Opts.stage_version = 2;
  Opts.stage_port    = 0;
  cs2client.setOptions(&Opts);
  cs2client.setAuthorizationId(uid,gid);
  
  try   {
    ZTRACE(stager,"Sending Get path=" <<path << " uid=" << uid << " gid=" << gid << " stagehost=" << stagehost << " svc=" <<serviceclass);
    std::string reqid = cs2client.sendRequest(&getReq_ro, &rh);
  }
  catch (castor::exception::Communication e) {
    TRACES("Communications error:" << e.getMessage().str().c_str());
    error.setErrInfo(ECOMM,e.getMessage().str().c_str());
    delete subreq;
    return false;
  }
  catch (castor::exception::Exception e) {
    TRACES("sendRequest exception:"<< e.getMessage().str().c_str());
    error.setErrInfo(ECOMM,e.getMessage().str().c_str());
    delete subreq;
    return false;
  }
  
  if (respvec.size() <= 0){
    TRACES("No response for get " << path );
    error.setErrInfo(ECOMM,"No response");
    delete subreq;
    return false;
  }
    
  // Proccess the response.
  //
  castor::rh::IOResponse* fr =
    dynamic_cast<castor::rh::IOResponse*>(respvec[0]);
  if (0 == fr) {
    TRACES("Invalid response object for get " << path);
    error.setErrInfo(ECOMM,"Invalid response object for get");
    delete subreq;
    return false;
  }
  
  if (fr->errorCode()) {
    TRACES("Got error code err=" << fr->errorCode());
    XrdOucString emsg = "received error code errc=";
    emsg += (int)fr->errorCode();
    error.setErrInfo(fr->errorCode(),emsg.c_str());
    delete subreq;
    delete respvec[0];
    return false;
  }  else {
    ZTRACE(stager, stage_fileStatusName(fr->status()) <<" rc=" << fr->errorCode()
	   <<" path=" <<fr->server().c_str() <<':' <<fr->fileName().c_str()
	   <<" (" <<fr->castorFileName().c_str() <<") id=" <<fr->id() );
  }
  
  redirectionhost = fr->server().c_str();
  redirectionpfn  = fr->fileName().c_str();
  status          = stage_fileStatusName(fr->status());

  char sid[4096];
  sprintf(sid,"%ld",fr->id());
  redirectionpfn2 = sid;  // request id
  redirectionpfn2 += ":"; redirectionpfn2 += stagehost;
  redirectionpfn2 += ":"; redirectionpfn2 += serviceclass;
  // attach the port for the local host connection on a diskserver to talk with stagerJob
  redirectionpfn2 += ":"; redirectionpfn2 += fr->port();
  // attach the sub req ID needed to identity the local host connection
  redirectionpfn2 += ":"; redirectionpfn2 += fr->subreqId().c_str();  
  delete subreq;
  delete respvec[0];

  return true;
}

bool
XrdxCastor2Stager::Put(XrdOucErrInfo &error, uid_t uid, gid_t gid,const char* path, const char* stagehost, const char* serviceclass, XrdOucString &redirectionhost, XrdOucString &redirectionpfn, XrdOucString &redirectionpfn2, XrdOucString &status) 
{
  EPNAME("Put");
  const char* tident = error.getErrUser();
  struct stage_options                  Opts;

  ZTRACE(stager,"uid=" << uid << " gid=" << gid << " path=" << path << " stagehost=" << stagehost << " serviceclass=" << serviceclass);
  
  const char* stageTag = "xCastor2Put";
  
  castor::stager::StagePutRequest  putReq_rw;
  castor::stager::SubRequest      *subreq = new castor::stager::SubRequest();
  std::vector<castor::rh::Response *>respvec;
  castor::client::VectorResponseHandler rh(&respvec);
  int mask = umask(0);
  umask(mask);
  
  putReq_rw.setUserTag(stageTag);
  putReq_rw.setMask(mask);
  putReq_rw.addSubRequests(subreq);
  subreq->setRequest(&putReq_rw);
  subreq->setProtocol(std::string("xroot"));
  subreq->setFileName(std::string(path));
  subreq->setModeBits(0744);

  castor::client::BaseClient       cs2client(stage_getClientTimeout(),-1);
  Opts.stage_host    = (char*)stagehost;
  Opts.service_class = (char*)serviceclass;
  Opts.stage_version = 2;
  Opts.stage_port    = 0;
  cs2client.setOptions(&Opts);
  cs2client.setAuthorizationId(uid,gid);
  
  try   {
    ZTRACE(stager,"Sending Put path=" <<path << " uid=" << uid << " gid=" << gid << " stagehost=" << stagehost << " svc=" <<serviceclass);
    std::string reqid = cs2client.sendRequest(&putReq_rw, &rh);
  }
  catch (castor::exception::Communication e) {
    TRACES("Communications error:" << e.getMessage().str().c_str());
    error.setErrInfo(ECOMM,e.getMessage().str().c_str());
    delete subreq;
    return false;
  }
  catch (castor::exception::Exception e) {
    TRACES("sendRequest exception:"<< e.getMessage().str().c_str());
    error.setErrInfo(ECOMM,e.getMessage().str().c_str());
    delete subreq;
    return false;
  }
  
  if (respvec.size() <= 0){
    TRACES("No response for put " << path );
    error.setErrInfo(ECOMM,"No response");
    delete subreq;
    return false;
  }
    
  // Proccess the response.
  //
  castor::rh::IOResponse* fr =
    dynamic_cast<castor::rh::IOResponse*>(respvec[0]);
  if (0 == fr) {
    TRACES("Invalid response object for put " << path);
    error.setErrInfo(ECOMM,"Invalid response object for put");
    delete subreq;
    return false;
  }
  
  if (fr->errorCode()) {
    TRACES("Got error code err=" << fr->errorCode());
    XrdOucString emsg = "received error code errc=";
    emsg += (int)fr->errorCode();
    error.setErrInfo(fr->errorCode(),emsg.c_str());
    delete subreq;
    delete respvec[0];
    return false;
  }  else {
    ZTRACE(stager, stage_fileStatusName(fr->status()) <<" rc=" << fr->errorCode()
	   <<" path=" <<fr->server().c_str() <<':' <<fr->fileName().c_str()
	   <<" (" <<fr->castorFileName().c_str() <<") id=" <<fr->id() );
  }
  
  redirectionhost = fr->server().c_str();
  redirectionpfn  = fr->fileName().c_str();
  char sid[4096];
  sprintf(sid,"%ld",fr->id());
  redirectionpfn2 = sid;  // request id
  redirectionpfn2 += ":"; redirectionpfn2 += stagehost;
  redirectionpfn2 += ":"; redirectionpfn2 += serviceclass;
  // attach the port for the local host connection on a diskserver to talk with stagerJob
  redirectionpfn2 += ":"; redirectionpfn2 += fr->port();
  // attach the sub req ID needed to identity the local host connection
  redirectionpfn2 += ":"; redirectionpfn2 += fr->subreqId().c_str();

  status          = stage_fileStatusName(fr->status());
  
  delete subreq;
  delete respvec[0];
  
  return true;
}


bool 
XrdxCastor2Stager::Rm(XrdOucErrInfo & error, uid_t uid, gid_t gid, const char* path, const char* stagehost, const char* serviceclass )
{
  EPNAME("Rm");
  const char* tident = error.getErrUser();

  struct stage_filereq requests[1];
  struct stage_fileresp *resp;
  struct stage_options                  Opts;

  int i;
  int nbresps;
  char *reqid;
  char errbuf[1024];
  int errflg, rc, ret;
  struct stage_options opts;

  Opts.stage_host    = (char*)stagehost;
  Opts.service_class = (char*)serviceclass;
  Opts.stage_version = 2;
  Opts.stage_port    = 0;


  requests[0].filename = (char *) path;

  stager_seterrbuf(errbuf, sizeof(errbuf));

  if (stage_rm(requests, 1, &resp, &nbresps, &reqid, &Opts) < 0) {
    if (serrno != 0) {
      TRACES(sstrerror(serrno));
      error.setErrInfo(ECOMM,sstrerror(serrno));
    }
    if (*errbuf) {
      TRACES(errbuf);
      error.setErrInfo(ECOMM,errbuf);
    }
    return false;
  } else {
    ZTRACE(stager,"Received " <<nbresps <<" stage_rm() responses");
    for (i = 0; i < nbresps; i++) {
      ZTRACE(stager,"reqid = " << reqid << " rc=" << resp[i].errorCode <<" msg=" <<resp[i].errorMessage);
      if (resp[i].errorCode) {
	TRACES("Error: path=" << path << " reqid = " << reqid << " rc=" << resp[i].errorCode <<" msg=" <<resp[i].errorMessage);
	error.setErrInfo(EINVAL,resp[i].errorMessage);
	free_fileresp(resp, nbresps);
	return false;
      } 
    }
    free_fileresp(resp, nbresps);
  }

  return true;
}

bool
XrdxCastor2Stager::Update(XrdOucErrInfo &error, uid_t uid, gid_t gid,const char* path, const char* stagehost, const char* serviceclass, XrdOucString &redirectionhost, XrdOucString &redirectionpfn, XrdOucString &redirectionpfn2, XrdOucString &status) 
{
  EPNAME("Update");
  const char* tident = error.getErrUser();
  struct stage_options                  Opts;

  ZTRACE(stager,"uid=" << uid << " gid=" << gid << " path=" << path << " stagehost=" << stagehost << " serviceclass=" << serviceclass);
  
  const char* stageTag = "xCastor2Update";
  
  castor::stager::StageUpdateRequest  putReq_rw;
  castor::stager::SubRequest      *subreq = new castor::stager::SubRequest();
  std::vector<castor::rh::Response *>respvec;
  castor::client::VectorResponseHandler rh(&respvec);
  int mask = umask(0);
  umask(mask);
  
  putReq_rw.setUserTag(stageTag);
  putReq_rw.setMask(mask);
  putReq_rw.addSubRequests(subreq);
  subreq->setRequest(&putReq_rw);
  subreq->setProtocol(std::string("xroot"));
  subreq->setFileName(std::string(path));
  subreq->setModeBits(0744);

  castor::client::BaseClient       cs2client(stage_getClientTimeout(),-1);
  Opts.stage_host    = (char*)stagehost;
  Opts.service_class = (char*)serviceclass;
  Opts.stage_version = 2;
  Opts.stage_port    = 0;
  cs2client.setOptions(&Opts);
  cs2client.setAuthorizationId(uid,gid);
  
  try   {
    ZTRACE(stager,"Sending Update path=" <<path << " uid=" << uid << " gid=" << gid << " stagehost=" << stagehost << " svc=" <<serviceclass);
    std::string reqid = cs2client.sendRequest(&putReq_rw, &rh);
  }
  catch (castor::exception::Communication e) {
    TRACES("Communications error:" << e.getMessage().str().c_str());
    error.setErrInfo(ECOMM,e.getMessage().str().c_str());
    delete subreq;
    return false;
  }
  catch (castor::exception::Exception e) {
    TRACES("sendRequest exception:"<< e.getMessage().str().c_str());
    error.setErrInfo(ECOMM,e.getMessage().str().c_str());
    delete subreq;
    return false;
  }
  
  if (respvec.size() <= 0){
    TRACES("No response for update " << path );
    error.setErrInfo(ECOMM,"No response");
    delete subreq;
    return false;
  }
    
  // Proccess the response.
  //
  castor::rh::IOResponse* fr =
    dynamic_cast<castor::rh::IOResponse*>(respvec[0]);
  if (0 == fr) {
    TRACES("Invalid response object for update " << path);
    error.setErrInfo(ECOMM,"Invalid response object for update");
    delete subreq;
    return false;
  }
  
  if (fr->errorCode()) {
    TRACES("Got error code err=" << fr->errorCode());
    XrdOucString emsg = "received error code errc=";
    emsg += (int)fr->errorCode();
    error.setErrInfo(fr->errorCode(),emsg.c_str());   
    delete subreq;
    delete respvec[0];
    return false;
  }  else {
    ZTRACE(stager, stage_fileStatusName(fr->status()) <<" rc=" << fr->errorCode()
	   <<" path=" <<fr->server().c_str() <<':' <<fr->fileName().c_str()
	   <<" (" <<fr->castorFileName().c_str() <<") id=" <<fr->id() );
  }
  
  redirectionhost = fr->server().c_str();
  redirectionpfn  = fr->fileName().c_str();
  char sid[4096];
  sprintf(sid,"%ld",fr->id());
  redirectionpfn2 = sid;  // request id
  redirectionpfn2 += ":"; redirectionpfn2 += stagehost;
  redirectionpfn2 += ":"; redirectionpfn2 += serviceclass;
  
  status          = stage_fileStatusName(fr->status());
  
  delete subreq;
  delete respvec[0];
  
  return true;
}

bool 
XrdxCastor2Stager::PrepareForMigration(XrdOucErrInfo &error, const char* path,  const char* reqid, const char* fileid, const char* nameserver, const char* stagehost, const char* serviceclass, const char* filesize, const char* mtime ) 
{
  EPNAME("PrepareForMigration");
  const char* tident = error.getErrUser();

  ZTRACE(stager,"reqid=" << reqid << " fileid=" << fileid << " path=" << path << " stagehost=" << stagehost << " serviceclass=" << serviceclass << " filesize=" << filesize << " mtime=" << mtime);

  castor::stager::SubRequest subReq;
  subReq.setId(atol(reqid));

  castor::Services *cs2service = castor::BaseObject::services();

  if (!cs2service) {
    error.setErrInfo(ENOMEM,"get castor2 baseobject services");
    return false; 
  }

  ZTRACE(stager,"PrepareForMigration cs2service");
 
  castor::IService *cs2iservice = cs2service->service("RemoteJobSvc",castor::SVC_REMOTEJOBSVC);

  ZTRACE(stager,"PrepareForMigratino got cs2iservice");

  if (!cs2iservice) {
    ZTRACE(stager,"No cs2iservice");
    return false;
  }
  castor::stager::IJobSvc *jobSvc;

  ZTRACE(stager,"PrepareForMigratino Cast");
  jobSvc = dynamic_cast<castor::stager::IJobSvc*>(cs2iservice);

  ZTRACE(stager,"PrepareForMigration cs2iservice");
  if (!cs2iservice) {
    error.setErrInfo(ENOMEM,"get castor2 remote job service");
    return false;
  }
  
  ZTRACE(stager,"Try PrepareForMigration");
  try {
    ZTRACE(stager, "Calling prepareForMigration(" <<reqid <<',' <<filesize <<") for " <<path);
    jobSvc->prepareForMigration(&subReq, (unsigned long long)atoll(filesize), (unsigned long long)atoll(mtime), (unsigned long long) atoll(fileid), nameserver);
  }
  
  catch (castor::exception::Communication e) {
    TRACES("Communications error;"<<e.getMessage().str().c_str());
    error.setErrInfo(ECOMM,e.getMessage().str().c_str());
    // delete cs2iservice;
    //    delete cs2service;
    return false;
  }
  
  catch (castor::exception::Exception e) {
    TRACES("Fatal exception;"<<e.getMessage().str().c_str());
    error.setErrInfo(ECOMM,e.getMessage().str().c_str());
    // delete cs2iservice;
    //    delete cs2service;
    return false;
    }

  // delete cs2iservice;
  //  delete cs2service;

  return true;
}

bool 
XrdxCastor2Stager::UpdateDone(XrdOucErrInfo &error, const char* path,  const char* reqid, const char* fileid, const char* nameserver, const char* stagehost, const char* serviceclass)
{
  EPNAME("UpdateDone");
  const char* tident = error.getErrUser();

  ZTRACE(stager,"reqid=" << reqid << " fileid=" << fileid << " path=" << path << " stagehost=" << stagehost << " serviceclass=" << serviceclass);

  castor::stager::SubRequest subReq;
  subReq.setId(atoll(reqid));

  castor::Services *cs2service = castor::BaseObject::services();

  if (!cs2service) {
    error.setErrInfo(ENOMEM,"get castor2 baseobject services");
    return false; 
  }

 
  castor::IService *cs2iservice = cs2service->service("RemoteJobSvc",castor::SVC_REMOTEJOBSVC);

  castor::stager::IJobSvc *jobSvc;

  jobSvc = dynamic_cast<castor::stager::IJobSvc*>(cs2iservice);

  if (!cs2iservice) {
    error.setErrInfo(ENOMEM,"get castor2 remote job service");
    return false;
  }

  try {
    ZTRACE(stager, "Calling getUpdateDone(" <<reqid <<") for " <<path);
    jobSvc->getUpdateDone((unsigned long long) atoll(reqid),(unsigned long long) atoll(fileid), nameserver);
  }

  catch (castor::exception::Communication e) {
    TRACES("Communications error;"<<e.getMessage().str().c_str());
    error.setErrInfo(ECOMM,e.getMessage().str().c_str());
    // delete cs2iservice;
    //    delete cs2service;
    return false;
  }
  
  catch (castor::exception::Exception e) {
    TRACES("Fatal exception;"<<e.getMessage().str().c_str());
    error.setErrInfo(ECOMM,e.getMessage().str().c_str());
    // delete cs2iservice;
    //    delete cs2service;
    return false;
    }

  // delete cs2iservice;
  //  delete cs2service;

  return true;
}

bool 
XrdxCastor2Stager::FirstWrite(XrdOucErrInfo &error, const char* path,  const char* reqid, const char* fileid, const char* nameserver, const char* stagehost, const char* serviceclass)
{
  EPNAME("FirstWrite");
  const char* tident = error.getErrUser();

  ZTRACE(stager,"reqid=" << reqid << " fileid=" << fileid << " path=" << path << " stagehost=" << stagehost << " serviceclass=" << serviceclass);

  castor::stager::SubRequest subReq;
  subReq.setId(atoll(reqid));

  castor::Services *cs2service = castor::BaseObject::services();

  if (!cs2service) {
    error.setErrInfo(ENOMEM,"get castor2 baseobject services");
    return false; 
  }

 
  castor::IService *cs2iservice = cs2service->service("RemoteJobSvc",castor::SVC_REMOTEJOBSVC);

  castor::stager::IJobSvc *jobSvc;

  jobSvc = dynamic_cast<castor::stager::IJobSvc*>(cs2iservice);

  if (!cs2iservice) {
    error.setErrInfo(ENOMEM,"get castor2 remote job service");
    return false;
  }

  try {
    ZTRACE(stager, "Calling firstByteWritten(" <<reqid <<") for " <<path);
    jobSvc->firstByteWritten((unsigned long long) atoll(reqid),(unsigned long long) atoll(fileid), nameserver);
  }

  catch (castor::exception::Communication e) {
    TRACES("Communications error;"<<e.getMessage().str().c_str());
    error.setErrInfo(ECOMM,e.getMessage().str().c_str());
    // delete cs2iservice;
    //    delete cs2service;
    return false;
  }
  
  catch (castor::exception::Exception e) {
    TRACES("Fatal exception;"<<e.getMessage().str().c_str());
    error.setErrInfo(ECOMM,e.getMessage().str().c_str());
    // delete cs2iservice;
    //    delete cs2service;
    return false;
    }

  // delete cs2iservice;
  //  delete cs2service;

  return true;
}

bool 
XrdxCastor2Stager::PutFailed(XrdOucErrInfo &error, const char* path,  const char* reqid, const char* fileid, const char* nameserver, const char* stagehost, const char* serviceclass)
{
  EPNAME("PutFailed");
  const char* tident = error.getErrUser();

  ZTRACE(stager,"reqid=" << reqid << " fileid=" << fileid << " path=" << path << " stagehost=" << stagehost << " serviceclass=" << serviceclass);

  castor::stager::SubRequest subReq;
  subReq.setId(atoll(reqid));

  castor::Services *cs2service = castor::BaseObject::services();

  if (!cs2service) {
    error.setErrInfo(ENOMEM,"get castor2 baseobject services");
    return false; 
  }

 
  castor::IService *cs2iservice = cs2service->service("RemoteJobSvc",castor::SVC_REMOTEJOBSVC);

  castor::stager::IJobSvc *jobSvc;

  jobSvc = dynamic_cast<castor::stager::IJobSvc*>(cs2iservice);

  if (!cs2iservice) {
    error.setErrInfo(ENOMEM,"get castor2 remote job service");
    return false;
  }

  try {
    ZTRACE(stager, "Calling PutFailed(" <<reqid <<") for " <<path);
    jobSvc->putFailed((unsigned long long) atoll(reqid),(unsigned long long) atoll(fileid), nameserver);
  }

  catch (castor::exception::Communication e) {
    TRACES("Communications error;"<<e.getMessage().str().c_str());
    error.setErrInfo(ECOMM,e.getMessage().str().c_str());
    // delete cs2iservice;
    //    delete cs2service;
    return false;
  }
  
  catch (castor::exception::Exception e) {
    TRACES("Fatal exception;"<<e.getMessage().str().c_str());
    error.setErrInfo(ECOMM,e.getMessage().str().c_str());
    // delete cs2iservice;
    //    delete cs2service;
    return false;
    }

  // delete cs2iservice;
  //  delete cs2service;

  return true;
}


bool 
XrdxCastor2Stager::StagerQuery(XrdOucErrInfo &error, uid_t uid, gid_t gid, const char* path, const char* stagehost, const char* serviceclass, XrdOucString &status) 
{
  EPNAME("StagerQuery");
  const char* tident = error.getErrUser();

  ZTRACE(stager,"uid=" << uid << " gid=" << gid << " path=" << path << " stagehost=" << stagehost << " serviceclass=" << serviceclass);
  struct stage_query_req       requests[1];  
  struct stage_filequery_resp  *resp;
  int                           nbresps, i;
  char errbuf[1024];
  struct stage_options                  Opts;
  errbuf[0] = 0;

  Opts.stage_host    = (char*)stagehost;
  Opts.service_class = (char*)serviceclass;
  Opts.stage_version = 2;
  Opts.stage_port    = 0;

  
  requests[0].type  = BY_FILENAME;
  requests[0].param = (char *) path;

  stager_seterrbuf(errbuf, sizeof(errbuf));

  if (stage_filequery(requests, 1, &resp, &nbresps, &Opts) < 0) {
    if (serrno != 0) {
      TRACES(sstrerror(serrno));
      error.setErrInfo(ECOMM,sstrerror(serrno));
    }
    if (*errbuf) {
      TRACES(errbuf);
      error.setErrInfo(ECOMM,errbuf);
    }
    return false;
  } else {
    ZTRACE(stager,"Received " <<nbresps <<" stage_filequery() responses");
    for (i = 0; i < nbresps; i++) {
      ZTRACE(stager,"status=" << stage_fileStatusName(resp[i].status) <<" rc=" <<
	     resp[i].errorCode <<" path=" <<resp[i].diskserver <<':'
	     <<resp[i].filename <<" (" <<resp[i].castorfilename <<')');
      status = stage_fileStatusName(resp[i].status);      

      if (*resp[i].castorfilename) {
	if (!resp[i].errorCode) {
	  ZTRACE(stager,"File status: " << status.c_str() << " for path: " << path);
	} else {
	  TRACES("Error for path:" << path );
	  free_filequery_resp(resp, nbresps);
	  error.setErrInfo(EBUSY,"file is not staged in requested stager");
	  return false;
	}
      } else {
	status = "NA";
      }
    }
    free_filequery_resp(resp, nbresps);
  }
  return true;
}

int 
XrdxCastor2Stager::GetDelayValue(const char* tag) 
{
  XrdOucString *delayval;

  if ( (delayval=delaystore->Find(tag)) ) {
    float oldval = atoi(delayval->c_str());
    oldval *= 1.8;
    if (oldval > 3600 ) {
      // more than 1 hour, doesn't make sense
      oldval = 3600;
    }

    // we double always the delay value
    (*delayval) = "";
    *(delayval) += (int) oldval;
  } else {
    delayval = new XrdOucString();
    *delayval = 20 + (rand()% 20);
    delaystore->Add(tag,delayval,3600);
  }
  return atoi(delayval->c_str());
}

#endif
