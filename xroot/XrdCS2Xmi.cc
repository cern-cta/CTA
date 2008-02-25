/******************************************************************************/
/*                                                                            */
/*                          X r d C S 2 X m i . c c                           */
/*                                                                            */
/* (c) 2006 by the Board of Trustees of the Leland Stanford, Jr., University  */
/*                            All Rights Reserved                             */
/*   Produced by Andrew Hanushevsky for Stanford University under contract    */
/*              DE-AC02-76-SFO0515 with the Department of Energy              */
/******************************************************************************/

//         $Id: XrdCS2Xmi.cc,v 1.1 2008/02/25 15:15:46 apeters Exp $

const char *XrdCS2Xmi2csCVSID = "$Id: XrdCS2Xmi.cc,v 1.1 2008/02/25 15:15:46 apeters Exp $";

#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <vector>

#include "Cns_api.h"

#include "XrdCS2/XrdCS2Req.hh"
#include "XrdCS2/XrdCS2Xmi.hh"

#include "Xrd/XrdScheduler.hh"
#include "XrdSys/XrdSysError.hh"
#include "XrdOuc/XrdOucName2Name.hh"
#include "XrdSys/XrdSysTimer.hh"
#include "XrdSys/XrdSysPlatform.hh"
#define XRDOLBTRACETYPE ->
#include "XrdOlb/XrdOlbTrace.hh"
#include "XrdOlb/XrdOlbTypes.hh"
  
/******************************************************************************/
/*                         L o c a l   C l a s s e s                          */
/******************************************************************************/
  
namespace CS2Xmi
{
// This class is used to re-initialize the external initerface. It is always
// runasynchnrously by another thread is is scheduled whenever initialization
// is needed (e.g., after a communications failure).
//
class XmiInit : XrdJob
{
public:

void DoIt() {XmiP->InitXeq(); delete this;}

     XmiInit(XrdCS2Xmi *xp) : XrdJob("CS2 xmi init") {XmiP = xp;}
    ~XmiInit() {}

private:

XrdCS2Xmi *XmiP;
};

class XmiHelper
{
public:

const char *Path() {return xmiReq->Path();}

            XmiHelper(XrdOlbReq *reqP, const char *Path)
                        {xmiReq = XrdCS2Req::Alloc(reqP, Path);}
           ~XmiHelper() {xmiReq->Recycle();}
private:

XrdCS2Req *xmiReq;
};

struct PollArgs
       {XrdCS2Xmi  *xmiP;
        const char *UserTag;
        int         reqType;
        int         is_W;
       };

class XmiJob : XrdJob
{
public:

void DoIt() {
  if ((!strcmp(Type,"Prep2Put") || (!strcmp(Type,"Prep2Update")))) {
    XmiP->doPut(ReqP, Path); delete this;
  } else {
    XmiP->doGet(ReqP,Path);
    delete this;
  }
}

     XmiJob(XrdCS2Xmi *xp,  XrdOlbReq *reqp, const char *path, const char* type) : XrdJob("CS2 xmi put")
           {XmiP = xp; ReqP = reqp; Path = strdup(path); Type= strdup(type);}
    ~XmiJob() {if (Path) free(Path); if (Type) free(Type);}

private:

XrdCS2Xmi *XmiP;
XrdOlbReq *ReqP;
char      *Path;
char      *Type;
};
}

/******************************************************************************/
/*                               G l o b a l s                                */
/******************************************************************************/

const char   *XrdCS2Xmi::prepTag     = "OlbXmiPrep";
const char   *XrdCS2Xmi::stageTag    = "OlbXmiStage";
const char   *XrdCS2Xmi::stageTag_ww = "OlbXmiStageWW";

XrdSysError  *XrdCS2Xmi::eDest;
XrdInet      *XrdCS2Xmi::iNet;
XrdScheduler *XrdCS2Xmi::Sched ;
XrdOucTrace  *XrdCS2Xmi::Trace;

/******************************************************************************/
/*            E x t e r n a l   T h r e a d   I n t e r f a c e s             */
/******************************************************************************/
  
void *XrdCS2Xmi_StartPoll(void *parg)
{  
   CS2Xmi::PollArgs *pArgs = (CS2Xmi::PollArgs *)parg;

   pArgs->xmiP->MSSPoll(pArgs->reqType, pArgs->UserTag, pArgs->is_W);

   return (void *)0;
}

/******************************************************************************/
/*                          X r d O l b g e t X m i                           */
/******************************************************************************/

// We allow the the following on the xmi directive line after the library path:

// [host[:port]] [-v version] [-s sclass] [-n castorns]

// Where: host:port is the location of the Castor stager
//        version   is the version number (example, '-v3')
//        sclass    is the service class  (example, '-sfoo')
//        castorns  is the castor nameserver host (default is stager 'host')

extern "C"
{
XrdOlbXmi *XrdOlbgetXmi(int argc, char **argv, XrdOlbXmiEnv *XmiEnv)
{

// Get an Xmi Object and return it
//
   return (XrdOlbXmi *)(new XrdCS2Xmi(XmiEnv));
}
}
  
/******************************************************************************/
/*                           C o n s t r u c t o r                            */
/******************************************************************************/
  
XrdCS2Xmi::XrdCS2Xmi(XrdOlbXmiEnv *Env)
{
   char *tp, *Parms, *cp;

// Set pointers and variables to zero
//
   initDone     = 0;
   initActive   = 0;

// Copy out needed stuff from the environment
//
   eDest = Env->eDest;         // -> Error message handler
   iNet  = Env->iNet;          // -> Network object
   Sched = Env->Sched;         // -> Thread scheduler
   Trace = Env->Trace;         // -> Trace handler

// Prefill the opts structure (this is always the same)
//
   Opts.stage_host    = NULL;
   Opts.service_class = NULL;
   Opts.stage_version = 2;
   Opts.stage_port    = 0;
   castorns           = 0;

// Obtain the optional data (we ignore malformed parameters)
//
   if (Env->Parms && *(Env->Parms))
      {Parms = strdup(Env->Parms); tp = Parms;
       while(*tp && *tp == ' ') tp++;
       if (*tp && *tp != '-')
          {Opts.stage_host = tp; tp = index(tp, ' ');
           if ((cp = index(Opts.stage_host, ':')))
              {*cp = '\0'; Opts.stage_port = atoi(cp+1);}
          }
       if (tp && &*tp)
          do {if (*tp) while(*tp && *tp == ' ') tp++;
              if (*tp != '-') break;
                   if (*(tp+1) == 'v') Opts.stage_version = atoi(tp+2);
              else if (*(tp+1) == 's') Opts.service_class = tp+2;
              else if (*(tp+1) == 'n') castorns = tp+2;
              else break;
              while(*tp && *tp != ' ') tp++;
              if (*tp) {*tp = '\0'; tp++;}
             } while(*tp);
      }

// Propogate the setting to the request object
//
   stage_apiInit(&Tinfo);
   setDefaultOption(&Opts);
   XrdCS2Req::Set(Env);

// Schedule initialization of this object
//
   Init(0,1);
};

/******************************************************************************/
/*                                  I n i t                                   */
/******************************************************************************/
  
void XrdCS2Xmi::Init(int deltatime, int force)
{
  EPNAME("CS2Init");
  XrdJob *jp = (XrdJob *)new CS2Xmi::XmiInit(this);

// Turn off the initdone flag unless we are already running
//
   initMutex.Lock();
   if (force) initActive = 0;
      else if (initActive)
              {DEBUG("CS2 Init not scheduled; already started.");
               initMutex.UnLock();
               return;
              }
   initDone = 0;
   initMutex.UnLock();

// Schedule this initialization job as needed
//
   if (deltatime) Sched->Schedule(jp, time(0)+deltatime);
      else        Sched->Schedule(jp);
}
  
/******************************************************************************/
/*                               I n i t X e q                                */
/******************************************************************************/
  
void XrdCS2Xmi::InitXeq()
{
   EPNAME("InitXeq");
   static int PollerOK_R = 0;
   static CS2Xmi::PollArgs PA_rw = {this, stageTag,    BY_USERTAG_GETNEXT, 0};
// static CS2Xmi::PollArgs PA_ww = {this, stageTag_ww, BY_USERTAG,         1};
   castor::stager::SubRequest *subreq;
   pthread_t tid;
   int       retc, TimeOut;
   mode_t    mask;

// First we must verify that we haven't been initialized yet. There may be
// several threads that have noticed that re-initialization is needed.
//
   initMutex.Lock();
   if (initDone)
      {DEBUG("CS2 Init skipped; already initialized.");
       initMutex.UnLock(); 
       return;
      }

// Obtain our umask (sort of silly if you ask me)
//
   mask = umask(0);
   umask(mask);


// First off, tell the base object to stop logging
//
try {
   castor::BaseObject::initLog("", castor::SVC_NOMSG);
   TimeOut = stage_getClientTimeout();
   DEBUG("Castor timeout = " <<TimeOut);
  }

// We catch some common problems here. Note that a communications error
// will cause initialization to be retried later. In the mean time, requests
// will be told to wait until initialization completes
//
  catch (castor::exception::Communication e)
        {eDest->Emsg("cs2", "Communications error;", e.getMessage().str().c_str());
         Init(reinitTime, 1);
         initMutex.UnLock();
         return;
        }

// For real exceptions we have no choice but to terminate ourselves
//
  catch (castor::exception::Exception e)
        {eDest->Emsg("cs2","Fatal exception;",e.getMessage().str().c_str());
         _exit(128);
        }

// Start the thread that handles polling
//
   if (!PollerOK_R)
      if ((retc = XrdSysThread::Run(&tid, XrdCS2Xmi_StartPoll, (void *)&PA_rw,
                                     XRDSYSTHREAD_BIND, "cs2 r request polling")))
         eDest->Emsg("CS2Xmi", retc, "create read polling thread");
         else PollerOK_R = 1;

// Start the thread that handles polling (n/a as no responses to prepareToPut)
//
// if (!PollerOK_W)
//    if ((retc = XrdSysThread::Run(&tid, XrdCS2Xmi_StartPoll, (void *)&PA_ww,
//                                   XRDSYSTHREAD_BIND, "cs2 w request polling")))
//       eDest->Emsg("CS2Xmi", retc, "create write polling thread");
//       else PollerOK_W = 1;

// All done
//
   initDone   = 1;
   initActive = 0;
   initMutex.UnLock();
   eDest->Say("XrdCS2Xmi: CS2 interface initialized.");
}

/******************************************************************************/
/*                            I n i t X e q D e l                             */
/******************************************************************************/
  
void XrdCS2Xmi::InitXeqDel(castor::stager::FileRequest    *req)

{
   castor::stager::SubRequest *subreq;

// Delete the pre-allocated subrequest
//
   subreq = *(req->subRequests().begin());
   delete subreq;
   req->subRequests().clear();

// Now delete the actual request and its helper (whatever that is)
//
   delete req;
}
 
/******************************************************************************/
/*                               M S S P o l l                                */
/******************************************************************************/
  
void XrdCS2Xmi::MSSPoll(int reqType, const char *UserTag, int is_W)
{
   EPNAME("MSSPoll");
   struct stage_query_req       *requests;  // <- this is allocated by next
   struct stage_filequery_resp  *resp;
   XrdCS2Req                    *reqP;
   char                          errbuf[1024];
   int                           nbresps, i, pcnt;

// Set the location of the error buffer
//
   stage_seterrbuf(errbuf, sizeof(errbuf));

// Create the query type here. We would prefer to use the internal API as the
// external one copies large amounts of data that we just don't need, sigh.
//
   create_query_req(&requests, 1);
   requests[0].type  = reqType;
   requests[0].param = (void *)UserTag;

// Endless loop waiting for thing to query. Eventually, we will have to be
// responsive to file creation requests (though who knows how).
//
do{pcnt = (is_W ? XrdCS2Req::Wait4Q_W() : XrdCS2Req::Wait4Q_R());
   DEBUG("polling for " <<pcnt <<" request(s)");
   while(pcnt > 0)
        {*errbuf = '\0';
         if (stage_filequery(requests, 1, &resp, &nbresps, &Opts) < 0)
            {if (serrno != 0)
                eDest->Emsg("MSSPoll","stage_filequery() failed;",sstrerror(serrno));
             if (*errbuf)
                eDest->Emsg("MSSPoll", errbuf);
	     //             Init(0);
             continue;
            }

          DEBUG("Received " <<nbresps <<" stage_filequery() responses");

          for (i = 0; i < nbresps; i++) {
	    DEBUG(stage_fileStatusName(resp[i].status) <<" rc=" <<
		  resp[i].errorCode <<" path=" <<resp[i].diskserver <<':'
		  <<resp[i].filename <<" (" <<resp[i].castorfilename <<')');
	    if (*resp[i].castorfilename) {
	      pcnt--;
	      if (!resp[i].errorCode) {
		if (!strcmp(stage_fileStatusName(resp[i].status),"STAGEIN")) {  
		  // sendWait(reqP, &resp[i]); 
		  // this file is in STAGEIN we cannot yet redirect 
		     pcnt++;
		} else {
		  if ( (reqP = XrdCS2Req::Remove(resp[i].castorfilename) )) {
		    sendRedirect(reqP, &resp[i]);
		  }
		}
	      } else { 
		if ( (reqP = XrdCS2Req::Remove(resp[i].castorfilename) )) {
		  sendError(reqP, resp[i].castorfilename,
			    resp[i].errorCode, resp[i].errorMessage);
		}
	      }
	    }
	  }
	  free_filequery_resp(resp, nbresps);
          if (pcnt > 0) XrdSysTimer::Wait(MSSPollTime*1000);
	}
   } while(1);
}

/******************************************************************************/
/*                    F u n c t i o n a l   M e t h o d s                     */
/******************************************************************************/
/******************************************************************************/
/*                                 C h m o d                                  */
/******************************************************************************/
  
int XrdCS2Xmi::Chmod(XrdOlbReq *Request, const char *path, mode_t mode)
{
   CS2Xmi::XmiHelper myRequest(Request, path);

// Try to change the mode
//
   if (Cns_chmod(myRequest.Path(), mode))
      return sendError(Request, serrno, "chmod", path);

// Tell client all went well
//
   Request->Reply_OK();
   return 1;
}
  
/******************************************************************************/
/*                                 M k d i r                                  */
/******************************************************************************/
  
int XrdCS2Xmi::Mkdir(XrdOlbReq *Request, const char *path, mode_t mode)
{
   CS2Xmi::XmiHelper myRequest(Request, path);

// Try to change the mode
//
   if (Cns_mkdir(myRequest.Path(), mode))
      return sendError(Request, serrno, "mkdir", path);

// Tell client all went well
//
   Request->Reply_OK();
   return 1;
}
  
/******************************************************************************/
/*                                M k p a t h                                 */
/******************************************************************************/
  
int XrdCS2Xmi::Mkpath(XrdOlbReq *Request, const char *path, mode_t mode) {
  return Mkpath_internal(Request,path, mode, true, true);
}


int XrdCS2Xmi::Mkpath_internal(XrdOlbReq *Request, const char *path, mode_t mode, bool sendreply, bool isdir)
{
   EPNAME("Mkpath_internal");
   CS2Xmi::XmiHelper myRequest(Request, path);
   char *next_path, *this_path = (char *)myRequest.Path();
   struct Cns_filestat cnsbuf;
   int retc;

// Trim off all trailing slashes (we will, however, make all components).
//
   DEBUG("Mkpath_internal " << path);
   if (!(retc = strlen(this_path)))
      return sendError(Request, ENOENT, "mkpath", path);
   while(retc && this_path[retc-1] == '/') 
        {retc--; this_path[retc] = '\0';}
   if (*this_path != '/') 
     if (sendreply)
       return sendError(Request, EINVAL, "mkpath", path);
     else 
       return EINVAL;

// Typically, the path exists. So, do a quick check before launching into it
//
   if (!Cns_stat(this_path, &cnsbuf)) {if (sendreply) Request->Reply_OK(); return 1;}

// Start creating directories starting with the root
//
   next_path = this_path;
   while((next_path = index(next_path+1, int('/'))))
        {*next_path = '\0';
	DEBUG("Mkpath_internal : create subpath " << this_path);
	if ((Cns_mkdir(this_path, mode)) && (Cns_stat(this_path,&cnsbuf)) ) {
	  DEBUG("Mkpath_internal : create subpath failed " << this_path);
	   if (sendreply)
	     return sendError(Request, serrno, "mkdir", next_path);
	   else 
	     return serrno;
	}
         *next_path = '/';
        }

// Make the final component in this path
//
   if (isdir) {
     if (Cns_mkdir(this_path, mode) && serrno != EEXIST)
       if (sendreply)
	 return sendError(Request, serrno, "mkdir", this_path);
       else
	 return serrno;
   }

// All done
//
   if (sendreply)
     Request->Reply_OK();
   return 1;
   }
  
/******************************************************************************/
/*                                  P r e p                                   */
/******************************************************************************/

int XrdCS2Xmi::Prep(const char *ReqID, const char *path, int opts)
{
  EPNAME("Prep");
  
  DEBUG("path=" <<path);
  
  castor::stager::FileRequest *req;
  castor::stager::SubRequest *subreq  = new castor::stager::SubRequest();
  std::vector<castor::rh::Response *>respvec;
  
  castor::client::VectorResponseHandler rh(&respvec);

  
  int TimeOut = stage_getClientTimeout();
  
  castor::stager::StagePrepareToGetRequest    prepReq_ro;
  castor::client::BaseClient                  prepClient(TimeOut);
  
  
#ifdef CASTOR_214
  castor::stager::RequestHelper               prepHlp_ro(&prepReq_ro);
#endif
  
  castor::rh::IOResponse *fr;
  const char *rType;
  mode_t    mask;
  
  // Obtain our umask (sort of silly if you ask me)
  //
  mask = umask(0);
  umask(mask);
  
  // We currently do not support cancel requests, so ignore them
  //
  if (opts & XMI_CANCEL || !*path) return 1;
  
  // Perform an initialization Check
  //
  if (!initDone) return 1;
  
  // Create a new Castor request
  
#ifdef CASTOR_214
  prepHlp_ro.setOptions(&Opts);
#endif

  prepReq_ro.setUserTag(prepTag);
  prepReq_ro.setMask(mask);
  prepReq_ro.addSubRequests(subreq);
  subreq->setRequest(&prepReq_ro);
  subreq->setProtocol(std::string("xroot"));
  subreq->setFileName(std::string(path));
  subreq->setModeBits(0744);
  subreq->setXsize(0);  // for now
  
  req = &prepReq_ro; rType = "Prep2Get";

#ifndef CASTOR_214
  prepClient.setOption(&Opts, &prepReq_ro);
#else 
  prepClient.setOption(&Opts);
#endif


  // Now send the request (we have no idea what to do with reqid which will
  // generate a warning, sigh). There is no need to wait for a response as
  // all we want is to start the staging operation.
  //
  DEBUG(rType << " sending prep path=" <<path);
  try   {std::string reqid = prepClient.sendRequest(req, &rh);
  DEBUG(rType << " reqid=" <<reqid.c_str() <<" path=" <<path);
  }
  catch (castor::exception::Communication e)
         {eDest->Emsg("Select","Communications error;",e.getMessage().str().c_str());
	 Init(reinitTime);
	 delete subreq;
	 return 1;
         }
  catch (castor::exception::Exception e)
    {eDest->Emsg("Select","sendRequest exception;",e.getMessage().str().c_str());
    delete subreq;
    return 1;
    }
  
  // Make sure we have an initial response.
  //
  if (respvec.size() <= 0)
    {eDest->Emsg("Prep", "No response for prepare", path);
    delete subreq;
    return 1;
    }
  
  // Proccess the response.
  //
  fr = dynamic_cast<castor::rh::IOResponse*>(respvec[0]);
  if (0 == fr)
    {eDest->Emsg("Prep", "Invalid response object for prepare", path);
    delete subreq;
    return 1;
    } else if (fr->errorCode()) {
      eDest->Emsg("Prep",fr->errorMessage().c_str(),"preparing",path);
      return fr->errorCode();
    }
  
  // Free response data and return
  //
  delete respvec[0];
  delete subreq;
  
  return 1;
}

/******************************************************************************/
/*                                R e n a m e                                 */
/******************************************************************************/
  
int XrdCS2Xmi::Rename(XrdOlbReq *Request, const char *opath,
                                          const char *npath)
{
   EPNAME("remdir");
   DEBUG("opath=" <<opath << " npath="<<npath);
   CS2Xmi::XmiHelper myRequestOld(Request, opath);
   CS2Xmi::XmiHelper myRequestNew(Request, npath);

// Try to change the mode
//
   if (Cns_rename(myRequestOld.Path(), myRequestNew.Path()))
      return sendError(Request, serrno, "rename", opath);

// Tell client all went well
//
   Request->Reply_OK();
   return 1;
}
  
/******************************************************************************/
/*                                R e m d i r                                 */
/******************************************************************************/
  
int XrdCS2Xmi::Remdir(XrdOlbReq *Request, const char *path)
{
   EPNAME("remdir");
   DEBUG("path=" <<path);
   CS2Xmi::XmiHelper myRequest(Request, path);

// Try to change the mode
//
   if (Cns_rmdir(myRequest.Path()))
      return sendError(Request, serrno, "rmdir", path);

// Tell client all went well
//
   Request->Reply_OK();
   return 1;
}
  
/******************************************************************************/
/*                                R e m o v e                                 */
/******************************************************************************/
  
int XrdCS2Xmi::Remove(XrdOlbReq *Request, const char *path)
{
   EPNAME("remove");
   DEBUG("path=" <<path);
   CS2Xmi::XmiHelper myRequest(Request, path);

   DEBUG("path=" <<myRequest.Path());;
   if ((Cns_delete(myRequest.Path())))
     return sendError(Request, serrno, "rm", path);

// Tell client all went well
//
   Request->Reply_OK();
   return 1;
}
  
/******************************************************************************/
/*                                S e l e c t                                 */
/******************************************************************************/

int XrdCS2Xmi::Select( XrdOlbReq *Request, const char *path, int opts)
{
   EPNAME("Select");
   const char *rType;

// Perform an initialization Check
//   
   if (!initDone) {Request->Reply_Wait(retryTime); return 1;}
   stage_apiInit(&Tinfo);


        if (opts & XMI_NEW) {rType = "Prep2Put";}
   else if (opts & XMI_RW)  {rType = "Prep2Upd";}
   else                     {rType = "Prep2Get";}

   CS2Xmi::XmiJob *jp = new CS2Xmi::XmiJob(this, Request, path, rType);
   Sched->Schedule((XrdJob *)jp);
   return 1;
}

/******************************************************************************/
/*                                  S t a t                                   */
/******************************************************************************/

int XrdCS2Xmi::Stat(XrdOlbReq *Request, const char *path)
{
   CS2Xmi::XmiHelper myRequest(Request, path);
   struct Cns_filestat cnsbuf;
   struct stat statbuf;

// Get information about the file
//
   if (Cns_stat(myRequest.Path(), &cnsbuf)) {
     if (!serrno) {
       return sendError(Request, ENOENT, "stat", path);
     } else 
       return sendError(Request, serrno, "stat", path);
   }

// Convert the Cns stat buffer to a normal Unix one
//
   memset(&statbuf, 0, sizeof(statbuf));
   statbuf.st_mode  = cnsbuf.filemode;
   statbuf.st_nlink = cnsbuf.nlink;
   statbuf.st_uid   = cnsbuf.uid;
   statbuf.st_gid   = cnsbuf.gid;
   statbuf.st_size  = static_cast<off_t>(cnsbuf.filesize);
   statbuf.st_atime = cnsbuf.atime;
   statbuf.st_ctime = cnsbuf.ctime;
   statbuf.st_mtime = cnsbuf.mtime;
   if (cnsbuf.status == '-') statbuf.st_ino = 1;

// Now send the response (let the request object figure out the format)
//
   Request->Reply_OK(statbuf);
   return 1;
}
  
/******************************************************************************/
/*                       P r i v a t e   M e t h o d s                        */
/******************************************************************************/
/******************************************************************************/
/*                                 d o P u t                                  */
/******************************************************************************/
  
void XrdCS2Xmi::doPut(XrdOlbReq *Request, const char *path)
{
   EPNAME("doPut");
   castor::stager::StagePutRequest  putReq_ww;
   castor::client::BaseClient       putClient(stage_getClientTimeout());
// castor::stager::Request         *req;
   castor::stager::SubRequest      *subreq = new castor::stager::SubRequest();
   std::vector<castor::rh::Response *>respvec;
   castor::client::VectorResponseHandler rh(&respvec);
   mode_t    mask;

// Obtain our umask (sort of silly if you ask me)
//
   mask = umask(0);
   umask(mask);

// Create always the castor path
   if ((Mkpath_internal(Request, path,S_IRWXU | S_IRGRP | S_IRWXO , false, false))!=1) {
     Request->Reply_Error("Unable to create castor path (no permissions)");
     return;
   }

// Initialize objects used for put processing.
//
   putReq_ww.setUserTag(stageTag_ww);
   putReq_ww.setMask(mask);
   putReq_ww.addSubRequests(subreq);
#ifdef CASTOR_214
   castor::stager::RequestHelper    putHlp_ww(&putReq_ww);   
#endif

   subreq->setRequest(&putReq_ww);
   subreq->setProtocol(std::string("xroot"));
   subreq->setFileName(std::string(path));
   subreq->setModeBits(0744);

// Now send the request (we have no idea what to do with reqid which will
// generate a warning, sigh). We also need to lock the response queue
// for this request prior to adding it to the queue of pending responses.
// Since Recycle() automatically unlocks the queue so we don't explictly do it.
//
#ifdef CASTOR_214
   putHlp_ww.setOptions(&Opts);
   putClient.setOption(&Opts);
#else
   putClient.setOption(&Opts, &putReq_ww);
#endif


   try   {DEBUG("Sending Put path=" <<path);
          std::string reqid = putClient.sendRequest(&putReq_ww, &rh);
          DEBUG("Put sent; reqid=" <<reqid.c_str() <<" path=" <<path);
         }
   catch (castor::exception::Communication e)
         {eDest->Emsg("Put","Communications error;",e.getMessage().str().c_str());
          Request->Reply_Wait(retryTime);
          return;
         }
   catch (castor::exception::Exception e)
         {eDest->Emsg("Select","sendRequest exception;",e.getMessage().str().c_str());
          Request->Reply_Error("Unexpected communications exception.");
          return;
         }

// Make sure we have an initial response.
//
   if (respvec.size() <= 0)
      {eDest->Emsg("Put", "No response for put", path);
       Request->Reply_Error("Internal Castor2 error receiving put response.");
       return;
      }

// Proccess the response.
//
   castor::rh::IOResponse* fr =
               dynamic_cast<castor::rh::IOResponse*>(respvec[0]);
   if (0 == fr)
      {eDest->Emsg("Select", "Invalid response object for put", path);
       Request->Reply_Error("Internal Castor2 error casting put response.");
       return;
      }

// Send an error or just add this to the queue (sendError/Recycle does unlock)
//
   if (fr->errorCode())
      sendError(Request,path,fr->errorCode(),fr->errorMessage().c_str());
      else {DEBUG(stage_fileStatusName(fr->status()) <<" rc=" << fr->errorCode()
                  <<" path=" <<fr->server().c_str() <<':' <<fr->fileName().c_str()
                  <<" (" <<fr->castorFileName().c_str() <<") id=" <<fr->id());
            sendRedirect(Request, fr, path);
           }

// Free response data and return
//
   delete subreq;
   delete respvec[0];
}

/******************************************************************************/
/*                                 d o G e t                                  */
/******************************************************************************/
  
void XrdCS2Xmi::doGet(XrdOlbReq *Request, const char *path)
{
   EPNAME("doGet");
   castor::stager::StageGetRequest  getReq_ro;
   castor::client::BaseClient       getClient(stage_getClientTimeout());
// castor::stager::Request         *req;
   castor::stager::SubRequest      *subreq = new castor::stager::SubRequest();
   std::vector<castor::rh::Response *>respvec;
   castor::client::VectorResponseHandler rh(&respvec);
   mode_t    mask;
   char      errbuf[1024];

#ifdef CASTOR_214
   castor::stager::RequestHelper    getHlp_ww(&getReq_ro);
#endif

// Obtain our umask (sort of silly if you ask me)
//
   mask = umask(0);
   umask(mask);

// check if this file exists at all!
   struct Cns_filestat cnsbuf;
   struct stat statbuf;

// Get information about the file
//
   if (Cns_stat(path, &cnsbuf)) {
     DEBUG("File does not exists: " << path);
     sendError(Request, ENOENT, "stat", path);
     return ;
   } else {
     if (cnsbuf.filesize == 0) {
       DEBUG("File has 0 size: " <<path);
       sendError(Request, ENODATA,"no tape copy for 0-size file", path);
       return ;
     }
   }

   // Set the location of the error buffer
   //
   stage_seterrbuf(errbuf, sizeof(errbuf));
   
   // we check the staging status
   if (1) {
     struct stage_query_req       *requests;  // <- this is allocated by next
     struct stage_filequery_resp  *resp;
     int                           nbresps, i;
     // Initialize objects used for get processing.                                                                                  
     create_query_req(&requests, 1);
     
     requests[0].type  = BY_FILENAME;
     requests[0].param = (char *) path;

     if (stage_filequery(requests, 1, &resp, &nbresps, &Opts) < 0) {
       if (serrno != 0)
	 eDest->Emsg("Get","stage_filequery() failed;",sstrerror(serrno));
       
       if (*errbuf)
	 eDest->Emsg("Get", errbuf);
       
       Request->Reply_Wait(retryTime);
       return;
     } else {
       DEBUG("Received " <<nbresps <<" stage_filequery() responses");
       for (i = 0; i < nbresps; i++) {
	 DEBUG("path=" << stage_fileStatusName(resp[i].status) <<" rc=" <<
	       resp[i].errorCode <<" path=" <<resp[i].diskserver <<':'
	       <<resp[i].filename <<" (" <<resp[i].castorfilename <<')');
	 
	 if (*resp[i].castorfilename) {
	   if (!resp[i].errorCode) {
	     if (!strcmp(stage_fileStatusName(resp[i].status),"STAGEIN")) {  
	       // this file is in STAGEIN we cannot yet redirect
	       eDest->Emsg("Get","file is STAGEIN",path);
	       Request->Reply_Wait(retryTime);
	       return;
	     } else {
	       DEBUG("Procedd to get path=" <<resp[i].filename );
	     }
	   } else { 
	     DEBUG("Error for " << path );
	     sendError(Request, resp[i].castorfilename,
		       resp[i].errorCode, resp[i].errorMessage);
	     return;
	   }
	 } else {
	   DEBUG("Issue Prep for " <<path );
	   int ec;
	   if ((ec = Prep("noid",path, 0))!=1) {
	     sendError(Request,path, ec, "failed to prepare");
	     return;
	   } else {
	     Request->Reply_Wait(retryTime);
	   }
	   return;
	 }
       }
       free_filequery_resp(resp, nbresps);
       }
     //     free_query_req(requests, 1);
   }

// Initialize objects used for get processing.
//

   getReq_ro.setUserTag(stageTag);
   getReq_ro.setMask(mask);
   getReq_ro.addSubRequests(subreq);
   subreq->setRequest(&getReq_ro);
   subreq->setProtocol(std::string("xroot"));
   subreq->setFileName(std::string(path));
   subreq->setModeBits(0744);

// Now send the request (we have no idea what to do with reqid which will
// generate a warning, sigh). We also need to lock the response queue
// for this request prior to adding it to the queue of pending responses.
// Since Recycle() automatically unlocks the queue so we don't explictly do it.
//
#ifdef CASTOR_214
   getHlp_ww.setOptions(&Opts);
   getClient.setOption(&Opts);
#else
   getClient.setOption(&Opts, &getReq_ro);
#endif

   try   {DEBUG("Sending Get path=" <<path);
          std::string reqid = getClient.sendRequest(&getReq_ro, &rh);
          DEBUG("Get sent; reqid=" <<reqid.c_str() <<" path=" <<path);
         }
   catch (castor::exception::Communication e)
         {eDest->Emsg("Get","Communications error;",e.getMessage().str().c_str());
          Request->Reply_Wait(retryTime);
          return;
         }
   catch (castor::exception::Exception e)
         {eDest->Emsg("Select","sendRequest exception;",e.getMessage().str().c_str());
          Request->Reply_Error("Unexpected communications exception.");
          return;
         }

// Make sure we have an initial response.
//
   if (respvec.size() <= 0)
      {eDest->Emsg("Get", "No response for get", path);
       Request->Reply_Error("Internal Castor2 error receiving get response.");
       return;
      }

// Proccess the response.
//
   castor::rh::IOResponse* fr =
               dynamic_cast<castor::rh::IOResponse*>(respvec[0]);
   if (0 == fr)
      {eDest->Emsg("Select", "Invalid response object for get", path);
       Request->Reply_Error("Internal Castor2 error casting get response.");
       return;
      }

// Send an error or just add this to the queue (sendError/Recycle does unlock)
//
   if (fr->errorCode())
      sendError(Request,path,fr->errorCode(),fr->errorMessage().c_str());
      else {DEBUG(stage_fileStatusName(fr->status()) <<" rc=" << fr->errorCode()
                  <<" path=" <<fr->server().c_str() <<':' <<fr->fileName().c_str()
                  <<" (" <<fr->castorFileName().c_str() <<") id=" <<fr->id());
            sendRedirect(Request, fr, path);
           }

// Free response data and return
//
   delete subreq;
   delete respvec[0];
}

/******************************************************************************/
/*                             s e n d E r r o r                              */
/******************************************************************************/
  
int XrdCS2Xmi::sendError(XrdOlbReq *reqP, int rc, const char *opn, 
                                                  const char *path)
{
   EPNAME("sendError");
   static int Retries = 255;
   const char *ecode;
   char buff[256];

// Convert errnumber to err string
//
   switch(rc)
         {case ENOENT:        ecode = "ENOENT";      break;
          case EPERM:         ecode = "EISDIR";      break;// Wierd but documented
          case EEXIST:        ecode = "ENOTEMPTY";   break;// Wierd but documented
          case EACCES:        ecode = "EACCES";      break;
          case ENOSPC:        ecode = "ENOSPC";      break;
          case EFAULT:        ecode = "ENOMEM";      break;// No other choice here
          case ENOTDIR:       ecode = "ENOTDIR";     break;
          case ENAMETOOLONG:  ecode = "ENAMETOOLONG";break;
	  case ENODATA:       ecode = "ENODATA";     break;
          case SENOSHOST:     ecode = "ENETUNREACH"; break;
          case SENOSSERV:     ecode = "ENETUNREACH"; break;
          case SECOMERR:      ecode = 0;             break;// Comm error
          case ENSNACT:       ecode = 0;             break;// NS is down
	  case EINPROGRESS:   ecode = 0;             break;// still staging  
          default:            ecode = "EINVAL";
                              break;
         }

// If we have an error tag then we can send it
//
   if (ecode)
      {snprintf(buff, sizeof(buff), "%s failed; %s", opn, sstrerror(rc));
       buff[sizeof(buff)-1] = '\0';
       reqP->Reply_Error(ecode, buff);
       DEBUG("msg='" <<buff <<"' path=" <<path);
       return 1;
      }

// For communication errors we will tell the client to try again later
//
   Retries++;
   if (!(Retries & 255)) eDest->Emsg("cs2", opn, "failed;", sstrerror(rc));
   reqP->Reply_Wait(retryTime);
   return 1;
}

/******************************************************************************/
int XrdCS2Xmi::sendError(XrdCS2Req *reqP, const char *fn, int rc, const char *emsg)
{
   EPNAME("sendError");
   char buff[1024];

   snprintf(buff, sizeof(buff), "Staging error %d: %s (%s)", rc, 
                                 sstrerror(rc), (emsg ? emsg : ""));
   buff[sizeof(buff)-1] = '\0';
   DEBUG("msg='" <<buff <<"'; path=" <<fn);

   do {reqP->Request()->Reply_Error(buff);} while((reqP = reqP->Recycle()));
   return 1;
}

/******************************************************************************/
int XrdCS2Xmi::sendError(XrdOlbReq *reqP, const char *fn, int rc, const char *emsg)
{
   EPNAME("sendError");
   char buff[1024];

   snprintf(buff, sizeof(buff), "Staging error %d: %s (%s)", rc, 
                                 sstrerror(rc), (emsg ? emsg : ""));
   buff[sizeof(buff)-1] = '\0';
   DEBUG("msg='" <<buff <<"'; path=" <<fn);

   reqP->Reply_Error(buff);
   return 1;
}

/******************************************************************************/
/*                          s e n d R e d i r e c t                           */
/******************************************************************************/
  
void XrdCS2Xmi::sendRedirect(XrdCS2Req                   *reqP,
                             struct stage_filequery_resp *resp)
{
   EPNAME("sendRedirect");
   char buff[256];

// Send the redirect
//
   do{sprintf(buff, "&cs2.fid=%s", resp->filename);
      DEBUG("-> " <<resp->diskserver <<'?' <<buff <<" path=" <<reqP->Path());
      reqP->Request()->Reply_Redirect(resp->diskserver, 0, buff);
     } while((reqP = reqP->Recycle()));
}

/******************************************************************************/
  
void XrdCS2Xmi::sendRedirect(XrdOlbReq                   *reqP,
                             castor::rh::IOResponse      *resp,
                             const char                  *Path)
{
   EPNAME("sendRedirect");
   char buff[256];

// Send the redirect
//
   sprintf(buff, "&cs2.fid=%llu@%s", resp->fileId(),
          (castorns ? castorns : (Opts.stage_host ? Opts.stage_host : "")));
   DEBUG("-> " <<resp->server().c_str() <<'?' <<buff <<" path=" <<Path);
   reqP->Reply_Redirect(resp->server().c_str(), 0, buff);
}

void XrdCS2Xmi::sendWait(XrdCS2Req                   *reqP,
			 struct stage_filequery_resp *resp)

{
  EPNAME("sendWait");

  // Send the Wait
  //

  do{
    DEBUG("Delaying client -> file " << reqP->Path() << " in status " << stage_fileStatusName(resp->status) );
    eDest->Emsg("cs2","Wait 90 sec;",reqP->Path() , stage_fileStatusName(resp->status) );
    reqP->Request()->Reply_Wait(retryTime);
  } while((reqP = reqP->Recycle()));


}
