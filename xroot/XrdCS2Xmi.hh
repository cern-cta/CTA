#ifndef _XRDCS2XMI_H_
#define _XRDCS2XMI_H_
/******************************************************************************/
/*                                                                            */
/*                          X r d C S 2 X m i . h h                           */
/*                                                                            */
/* (c) 2006 by the Board of Trustees of the Leland Stanford, Jr., University  */
/*                            All Rights Reserved                             */
/*   Produced by Andrew Hanushevsky for Stanford University under contract    */
/*              DE-AC02-76-SFO0515 with the Department of Energy              */
/******************************************************************************/

//         $Id: XrdCS2Xmi.hh,v 1.1 2008/02/25 15:15:47 apeters Exp $

#include "XrdOlb/XrdOlbReq.hh"
#include "XrdOlb/XrdOlbXmi.hh"
#include "XrdSys/XrdSysPthread.hh"
  
#include "castor/BaseObject.hpp"
#include "castor/Constants.hpp"
#include "castor/client/VectorResponseHandler.hpp"
#include "castor/client/BaseClient.hpp"
#include "castor/stager/Request.hpp"
#include "castor/stager/StagePrepareToGetRequest.hpp"
#include "castor/stager/StagePrepareToPutRequest.hpp"
#include "castor/stager/StagePrepareToUpdateRequest.hpp"
#include "castor/stager/StagePutRequest.hpp"
#include "castor/stager/StageGetRequest.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/rh/IOResponse.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/Communication.hpp"
#include "stager_client_api_common.hpp"

#ifdef CASTOR_214
#include "castor/stager/RequestHelper.hpp"
#endif

class XrdSysError;
class XrdInet;
class XrdScheduler;
class XrdOucTrace;

class XrdCS2Xmi : XrdOlbXmi
{
public:

       int  Chmod (      XrdOlbReq      *Request,
                   const char           *path,
                         mode_t          mode);

       int  Mkdir (      XrdOlbReq      *Request,
                   const char           *path,
                         mode_t          mode);

       int  Mkpath(      XrdOlbReq      *Request,
                   const char           *path,
                         mode_t          mode);

       int  Mkpath_internal(      XrdOlbReq      *Request,
                   const char           *path,
                         mode_t          mode,
                         bool            sendreply,
                         bool            isdir);

       int  Prep  (const char           *ReqID,
                   const char           *path,
                         int             opts);

       int  Rename(      XrdOlbReq      *Request,
                   const char           *oldpath,
                   const char           *newpath);

       int  Remdir(      XrdOlbReq      *Request,
                   const char           *path);

       int  Remove(      XrdOlbReq      *Request,
                   const char           *path);

       int  Select(      XrdOlbReq      *Request,
                   const char           *path,
                         int             opts);

       int  Stat  (      XrdOlbReq      *Request,
                   const char           *path);

       void InitXeq();

       void doPut(XrdOlbReq *Request, const char *path);
       void doGet(XrdOlbReq *Request, const char *path);

       void MSSPoll(int reqType, const char *UserTag, int is_W);

            XrdCS2Xmi(XrdOlbXmiEnv *);

virtual    ~XrdCS2Xmi() {}

private:
int   sendError(XrdOlbReq *reqP, int rc, const char *opn, const char *path);
int   sendError(XrdCS2Req *reqP, const char *fn, int rc, const char *emsg);
int   sendError(XrdOlbReq *reqP, const char *fn, int rc, const char *emsg);
void  sendRedirect(XrdCS2Req *, struct stage_filequery_resp *);
void  sendWait(XrdCS2Req *, struct stage_filequery_resp *);
void  sendRedirect(XrdOlbReq *, castor::rh::IOResponse *, const char *);
void  Init(int When=0, int force=0);
void  InitXeqDel(castor::stager::FileRequest    *req);


// The objects to handle prepare (we never query them)
//
   static const char                           *prepTag;
   static const char                           *stageTag;
   static const char                           *stageTag_ww;
   static const char                           *stageTag_ro;

// Stage options filled in by the constructor
//
   struct stage_options                         Opts;
   struct stager_client_api_thread_info        *Tinfo;
   char                                        *castorns;

// Other pointers
//
static XrdSysError  *eDest;         // -> Error message handler
static XrdInet      *iNet;          // -> Network object
static XrdScheduler *Sched;         // -> Thread scheduler
static XrdOucTrace  *Trace;         // -> Trace handler

// Initialization control variable protected by initMutex
//
static const int reinitTime = 60; // Re-initialize no more then 60 times an hour
static const int retryTime  = 90; // How long a client waits between retries
static const int MSSPollTime= 2 ; // Poll for new files every 2 seconds
XrdSysMutex      initMutex;
char             initDone;
char             initActive;
};
#endif
