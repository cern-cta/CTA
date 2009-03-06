//         $Id: XrdxCastor2Xmi.hh,v 1.2 2009/03/06 13:45:04 apeters Exp $

#ifndef __XCASTOR2_XMI_H__
#define __XCASTRO2_XMI_H__

#include "XrdCms/XrdCmsXmi.hh"
#include "XrdOuc/XrdOucHash.hh"
#include "XrdOuc/XrdOucString.hh"
#include <stdio.h>

class XrdSysError;
class XrdInet;
class XrdScheduler;
class XrdOucTrace;

XrdInet *XrdXrootdNetwork;

class XrdxCastor2Xmi : XrdCmsXmi
{
public:
  static XrdSysError  *eDest;         // -> Error message handler
  static XrdInet      *iNet;          // -> Network object
  static XrdScheduler *Sched;         // -> Thread scheduler
  static XrdOucTrace  *Trace;         // -> Trace handler
  XrdOucHash<XrdOucString> *stagertable;
  XrdOucHash<XrdOucString> *stagerpolicy;
  XrdOucString         LocationCacheDir;
  char*                HostName;
  char*                HostPref;
  XrdOucString         ManagerId;

  int  Chmod (      XrdCmsReq      *Request,
		    mode_t          mode,
                    const char           *path,
		    const char           *opaque) { return 0;}
  
  // Called when trying to determine the load on this host (not yet implemented)
  //
  int  Load  (      XrdCmsReq      *Request) {return 0;} // Server only
  
  // Called to make a directory; opaque may be a nil ptr.
  //
  int  Mkdir (      XrdCmsReq      *Request,
		    mode_t          mode,
		    const char           *path,
		    const char           *opaque) { return 0;}
  
  // Called to make a directory path; opaque may be a nil ptr.
  //
  int  Mkpath(      XrdCmsReq      *Request,
		    mode_t          mode,
                    const char           *path,
		    const char           *opaque) { return 0;}
  
  // Called to prepare future access to a file; opaque may be a nil ptr.
  //
  int  Prep  (const char           *ReqID,
	      int             Opts,
	      const char           *Path,
	      const char           *Opaque);
  
  // Called to rename a file or directory; oldopaque/newopaque may be a nil ptrs.
  //
  int  Rename(      XrdCmsReq      *Request,
                    const char           *oldpath,
                    const char           *oldopaque,
                    const char           *newpath,
		    const char           *newopaque) { return 0;}

  // Called to remove a directory; opaque may be a nil ptr.
  //
  int  Remdir(      XrdCmsReq      *Request,
                    const char           *path,
		    const char           *opaque) { return 0;}
  
  int  Remove(      XrdCmsReq      *Request,
                    const char           *path,
		    const char           *opaque) { return 0;}
 
  // Called when a client attempts to locate or open a file. The opts indicate 
  // how the file will used and whether it is to be created. The opaque may be 
  // a nil ptr.
  //
  int  Select(      XrdCmsReq      *Request, // See description above
		    int             opts,
                    const char           *path,
		    const char           *opaque) ;
  
  // Called to determine how much space exists in this server (not implemented)
  //
  int  Space (      XrdCmsReq      *Request) {return 0;}  // Server Only

  // Called to get information about a file; opaque may be a nil ptr.
  //
  int  Stat  (      XrdCmsReq      *Request,
		    const char           *path,
		    const char           *opaque);

  bool Init  (      XrdCmsXmiEnv   *xmienv);

  bool SetStageVariables(const char* Path, const char* Opaque, XrdOucString &stagehost, XrdOucString &serviceclass);

// Called after the plugin is loaded to determine which and how the above
// methods are to be called.
//
  void XeqMode(unsigned int &isNormal, 
                     unsigned int &isDirect)
                    {isNormal = XMI_LOAD; isDirect = 0xffff;}
  
  XrdxCastor2Xmi() {
    stagertable   = new XrdOucHash<XrdOucString> ();
    stagerpolicy  = new XrdOucHash<XrdOucString> ();
    LocationCacheDir = "";
  }
  virtual    ~XrdxCastor2Xmi() {if (stagertable) delete stagertable; if (stagerpolicy) delete stagerpolicy;}
};


extern "C"
{
  XrdCmsXmi *XrdCmsgetXmi(int argc, char **argv, XrdCmsXmiEnv *XmiEnv) {
    XrdxCastor2Xmi* xmi =  new XrdxCastor2Xmi();
    if (!xmi) {
      return 0;
    }
    if (!xmi->Init(XmiEnv)) {
      return 0;
    }
    return (XrdCmsXmi*) xmi;
  }
};
#endif
