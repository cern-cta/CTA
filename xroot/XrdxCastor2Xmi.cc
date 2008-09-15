//         $Id: XrdxCastor2Xmi.cc,v 1.1 2008/09/15 10:04:02 apeters Exp $

#include "XrdxCastor2Xmi.hh"
#include "XrdOuc/XrdOucStream.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdxCastor2XmiTrace.hh"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#define RFIO_NOREDEFINE
#include <shift.h>
#include <shift/Cns_api.h>


XrdSysError  *XrdxCastor2Xmi::eDest;
XrdInet      *XrdxCastor2Xmi::iNet;
XrdScheduler *XrdxCastor2Xmi::Sched ;
XrdOucTrace  *XrdxCastor2Xmi::Trace;

extern XrdSysLogger      XrdLogger;

int 
XrdxCastor2Xmi::Prep  (const char           *ReqID,
		       int             Opts,
		       const char           *Path,
		       const char           *Opaque)
{
  EPNAME("Prep");
  if (Opaque) {
    ZTRACE(Stage,"path=" << Path << " Opaque=" << Opaque);
  } else {
    ZTRACE(Stage,"path=" << Path);
  }
  return 1;
}

bool 
XrdxCastor2Xmi::SetStageVariables( const char* Path, const char* Opaque, XrdOucString &stagehost, XrdOucString &serviceclass) 
{
  EPNAME("SetStageVariables");
  if (Opaque) {
    ZTRACE(Stage,"path=" << Path << " Opaque=" << Opaque);
  } else {
    ZTRACE(Stage,"path=" << Path);
  }

  XrdOucEnv Open_Env(Opaque);
  const char* val;
  if ((val=Open_Env.Get("stagerHost"))){
    stagehost = val;
  } else {
    XrdOucString spath = Path;
    XrdOucString *mhost;
    // try the stagertable
    int pos=0;
    bool found=false;
    while ( (pos = spath.find("/",pos) ) != STR_NPOS ) {
      XrdOucString subpath;
      subpath.assign(Path,0,pos);
      if ( (mhost = stagertable->Find(subpath.c_str()))) {
	XrdOucString shost;
	int offset;
	if ((offset=mhost->find("::")) != STR_NPOS) {
	  shost.assign(mhost->c_str(),0,offset-1);
	  stagehost = shost.c_str();
	  if (!Open_Env.Get("svcClass")) {
	    shost.assign(mhost->c_str(),offset+2);
	    serviceclass = shost.c_str();
	  }
	} else {
	  stagehost = mhost->c_str();
	}
	found=true;
      }
      pos++;
    }
    if (!found) {
      if (mhost = stagertable->Find("default")) {
	stagehost = mhost->c_str();
      } else {
	return false;
      }
    }
  }   
  
  if ((val=Open_Env.Get("svcClass"))) {
    serviceclass = val;
  }

  return true;
}

bool 
XrdxCastor2Xmi::GetCacheFile(XrdOucString stagehost, XrdOucString serviceclass, const char* Path, XrdOucString locationfile, XrdOucString locationhost, XrdOucString locationpath) 
{
  EPNAME("GetCacheFile");
  if (LocationCacheDir != "") {
    locationfile = LocationCacheDir;
    locationfile += "/"; locationfile+= stagehost; locationfile+="/"; locationfile += serviceclass; 
    locationfile += Path;
    ZTRACE(Stage,"Cache Search: " << locationfile.c_str());
    char linklookup[8192];
    if ((::readlink(locationfile.c_str(),linklookup,sizeof(linklookup)))>0) {
      ZTRACE(Stage,"Cache location: " << linklookup);
      XrdOucString slinklookup = linklookup;
      XrdOucString slinkpath;
      XrdOucString slinkhost;
      while(slinklookup.replace("%","/")) {}; 
      int pathposition;
      if ((slinklookup.length() > 7) && ((pathposition=slinklookup.find("//",7))!=STR_NPOS)) {
	slinkpath.assign(slinklookup,pathposition + 1);
	slinkhost.assign(slinklookup,7,pathposition-1);
	locationhost=slinkhost;
	locationpath=slinkpath;
	return true;
      } 
    }
  }
  return false;
}

int  
XrdxCastor2Xmi::Select( XrdCmsReq      *Request, 
			int             opts,
			const char           *Path,
			const char           *Opaque) 
{
  EPNAME("Select");


  if (Opaque) {
    ZTRACE(Stage,"path=" << Path << " Opaque=" << Opaque);
  } else {
    ZTRACE(Stage,"path=" << Path);
  }
  
  // set the proper stager setting
  XrdOucString stagehost="";
  XrdOucString serviceclass="default";
  
  XrdOucString locationfile;
  XrdOucString locationhost;
  XrdOucString locationpath;
  
  if (!SetStageVariables(Path,Opaque,stagehost,serviceclass)) {
    eDest->Emsg(epname,EINVAL, "set the stager host - you didn't specify it and I have no stager map for fn = ", Path);
    Request->Reply_Error("you didn't specify the stager host and it is not defined in my stager map");
    return 0;
  }

  if (GetCacheFile(stagehost,serviceclass,Path,locationfile,locationhost,locationpath)) {
    Request->Reply_Redirect("pcitsmd01.cern.ch",0,0);
    return 1;
  } else {
    Request->Reply_Error("file location is not cached");
    return 0;
  }
}
  
int  
XrdxCastor2Xmi::Stat  ( XrdCmsReq      *Request,
			const char           *Path,
			const char           *Opaque) 
{
  EPNAME("Stat");
  if (Opaque) {
    ZTRACE(Stage,"path=" << Path << " Opaque=" << Opaque);
  } else {
    ZTRACE(Stage,"path=" << Path);
  }

  struct Cns_filestat buf;
  int retc = Cns_stat(Path,&buf);
  if (!retc) {
    ZTRACE(Stage,"stat OK");
    Request->Reply_OK();
    return 1;
  } else {
    ZTRACE(Stage,"stat FAILED");
    Request->Reply_Error("no such file or directory");
    return 0;
  }
}

int  
XrdxCastor2Xmi::State  ( XrdCmsReq      *Request,
			const char           *Path,
			const char           *Opaque) 
{
  EPNAME("State");
  if (Opaque) {
    ZTRACE(Stage,"path=" << Path << " Opaque=" << Opaque);
  } else {
    ZTRACE(Stage,"path=" << Path);
  }

  // set the proper stager setting
  XrdOucString stagehost="";
  XrdOucString serviceclass="default";
  
  if (!SetStageVariables(Path,Opaque,stagehost,serviceclass)) {
    eDest->Emsg(epname,EINVAL, "set the stager host - you didn't specify it and I have no stager map for fn = ", Path);
    Request->Reply_Error("you didn't specify the stager host and it is not defined in my stager map");
    return 0;
  }

  XrdOucString locationfile;
  XrdOucString locationhost;
  XrdOucString locationpath;

  if (GetCacheFile(stagehost,serviceclass,Path,locationfile,locationhost,locationpath)) {
    Request->Reply_OK();
    return 1;
  } else {
    Request->Reply_Error("file location is not cached");
  }

  return 0;
}

bool
XrdxCastor2Xmi::Init( XrdCmsXmiEnv* Env)
{
  //  XrdXrootdNetwork = Env->iNet;
  
  eDest = Env->eDest;         // -> Error message handler
  iNet  = Env->iNet;          // -> Network object
  Sched = Env->Sched;         // -> Thread scheduler
  Trace = Env->Trace;         // -> Trace handler
  eDest->logger(&XrdLogger);
  Trace->What = TRACE_ALL;
  EPNAME("Init");
  ZTRACE(ALL,"++++++ (c) 2008 CERN/IT-DM-SMD xCastor2Xmi (extended Castor2 Interface Xmi Plugin ) v 1.0");
  if (Env->ConfigFN) {
    char *var;
    const char *val;
    int cfgFD, retc, NoGo = 0;
    XrdOucStream Config(eDest, 0);

    if ( (cfgFD = open(Env->ConfigFN, O_RDONLY, 0)) < 0)
      return eDest->Emsg("Config", errno, "open config file", Env->ConfigFN);

    Config.Attach(cfgFD);

    while (( var = Config.GetMyFirstWord())) {
      if (!strncmp(var, "xcastor2.",9)) {
	var += 9;
	
	if (!strcmp("stagermap",var)) {
	  if ((!(val = Config.GetWord()))) {
	    eDest->Emsg("Config","you have to give a path + the stagerhost to assign"); NoGo=1;
	  } else {
	    XrdOucString stagepath = val;
	    if ((!(val = Config.GetWord()))) {
	      eDest->Emsg("Config","you have to give a path + the stagerhost to assign"); NoGo=1;
	    } else {
	      if (!stagertable->Find(stagepath.c_str())) {
		stagertable->Add(stagepath.c_str(),new XrdOucString(val));
		XrdOucString sayit;
		sayit = stagepath;
		sayit += " -> ";
		sayit += val;
		eDest->Say("=====> xcastor2.stagermap : ", sayit.c_str());
	      }
	    }
	  }
	}
	
	if (!strcmp("stagerpolicy",var)) {
	  if ((!(val = Config.GetWord()))) {
	    eDest->Emsg("Config","you have to give a stagerhost::svcclass tag and a policy ( schedall|schedread|schedwrite| )"); NoGo=1;
	  } else {
	    XrdOucString stagepair = val;
	    if ((!(val = Config.GetWord())) || ((!strstr(val,"schedall")) && (!strstr(val,"schedread")) && (!strstr(val,"schedwrite")))) {
	      eDest->Emsg("Config","you have to give a stagerhost::svcclass tag and a policy ( schedall|schedread|schedwrite| )"); NoGo=1;
	    } else {
	      if (!stagertable->Find(stagepair.c_str())) {
		stagerpolicy->Add(stagepair.c_str(),new XrdOucString(val));
		XrdOucString sayit;
		sayit = stagepair;
		sayit += " -> ";
		sayit += val;
		eDest->Say("=====> xcastor2.stagerpolicy : ", sayit.c_str());
	      }
	    }
	  }
	}

	
	if (!strcmp("locationcache",var)) {
	  
	  if ((!(val = Config.GetWord())) || (::access(val,W_OK))) {
	    eDest->Emsg("Config","I cannot access your location cache directory!"); NoGo=1;} else {
	      LocationCacheDir=val;
	    }
	  eDest->Say("=====> xcastor2.locationcache : ", val);
	}
      }
    }
    close(cfgFD);
  }
  return true;
}
