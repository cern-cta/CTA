//         $Id: XrdxCastor2Xmi.cc,v 1.2 2009/03/06 13:45:04 apeters Exp $

#include "XrdxCastor2Xmi.hh"
#include "XrdOuc/XrdOucStream.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdxCastor2XmiTrace.hh"
#include "XrdxCastor2Stager.hh"
#include "XrdxCastor2Fs.hh"
#include "XrdNet/XrdNetDNS.hh"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>

#define RFIO_NOREDEFINE
#include <shift.h>
#include <shift/Cns_api.h>


XrdSysError  *XrdxCastor2Xmi::eDest;
XrdInet      *XrdxCastor2Xmi::iNet;
XrdScheduler *XrdxCastor2Xmi::Sched ;
XrdOucTrace  *XrdxCastor2Xmi::Trace;

extern XrdSysLogger      XrdLogger;
extern XrdOucTrace       xCastor2FsTrace;

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

  int rc;
  XrdOucString stagehost="";
  XrdOucString serviceclass="default";
  XrdOucString stagestatus="";
  
  if (!SetStageVariables(Path,Opaque,stagehost,serviceclass)) {
    eDest->Emsg(epname,EINVAL, "set the stager host - you didn't specify it and I have no stager map for fn = ", Path);
    return 0;
  }

  struct Cns_filestatcs cstat;
  
  if (XrdxCastor2FsUFS::Statfn(Path, &cstat) ) {
    eDest->Emsg(epname,EINVAL, "stat the file fn = ", Path);
    return 0;
  }

  XrdOucErrInfo error;
  XrdOucString dummy1;
  XrdOucString dummy2;

  ZTRACE(Stage,"issue prepare2get for path=" << Path);

  if (!XrdxCastor2Stager::Prepare2Get(error, (uid_t) 0, (gid_t) 0, Path, stagehost.c_str(),serviceclass.c_str(),dummy1, dummy2, stagestatus)) {
    eDest->Emsg(epname,error.getErrInfo(),error.getErrText());
    return 0;
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

  uid_t client_uid=0;
  gid_t client_gid=0;


  XrdOucEnv Open_Env(Opaque);

  // currently nobody set's this information ....
  if (Open_Env.Get("client_uid")) {
    client_uid = atol(Open_Env.Get("client_uid"));
  }
  if (Open_Env.Get("client_gid")) {
    client_gid = atol(Open_Env.Get("client_gid"));
  }

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
    
    // mapping by uid/gid overwrites path mapping
    XrdOucString uidmap="uid:";
    XrdOucString gidmap="gid:";
    uidmap += (int)client_uid;
    gidmap += (int)client_gid;
    
    if ( (mhost = stagertable->Find(uidmap.c_str()))) {
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
    
    if ( (mhost = stagertable->Find(gidmap.c_str()))) {
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

  // currently we appear to locate requests as a plain disk server e.g. we don't resolve locations deeper than our 
  return 0;
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

  struct Cns_filestatcs buf;
  int retc = XrdxCastor2FsUFS::Statfn(Path,&buf);
  
  if (retc) {
    eDest->Emsg(epname,errno, "to stat file for fn = ", Path);
    Request->Reply_Error("no such file or directory");
    return 0;
  }
  
  XrdOucString stagehost="";
  XrdOucString serviceclass="default";
  XrdOucString stagestatus="";

  if (!S_ISDIR(buf.filemode)) {
    if (!SetStageVariables(Path,Opaque,stagehost,serviceclass)) {
      eDest->Emsg(epname,EINVAL, "set the stager host - you didn't specify it and I have no stager map for fn = ", Path);
      Request->Reply_Error("you didn't specify the stager host and it is not defined in my stager map");
      return 0;
    }
  } else {
    Request->Reply_Error("you are stat'ing a directory");
    return 0;    
  }

  // check if the file is staged
  XrdOucErrInfo error;
  
  if (!XrdxCastor2Stager::StagerQuery(error, (uid_t) 0, (gid_t) 0, Path, stagehost.c_str(),serviceclass.c_str(),stagestatus)) {
    stagestatus = "NA";
  }
  if ( (stagestatus != "STAGED") && (stagestatus != "CANBEMIGR") ) {
    eDest->Emsg(epname,EINVAL, "find file in stager: fn = ", Path);
    Request->Reply_Error("file is not staged");
    return 0;
  } else {
    Request->Reply_OK();
    return 1;
  }
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
  xCastor2FsTrace = *Trace;
  EPNAME("Init");
  ZTRACE(ALL,"++++++ (c) 2008 CERN/IT-DM-SMD xCastor2Xmi (extended Castor2 Interface Xmi Plugin ) v 1.0");

  {
    // borrowed from XrdOfs 
    unsigned int myIPaddr = 0;
    
    char buff[256], *bp;
    int i;
    char* locResp;
    
    // Obtain port number we will be using
    //
    long myPort = (bp = getenv("XRDPORT")) ? strtol(bp, (char **)NULL, 10) : 0;
    
    // Establish our hostname and IPV4 address
    //
    HostName      = XrdNetDNS::getHostName();
    
    if (!XrdNetDNS::Host2IP(HostName, &myIPaddr)) myIPaddr = 0x7f000001;
    strcpy(buff, "[::"); bp = buff+3;
    bp += XrdNetDNS::IP2String(myIPaddr, 0, bp, 128);
    *bp++ = ']'; *bp++ = ':';
    sprintf(bp, "%d", myPort);
    for (i = 0; HostName[i] && HostName[i] != '.'; i++);
    HostName[i] = '\0';
    HostPref = strdup(HostName);
    HostName[i] = '.';
    eDest->Say("=====> xcastor2.hostname: ", HostName,"");
    eDest->Say("=====> xcastor2.hostpref: ", HostPref,"");
    ManagerId=HostName;
    ManagerId+=":";
    ManagerId+=(int)myPort;
    eDest->Say("=====> xcastor2.managerid: ",ManagerId.c_str(),"");
  }

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
