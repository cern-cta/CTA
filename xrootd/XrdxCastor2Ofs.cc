//          $Id: XrdxCastor2Ofs.cc,v 1.6 2009/04/29 10:15:03 apeters Exp $

#include "XrdAcc/XrdAccAuthorize.hh"
#include "XrdOfs/XrdOfsTrace.hh"
#include "XrdClient/XrdClientAdmin.hh"
#include "XrdOfs/XrdOfs.hh"
#include "XrdSfs/XrdSfsAio.hh"
#include "XrdSys/XrdSysTimer.hh"
#include "XrdOss/XrdOssApi.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "XrdOuc/XrdOucHash.hh"
#include "XrdNet/XrdNetOpts.hh"
#include "XrdxCastor2Ofs.hh"
#include "XrdxCastor2FsConstants.hh"
#include "XrdxCastor2Timing.hh"
#include "XrdxCastor2ClientAdmin.hh"
#include <sys/types.h>
#include <sys/stat.h>
#include <pwd.h>
#include <grp.h>
#include <fcntl.h>
#include <attr/xattr.h>
#include "h/Cns_api.h"
#include "h/serrno.h"
#include <string.h>

class XrdxCastor2Ofs;

XrdxCastor2Ofs XrdxCastor2OfsFS;

extern XrdSfsFileSystem *XrdSfsGetDefaultFileSystem(XrdSfsFileSystem *native_fs,
						    XrdSysLogger     *lp,
						    const char       *configfn);

// extern symbols
extern XrdOfs     *XrdOfsFS;
extern XrdSysError OfsEroute;
extern XrdOssSys *XrdOfsOss;
extern XrdOss    *XrdOssGetSS(XrdSysLogger *, const char *, const char *);
extern XrdOucTrace OfsTrace;

// global singletons
XrdOucHash<struct passwd> *XrdxCastor2Ofs::passwdstore;
XrdOucHash<XrdxCastor2ClientAdmin> *XrdxCastor2Ofs::clientadmintable;

#define ADDTOTALREADBYTES(__bytes) \
  do {									\
    XrdxCastor2OfsFS.StatisticMutex.Lock();					\
    XrdxCastor2OfsFS.TotalReadBytes += (long long)__bytes;			\
    XrdxCastor2OfsFS.StatisticMutex.UnLock();					\
  } while (0);

#define ADDTOTALWRITEBYTES(__bytes) \
  do {									\
    XrdxCastor2OfsFS.StatisticMutex.Lock();					\
    XrdxCastor2OfsFS.TotalWriteBytes += (long long)__bytes;			\
    XrdxCastor2OfsFS.StatisticMutex.UnLock();					\
  } while (0);

#define ADDTOTALSTREAMREADBYTES(__bytes) \
  do {									\
    XrdxCastor2OfsFS.StatisticMutex.Lock();					\
    XrdxCastor2OfsFS.TotalStreamReadBytes += (long long)__bytes;			\
    XrdxCastor2OfsFS.StatisticMutex.UnLock();					\
  } while (0);

#define ADDTOTALSTREAMWRITEBYTES(__bytes) \
  do {									\
    XrdxCastor2OfsFS.StatisticMutex.Lock();					\
    XrdxCastor2OfsFS.TotalStreamWriteBytes += (long long)__bytes;			\
    XrdxCastor2OfsFS.StatisticMutex.UnLock();					\
  } while (0);

#define DOREADDELAY() \
  do {                                                                  \
    double period=0; RateTimer.Report(period);                          \
    double realreadtime = FileReadBytes/1000.0/1000.0/ ReadRateLimit;   \
    double missingrate =  ReadRateLimit - ( FileReadBytes / 1000.0 / 1000.0 / period);\
    if (realreadtime > period) {                                        \
      XrdSysTimer::Wait( (int)((realreadtime - period)*1000.0) );       \
    } else {                                                            \
      if (RateMissingCnt != XrdxCastor2OfsFS.RateMissingCnt) {                  \
          XrdxCastor2OfsFS.ReadRateMissing += (unsigned int)missingrate;        \
          RateMissingCnt = XrdxCastor2OfsFS.RateMissingCnt;                     \
        }                                                               \
    }                                                                   \
  } while (0);                                                          

#define DOWRITEDELAY() \
  do {                                                                  \
    double period; RateTimer.Report(period);                            \
    double realwritetime = FileWriteBytes/1000.0/1000.0/ WriteRateLimit;\
    if (realwritetime > period) {                                       \
      XrdSysTimer::Wait( (int)((realwritetime - period)*1000.0) );      \
    }                                                                   \
  } while (0);                                               
 
			
/******************************************************************************/
/*                   S f s G e t F i l e S y s t e m                          */
/******************************************************************************/

extern "C"
{
XrdSfsFileSystem *XrdSfsGetFileSystem(XrdSfsFileSystem *native_fs, 
                                      XrdSysLogger     *lp,
                                      const char       *configfn)
{
// Do the herald thing
//
   OfsEroute.SetPrefix("castor2ofs_");
   OfsEroute.logger(lp);
   OfsEroute.Say("++++++ (c) 2008 CERN/IT-DM-SMD ",
		"xCastor2Ofs (extended Castor2 File System) v 1.0");

// Initialize the subsystems
//
   XrdxCastor2OfsFS.ConfigFN = (configfn && *configfn ? strdup(configfn) : 0);
   XrdxCastor2OfsFS.passwdstore = new XrdOucHash<struct passwd> ();
   XrdxCastor2OfsFS.clientadmintable = new XrdOucHash<XrdxCastor2ClientAdmin> ();

   if ( XrdxCastor2OfsFS.Configure(OfsEroute) ) return 0;
// Initialize the target storage system
//
//   if (!(XrdOfsOss = (XrdOssSys*) XrdOssGetSS(lp, configfn, XrdxCastor2OfsFS.OssLib))) return 0;

// All done, we can return the callout vector to these routines.
//
   XrdOfsFS = &XrdxCastor2OfsFS;
   return &XrdxCastor2OfsFS;
}
}

/******************************************************************************/
/*                         C o n f i g  u r e                                 */
/******************************************************************************/
int XrdxCastor2Ofs::Configure(XrdSysError& Eroute)
{
  char *var;
  const char *val;
  int  cfgFD;
  // extract the manager from the config file
  XrdOucStream Config(&Eroute, getenv("XRDINSTANCE"));

  XrdxCastor2OfsFS.doChecksumStreaming = true;
  XrdxCastor2OfsFS.doChecksumUpdates   = false;
  XrdxCastor2OfsFS.doPOSC = false;
  XrdxCastor2OfsFS.ThirdPartyCopy = true;
  XrdxCastor2OfsFS.ThirdPartyCopySlots = 5;
  XrdxCastor2OfsFS.ThirdPartyCopySlotRate = 25;
  XrdxCastor2OfsFS.ThirdPartyCopyStateDirectory = "/var/log/xroot/server/transfer";

  Procfilesystem = "/tmp/xcastor2-ofs/";
  ProcfilesystemSync = false;

  if( !ConfigFN || !*ConfigFN) {
    // this error will be reported by XrdxCastor2OfsFS.Configure
  } else {
    // Try to open the configuration file.
    //
    if ( (cfgFD = open(ConfigFN, O_RDONLY, 0)) < 0)
      return Eroute.Emsg("Config", errno, "open config file fn=", ConfigFN);

    Config.Attach(cfgFD);
    // Now start reading records until eof.
    //
    
    while((var = Config.GetMyFirstWord())) {
      if (!strncmp(var, "xcastor2.",9)) {
	var += 9;
	if (!strcmp("procuser",var)) {
	  if ((val = Config.GetWord())) {
	    XrdOucString sval = val;
	    procUsers.Push_back(sval);
	  }
	}

	if (!strcmp("posc",var)) {
	  if ((val = Config.GetWord())) {
	    XrdOucString sval = val;
	    if ( (sval == "true") || (sval == "1") || (sval == "yes")) {
	      XrdxCastor2OfsFS.doPOSC = true;
	    }
	  }
	}

	if (!strcmp("checksum",var)) {
	  if ((val = Config.GetWord())) {
	    XrdOucString sval = val;
	    if (sval == "always") {
	      XrdxCastor2OfsFS.doChecksumStreaming = true;
	      XrdxCastor2OfsFS.doChecksumUpdates   = true;
	    } else {
	      if (sval == "never") {
		XrdxCastor2OfsFS.doChecksumStreaming = false;
		XrdxCastor2OfsFS.doChecksumUpdates   = false;
	      } else {
		if (sval == "streaming") {
		  XrdxCastor2OfsFS.doChecksumStreaming = true;
		  XrdxCastor2OfsFS.doChecksumUpdates   = false;
		} else {
		  Eroute.Emsg("Config","argument for checksum invalid. Specify xcastor2.checksum [always|never|streaming]");
		  exit(-1);
		}
	      }
	    }
	  }
	}

	if (!strcmp("proc",var)) 
	  if (!(val = Config.GetWord())) {
	    Eroute.Emsg("Config","argument for proc invalid.");exit(-1);
	  } else {
	    Procfilesystem = val;
	    if ((val = Config.GetWord())) {
	      if ((!strcmp(val, "sync")) || (!strcmp(val,"async"))) {
		if (!strcmp(val,"sync")) ProcfilesystemSync=true;
		if (!strcmp(val,"async")) ProcfilesystemSync=false;
	      } else {
		Eroute.Emsg("Config","argument for proc invalid. Specify xcastor2.proc <path> [sync|async]");
		exit(-1);
	      }
	    }

	    while (Procfilesystem.replace("//","/")) {}

	    if (ProcfilesystemSync) 
	      Eroute.Say("=====> xcastor2.proc: ", Procfilesystem.c_str()," sync");
	    else 
	      Eroute.Say("=====> xcastor2.proc: ", Procfilesystem.c_str()," async");
	  }


	if (!strcmp("ratelimiter",var)) {
	  if ((val = Config.GetWord())) {
	    if ((!strcmp(val,"1")) || (!strcmp(val,"yes")) || (!strcmp(val,"true"))) {
	      RunRateLimiter=true;
	    }
	  }
	}

	if (!strcmp("thirdparty",var)) {
	  if ((val = Config.GetWord())) {
	    if ((!strcmp(val,"1")) || (!strcmp(val,"yes")) || (!strcmp(val,"true"))) {
	      ThirdPartyCopy=true;
	    }  else {
	      ThirdPartyCopy=false;
	    }
	  }
	}

	if (!strcmp("thirdparty.slots",var)) {
	  if ((val = Config.GetWord())) {
	    int slots = atoi(val);
	    if (slots<1)   slots = 1;
	    if (slots>128) slots = 128;
	    ThirdPartyCopySlots = slots;
	  }
	}

	if (!strcmp("thirdparty.slotrate",var)) {
	  if (( val = Config.GetWord())) {
	    int rate = atoi(val);
	    if (rate < 1) rate = 1;
	    ThirdPartyCopySlotRate = rate;
	  }
	}
	if (!strcmp("thirdparty.statedirectory",var)) {
	  if (( val = Config.GetWord())) {
	    ThirdPartyCopyStateDirectory = val;
	  }
	}
      }
    }
    
    Config.Close();
  }

  XrdOucString makeProc;
  makeProc="mkdir -p ";
  makeProc+=Procfilesystem;
  makeProc+="/proc";
#ifdef XFS_SUPPORT
  makeProc+="/xfs";
#endif
  makeProc+= "; chown stage.st "; makeProc+=Procfilesystem; makeProc+="/proc";

  system(makeProc.c_str());

  if ( access(Procfilesystem.c_str(),R_OK | W_OK )) {
    Eroute.Emsg("Config","Cannot use given third proc directory ",Procfilesystem.c_str());
    exit(-1);
    
  }

  if (XrdxCastor2OfsFS.ThirdPartyCopy) {
    XrdOucString makeStateDirectory;
    makeStateDirectory="mkdir -p "; 
    makeStateDirectory+= ThirdPartyCopyStateDirectory.c_str();
    makeStateDirectory+= "; chown stage.st ";
    makeStateDirectory+= ThirdPartyCopyStateDirectory.c_str();
    
    system(makeStateDirectory.c_str());
    
    if ( access(ThirdPartyCopyStateDirectory.c_str(),R_OK | W_OK )) {
      Eroute.Emsg("Config","Cannot use given third party state directory ",ThirdPartyCopyStateDirectory.c_str());
      exit(-1);
    }
    // set the configuration values
    XrdxCastor2OfsFS.ThirdPartySetup(XrdxCastor2OfsFS.ThirdPartyCopyStateDirectory.c_str(), ThirdPartyCopySlots, ThirdPartyCopySlotRate);
    XrdxCastor2OfsFS.ThirdPartySetup(XrdxCastor2OfsFS.ThirdPartyCopyStateDirectory.c_str(), ThirdPartyCopySlots, ThirdPartyCopySlotRate);
    XrdxCastor2Ofs::ThirdPartyCleanupOnRestart();
  }

  for (int i=0; i< procUsers.GetSize(); i++) {
    Eroute.Say("=====> xcastor2.procuser: ", procUsers[i].c_str());
  }

  if (RunRateLimiter) {
    Eroute.Say("=====> xcastor2.ratelimiter: yes","","");
  } else {
    Eroute.Say("=====> xcastor2.ratelimiter: no ","","");
  }

  if (doPOSC) {
    Eroute.Say("=====> xcastor2.posc: yes","","");
  } else {
    Eroute.Say("=====> xcastor2.posc: no ","","");
  }

  if (ThirdPartyCopy) {
    Eroute.Say("=====> xcastor2.thirdparty: yes","","");
    XrdOucString slots=""; slots+= ThirdPartyCopySlots;
    Eroute.Say("=====> xcastor2.thirdparty.slots: ", slots.c_str(),"");
    XrdOucString rate =""; rate += ThirdPartyCopySlotRate; rate += " Mb/s";
    Eroute.Say("=====> xcastor2.thirdparty.rate: ", rate.c_str(),"");
    Eroute.Say("=====> xcastor2.thirdparty.statedirectory: ", ThirdPartyCopyStateDirectory.c_str());
  } else {
    Eroute.Say("=====> xcastor2.thirdparty: no ","","");
  }


  // read in /proc variables at startup time to reset to previous values
  ReadAllProc();
  UpdateProc("*");
  ReadAllProc();
  // start eventually the rate limiter thread
  if (RunRateLimiter) {
    XrdxCastor2OfsRateLimit* Limiter = new XrdxCastor2OfsRateLimit();
    if (!Limiter->Init(Eroute)) {
      int retc = ENOMEM;
      Eroute.Emsg("Configure", retc, "create rate limiter thread");
      _exit(3);
    } else {
      Eroute.Say("++++++ Xcastor2Fs RateLimiter running. ","","");
    }
  }

  int rc = XrdOfs::Configure(Eroute);
  
  // we need to set the effective user for all the XrdClient's used to issue 'prepares' to redirectors or third-party transfers
  setenv("XrdClientEUSER","stage",1);
  return rc;
}

int
XrdxCastor2OfsFile::Unlink() {
  EPNAME("Unlink");

  if ( (!XrdxCastor2OfsFS.doPOSC) || (!isRW) ) {
    return SFS_OK;
  }

  if (IsThirdPartyStreamCopy) {
    return SFS_OK;
  }

  XrdOucString preparename = envOpaque->Get("castor2fs.sfn");
  XrdOucString secuid      = envOpaque->Get("castor2fs.client_sec_uid");
  XrdOucString secgid      = envOpaque->Get("castor2fs.client_sec_gid");

  ZTRACE(open,"unlink: lfn=" << preparename.c_str());

  // don't deal with proc requests
  if (procRequest != kProcNone) 
    return SFS_OK;
      
  const char *tident = error.getErrUser();

  XrdOucString mdsaddress = "root://";
  mdsaddress += envOpaque->Get("castor2fs.manager");
  mdsaddress += "//dummy";

  XrdxCastor2OfsFS.MetaMutex.Lock();

  XrdxCastor2ClientAdmin* mdsclient;
  if ( (mdsclient = XrdxCastor2OfsFS.clientadmintable->Find(mdsaddress.c_str())) ) {
  } else {
    mdsclient = new XrdxCastor2ClientAdmin(mdsaddress.c_str());
    mdsclient->GetAdmin()->Connect();
    XrdxCastor2OfsFS.clientadmintable->Add(mdsaddress.c_str(),mdsclient);
  }

  if (!mdsclient) {
    if (mdsclient) {
      XrdxCastor2OfsFS.clientadmintable->Del(mdsaddress.c_str());
    }
    XrdxCastor2OfsFS.MetaMutex.UnLock();
    return XrdxCastor2OfsFS.Emsg(epname,error, EHOSTUNREACH, "connect to the mds host (normally the manager) ", "");
  }

  preparename += "?";

  preparename += "&unlink=true";
  
  preparename += "&reqid=";
  preparename += reqid;
  preparename += "&stagehost=";
  preparename += stagehost;
  preparename += "&serviceclass=";
  preparename += serviceclass;
  preparename += "&uid=";
  preparename += secuid;
  preparename += "&gid=";
  preparename += secgid;

  ZTRACE(debug,"informing about " << preparename.c_str());

  if (!mdsclient->GetAdmin()->Prepare(preparename.c_str(),0,0)) {
    XrdxCastor2OfsFS.clientadmintable->Del(mdsaddress.c_str());
    XrdxCastor2OfsFS.MetaMutex.UnLock();
    return XrdxCastor2OfsFS.Emsg(epname,error,ESHUTDOWN,"unlink",FName());
  }

  XrdxCastor2OfsFS.MetaMutex.UnLock();
  return SFS_OK;
}


int
XrdxCastor2OfsFile::UpdateMeta() {
  EPNAME("UpdateMeta");

  if (IsThirdPartyStreamCopy) {
    return SFS_OK;
  }

  XrdOucString preparename = envOpaque->Get("castor2fs.sfn");

  ZTRACE(open,"haswrite=" <<hasWrite << " firstWrite="<< firstWrite << " 3rd-party="<< IsThirdPartyStream << "/" << IsThirdPartyStreamCopy << " lfn=" << preparename.c_str());



  // don't deal with proc requests
  if (procRequest != kProcNone) 
    return SFS_OK;
      
  const char *tident = error.getErrUser();

  XrdOucString mdsaddress = "root://";
  mdsaddress += envOpaque->Get("castor2fs.manager");
  mdsaddress += "//dummy";

  XrdxCastor2OfsFS.MetaMutex.Lock();

  XrdxCastor2ClientAdmin* mdsclient;
  if ( (mdsclient = XrdxCastor2OfsFS.clientadmintable->Find(mdsaddress.c_str())) ) {
  } else {
    mdsclient = new XrdxCastor2ClientAdmin(mdsaddress.c_str());
    mdsclient->GetAdmin()->Connect();
    XrdxCastor2OfsFS.clientadmintable->Add(mdsaddress.c_str(),mdsclient);
  }

  if (!mdsclient) {
    if (mdsclient) {
      XrdxCastor2OfsFS.clientadmintable->Del(mdsaddress.c_str());
    }
    XrdxCastor2OfsFS.MetaMutex.UnLock();
    return XrdxCastor2OfsFS.Emsg(epname,error, EHOSTUNREACH, "connect to the mds host (normally the manager) ", "");
  }

  preparename += "?";

  if (firstWrite && hasWrite) 
    preparename += "&firstbytewritten=true";
  else {
    XrdxCastor2OfsFS.MetaMutex.UnLock();
    return SFS_OK;
  }
  
  preparename += "&reqid=";
  preparename += reqid;
  preparename += "&stagehost=";
  preparename += stagehost;
  preparename += "&serviceclass=";
  preparename += serviceclass;

  ZTRACE(debug,"informing about " << preparename.c_str());

  if (!mdsclient->GetAdmin()->Prepare(preparename.c_str(),0,0)) {
    XrdxCastor2OfsFS.clientadmintable->Del(mdsaddress.c_str());
    XrdxCastor2OfsFS.MetaMutex.UnLock();
    return XrdxCastor2OfsFS.Emsg(epname,error,ESHUTDOWN,"update mds size/modtime",FName());
  }

  XrdxCastor2OfsFS.MetaMutex.UnLock();
  return SFS_OK;
}


/******************************************************************************/
/*                         x C a s t o r O f s F i l e                        */
/******************************************************************************/
/*                         o p e n                                            */
/******************************************************************************/
int          
XrdxCastor2OfsFile::open(const char                *path,
			XrdSfsFileOpenMode   open_mode,
			mode_t               create_mode,
			const XrdSecEntity        *client,
			const char                *opaque)
{
  EPNAME("open");
  // the OFS open is catched to set the access/modify time in the nameserver
  //time_t now = time(NULL);
  procRequest = kProcNone;

  const char *tident = error.getErrUser();
  char *val=0;

  XrdxCastor2Timing opentiming("ofsf::open");
  
  TIMING(OfsTrace,"START",&opentiming);

  XrdOucString spath = path;
  isRW=false;
  isTruncate=false;
  castorlfn = path;
  ZTRACE(open,"castorlfn=" << castorlfn.c_str());
  XrdOucString newpath="";

  XrdOucString newopaque = opaque;
  // if thereis explicit user opaque information we find two ?, so we just replace it with a seperator.
  
  newopaque.replace("?","&");
  newopaque.replace("&&","&");

  // check if there are several redirection tokens
  int firstpos=0;
  int lastpos=0;
  int newpos=0;
  firstpos = newopaque.find("castor2fs.sfn",0);
  lastpos = firstpos+1;
  while ( (newpos = newopaque.find("castor2fs.sfn",lastpos) ) != STR_NPOS) {
    lastpos = newpos+1;
  }

  // erase from the beginning to the last token start
  if (lastpos>(firstpos+1)) 
    newopaque.erase(firstpos,lastpos-2-firstpos);
  
  int triedpos = newopaque.find("tried=");
  if (triedpos != STR_NPOS) {
    newopaque.erase(triedpos);
  }

  if (newopaque.find("castor2fs.signature=") == STR_NPOS) {
    // this is a backdoor for the tape to update files
    firstWrite = false;
  }

  // this prevents 'clever' users faking internal opaque information 

  newopaque.replace("ofsgranted=", "notgranted=");
  newopaque.replace("source=","nosource=");

  envOpaque = new XrdOucEnv(newopaque.c_str());
  newpath = envOpaque->Get("castor2fs.pfn1");
  if (newopaque.endswith("&")) {
    newopaque += "source="; 
  } else {
    newopaque += "&source=";
  }

  newopaque+= newpath.c_str();
  if (!envOpaque->Get("target")) {
    if (newopaque.endswith("&")) {
      newopaque += "target="; 
    } else {
      newopaque += "&target=";
    }
    newopaque += "&target="; newopaque+= newpath.c_str();
  }

  // store opaque information

  if (envOpaque->Get("castor2ofsproc")) {
    // that is strange ;-) ....
    newopaque.replace("castor2ofsproc","castor2ofsfake");
  }

  XrdOucString verifytag = envOpaque->Get("verifychecksum");
  if (  (verifytag == "false" ) ||
	(verifytag == "0"     ) ||
	(verifytag == "off") ) {
    VerifyChecksum=false;
  }


  open_mode |= SFS_O_MKPTH;
  create_mode|= SFS_O_MKPTH;

  if ( (open_mode & (SFS_O_RDONLY | SFS_O_WRONLY | SFS_O_RDWR |
		     SFS_O_CREAT  | SFS_O_TRUNC) ) != 0) 
    isRW = true;
  
  if ( open_mode & SFS_O_TRUNC )
    isTruncate = true;

  if (spath.beginswith("/proc/")) {

    procRequest = kProcRead;
    if (isRW)
      procRequest = kProcWrite;
    // declare this as a proc path to the authorization plugin
    newopaque += "&";
    newopaque += "castor2ofsproc=true";
  } else {
    // deny this as a proc path to the authorization plugin
    newopaque += "&";
    newopaque += "castor2ofsproc=false";
  }
	

  if (!procRequest) {
    // send an acc/mod update to the server
    
    const char* streamrate;
    if ((streamrate=envOpaque->Get("streamin"))) {
      IsAdminStream = true;
      WriteRateLimit = atoi(streamrate);
    } else {
      WriteRateLimit = 99999;
    }

    if ((streamrate=envOpaque->Get("streamout"))) {
      IsAdminStream = true;
      ReadRateLimit = atoi(streamrate);
    } else {
      ReadRateLimit = 99999;
    }

    RateTimer.Reset();

    switch(open_mode & (SFS_O_RDONLY | SFS_O_WRONLY | SFS_O_RDWR |
			SFS_O_CREAT  | SFS_O_TRUNC)) {
      
    case SFS_O_RDONLY: break;
      break;
    default:
      break;
    }
  } else {
    // reroot the /proc location
    XrdOucString prependedspath = XrdxCastor2OfsFS.Procfilesystem;
    prependedspath += "/";
    prependedspath += spath;
    while (prependedspath.replace("//","/")) {};

    // check the allowed proc users
    // this is not a high performance implementation but the usage of /proc is very rare anyway
    // 
    // clients are allowed to read/modify /proc, when they appear in the configuration file f.e. as
    // -> xcastor2.procUsers xrootd@pcitmeta.cern.ch
    // where the user name in front of @ is the UID name on the client side
    // and the host name after the @ is the hostname without domain

    XrdOucString reducedTident,user, stident;
    reducedTident=""; user = ""; stident = tident;
    int dotpos = stident.find(".");
    reducedTident.assign(tident,0,dotpos-1);reducedTident += "@";
    int adpos  = stident.find("@"); user.assign(tident,adpos+1); reducedTident += user;

    bool allowed = false;
    for (int i = 0 ; i < XrdxCastor2OfsFS.procUsers.GetSize(); i++) {
      if (XrdxCastor2OfsFS.procUsers[i] == reducedTident) {
	allowed = true;
	break;
      }
    }

    if (!allowed) {
      return XrdxCastor2OfsFS.Emsg(epname,error,EPERM,"allow access to /proc/ fs in xrootd",path);
    }
    if (procRequest == kProcWrite) {
      open_mode = SFS_O_RDWR | SFS_O_TRUNC;
      prependedspath += ".__proc_update__";
    } else {
      // depending on the desired file, we update the information in that file
      if (!XrdxCastor2OfsFS.UpdateProc(prependedspath.c_str()))
      	return XrdxCastor2OfsFS.Emsg(epname,error,EIO,"update /proc information",path);
    }
     
    // we have to open the rerooted proc path!
    int rc = XrdOfsFile::open(prependedspath.c_str(),open_mode,create_mode,client,newopaque.c_str());
    return rc;
  }

  TIMING(OfsTrace,"PROCBLOCK",&opentiming);


  /////////////////////////////////////////////////////////////////////////////////////////////
  // section to deal with third party transfer streams
  if (XrdxCastor2OfsFS.ThirdPartyCopy && ( val = envOpaque->Get("xferuuid"))) {
    IsThirdPartyStream = true;
    // look at third party transfers
    // do some checks to allow to bypass the authorization module
    val = envOpaque->Get("xfercmd");
    XrdOucString xcmd = val;
    if (xcmd == "copy") {
      IsThirdPartyStreamCopy=true;
      // we have to get the physical mapping from the Map
      XrdOucString* mappedpath=0;
      XrdxCastor2OfsFS.FileNameMapLock.Lock();
      mappedpath = XrdxCastor2OfsFS.FileNameMap.Find(path);
      XrdxCastor2OfsFS.FileNameMapLock.UnLock();
      if (!mappedpath) {
	// the user has not anymore the file open, for the moment we forbid this!
	return XrdxCastor2OfsFS.Emsg(epname,error,EPERM,"open transfer stream - destination file is not open anymore",newpath.c_str());
      } else {
	newpath = mappedpath->c_str();
	// place it for the close statement
	envOpaque->Put("castor2fs.pfn1",newpath.c_str());
      }
      
      // determine if the file exists and has size 0
      if (!(XrdOfsOss->Stat(newpath.c_str(), &statinfo))) {
	// this must be a copy write stream, and the file has to exist with size 0
	if ( isRW && (!statinfo.st_size)) {
	  newopaque+= "&ofsgranted=yes";
	} // else: this will file latest during the open authorization
      } // else: this will file latest during the open authorization
    } else {
      // store a mapping for the third party copy who doesn't know the PFN
      XrdxCastor2OfsFS.FileNameMapLock.Lock();
      XrdxCastor2OfsFS.FileNameMap.Add(path,new XrdOucString(newpath.c_str()));
      XrdxCastor2OfsFS.FileNameMapLock.UnLock();
    }
  }
  //
  /////////////////////////////////////////////////////////////////////////////////////////////
  

      
  int retc;
  char* v=0;
  if ((v=envOpaque->Get("createifnotexist"))) {
    if (atoi(v)) {    
      if (!isRW) {
	if ((retc = XrdOfsOss->Stat(newpath.c_str(), &statinfo))) {
	  // if this does not work we get the error later in any case!
	  creat(newpath.c_str(),S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
	}
      }
    }
  }

  if ((retc = XrdOfsOss->Stat(newpath.c_str(), &statinfo))) {
    // file does not exist, keep the create flag (unless this is an update but the file didn't exist on the disk server
    if ( envOpaque->Get("castor2fs.accessop") && ( atoi( envOpaque->Get("castor2fs.accessop")) == AOP_Update) ) {
      firstWrite = true;
    } else {
      firstWrite=false;
    }
  } else {
    if (open_mode & SFS_O_CREAT) 
      open_mode -= SFS_O_CREAT;

    // the file exists and it is in write mode = update

    // hmm... not this one
    // try to get a checksum from the filesystem
    char diskckSumbuf[32+1];
    char diskckSumalg[32];
    diskckSumbuf[0]=diskckSumalg[0]=0;

    // get existing checksum - we don't check errors here
    int nattr=0;
    nattr = getxattr(newpath.c_str(),"user.castor.checksum.type",diskckSumalg,sizeof(diskckSumalg));
    if (nattr) diskckSumalg[nattr] = 0;
    nattr = getxattr(newpath.c_str(),"user.castor.checksum.value",diskckSumbuf,sizeof(diskckSumbuf));
    if (nattr) diskckSumbuf[nattr] = 0;
    DiskChecksum = diskckSumbuf;
    DiskChecksumAlgorithm = diskckSumalg;
    ZTRACE(debug,"Checksum-Algorithm: " << DiskChecksumAlgorithm.c_str() << " Checksum: " << DiskChecksum.c_str());
  }

  uid_t sec_uid = 0;
  uid_t sec_gid = 0;

  if (envOpaque->Get("castor2fs.client_sec_uid")) {
    sec_uid = atoi(envOpaque->Get("castor2fs.client_sec_uid"));
  }

  if (envOpaque->Get("castor2fs.client_sec_gid")) {
    sec_gid = atoi(envOpaque->Get("castor2fs.client_sec_gid"));
  }
  
  // extract the stager information
  val = envOpaque->Get("castor2fs.pfn2");

  if (!(XrdxCastor2OfsFS.ThirdPartyCopy) || (!IsThirdPartyStreamCopy)) {
    if (!(val) || (!strlen(val))) {
      return XrdxCastor2OfsFS.Emsg(epname,error,ESHUTDOWN,"reqid/pfn2 is missing",FName());
    }
  } else {
    // third party streams don't have any req id
    val = "";
  }

  XrdOucString reqtag = val;
  reqid ="0";
  stagehost ="none";
  serviceclass ="none";
  stagerjobport = 0;
  SjobUuid="";

  int pos1;
  int pos2;
  int pos3;
  int pos4;

  // syntax is reqid: <reqid:stagerhost:serviceclass:stagerjobport:stagerjobuuid

  if ( (pos1 = reqtag.find(":") ) != STR_NPOS) {
    reqid.assign(reqtag,0,pos1-1);
    if ( ( pos2 = reqtag.find(":",pos1+1)) != STR_NPOS)  {
      stagehost.assign(reqtag,pos1+1,pos2-1);
      if  ( ( pos3 = reqtag.find(":",pos2+1)) != STR_NPOS) {
	serviceclass.assign(reqtag,pos2+1,pos3-1);
	if ( ( pos4 = reqtag.find(":",pos3+1)) != STR_NPOS) {
	  XrdOucString sport;
	  sport.assign(reqtag,pos3+1,pos4-1);
	  stagerjobport = atoi(sport.c_str());
	  SjobUuid.assign(reqtag,pos4+1);
	}
      }
    }
  }

  ZTRACE(debug,"StagerJob uuid: " << SjobUuid.c_str() << " port: " << stagerjobport);
  StagerJob = new XrdxCastor2Ofs2StagerJob(SjobUuid.c_str(), stagerjobport);


  if (!IsThirdPartyStreamCopy) {
    // send the sigusr1 to the local xcastor2job to signal the open
    if (!StagerJob->Open()) {
      // the open failed
      TRACES("error: open => couldn't run the Open towards StagerJob: reqid=" <<reqid <<" stagerjobport=" <<stagerjobport << " stagerjobuuid=" << SjobUuid.c_str());
      return XrdxCastor2OfsFS.Emsg(epname,error, EIO, "signal open to the corresponding LSF stagerjob", "");
    }

    if (IsThirdPartyStream) {
      if (isRW) {
	hasWrite=true;
	// here we send the update for the 1st byte written
	if (UpdateMeta()!=SFS_OK) {
	  return SFS_ERROR;
	}
	firstWrite = false;
      }
    }
  }

  TIMING(OfsTrace,"FILEOPEN",&opentiming);
  int rc = XrdOfsFile::open(newpath.c_str(),open_mode,create_mode,client,newopaque.c_str()); 
  /////////////////////////////////////////////////////////////////////////////////////////////
  // section to deal with third party transfer streams
  if (rc == SFS_OK) {
    isOpen = true;
    if (IsThirdPartyStream) {
      TIMING(OfsTrace,"THIRDPARTY",&opentiming);

      ZTRACE(open,"Copy Slots scheduled: " << XrdTransferManager::TM()->ScheduledTransfers.Num() << " avail: " << XrdxCastor2OfsFS.ThirdPartyCopySlots);

      // evt. change the thread parameters defined on the /proc state
      XrdxCastor2OfsFS.ThirdPartySetup(XrdxCastor2OfsFS.ThirdPartyCopyStateDirectory.c_str(), XrdxCastor2OfsFS.ThirdPartyCopySlots, XrdxCastor2OfsFS.ThirdPartyCopySlotRate);

      // execute the third party managing function
      rc = ThirdPartyTransfer(path,open_mode,create_mode,client,newopaque.c_str());

      if (rc>0) {
	// send a stall to the client, the transfer is already detached ....
	XrdOfsFile::close();
	return retc;
      }
    }
    if (isTruncate) 
      truncate(0);
  }
  //
  /////////////////////////////////////////////////////////////////////////////////////////////


  if (IsThirdPartyStreamCopy) {
    // retrieve the req id from the opaque info
    
    // we wait for the stagerJob PID from x2castorjob
    // try to read the pid file
    if (!Transfer) {
      // uups this is fatal and strange
      return XrdxCastor2OfsFS.Emsg(epname,error,EINVAL,"open attach any transfer as a copy stream; ",newpath.c_str());
    }
    
    XrdOucString reqtag = Transfer->Options->Get("castor2fs.pfn2");
    reqid ="0";
    int pos1;
    if ( (pos1 = reqtag.find(":") ) != STR_NPOS) {
      reqid.assign(reqtag,0,pos1-1);
    }
  }

  opentiming.Print(OfsTrace);
  TIMING(OfsTrace,"DONE",&opentiming);
  return rc;
}

/******************************************************************************/
/*                         c l o s e                                          */
/******************************************************************************/
int
XrdxCastor2OfsFile::close()
{
  EPNAME("close");
  //time_t now = time(NULL);
  int rc = SFS_OK;

  if (IsClosed)
    return true;

  IsClosed=true;
  XrdOucString spath = FName();

  if (procRequest == kProcWrite) {
    sync();
    XrdOucString newpath = spath;
    newpath.erase(".__proc_update__");
    
    XrdxCastor2OfsFS.rem(newpath.c_str(),error,0,"");
    // we update the internal values e.g. the rate limiter should see the changed settings immedeatly!
    int rc =  ::rename(spath.c_str(),newpath.c_str()); //,error,0,"","");
    //    printf("rc is %d %s->%s\n",rc, spath.c_str(),newpath.c_str());
    XrdxCastor2OfsFS.rem(spath.c_str(),error,0,"");
    XrdxCastor2OfsFS.ReadAllProc();
    XrdxCastor2OfsFS.UpdateProc(newpath.c_str());
    XrdOfsFile::close();

    return rc;
  } else {
    XrdxCastor2OfsFS.ReadFromProc("trace");
  }

  char ckSumbuf[32+1];
  sprintf(ckSumbuf,"%x",adler);
  char *ckSumalg="ADLER32";

  XrdOucString newpath="";
  newpath = envOpaque->Get("castor2fs.pfn1");
  
  if (hasWrite) {
    if (XrdxCastor2OfsFS.doChecksumUpdates && (!hasadler)) {
      XrdxCastor2Timing checksumtiming("ofsf::checksum");
      TIMING(OfsTrace,"START",&checksumtiming);
      char chcksumbuf[64* 1024];
      // we just quickly rescan the file and recompute the checksum
      adler = adler32(0L, Z_NULL, 0);adleroffset=0;
      XrdSfsFileOffset checkoffset=0;
      XrdSfsXferSize checksize=0;
      XrdSfsFileOffset checklength=0;
      while ((checksize = read(checkoffset,chcksumbuf,sizeof(chcksumbuf)))>0) {
	adler = adler32(adler,(const Bytef*)chcksumbuf,checksize);
	checklength+= checksize;
	checkoffset+= checksize;
      }
      TIMING(OfsTrace,"STOP",&checksumtiming);
      checksumtiming.Print(OfsTrace);
      sprintf(ckSumbuf,"%x",adler);
      ZTRACE(close,"Recalculated Checksum [" << checklength << " bytes] : " << ckSumbuf);
      hasadler = true;
    } else {
      if ((XrdxCastor2OfsFS.doChecksumStreaming || XrdxCastor2OfsFS.doChecksumUpdates) && hasadler) {
	sprintf(ckSumbuf,"%x",adler);
	ZTRACE(close,"Streaming    Checksum [" << FileWriteBytes << " bytes] : " << ckSumbuf);
      }
    }
    
    if ((XrdxCastor2OfsFS.doChecksumStreaming || XrdxCastor2OfsFS.doChecksumUpdates) && hasadler) {
      // only the real copy stream set's checksums, not the fake open
      if ( (!IsThirdPartyStream) || (IsThirdPartyStream && IsThirdPartyStreamCopy)) {
	if (setxattr(newpath.c_str(),"user.castor.checksum.type",ckSumalg, strlen(ckSumalg),0)) {
	  XrdxCastor2OfsFS.Emsg(epname,error, EIO, "set checksum type", "");
	  rc=SFS_ERROR;
	  // anyway this is not returned to the client
	} else {
	  if (setxattr(newpath.c_str(),"user.castor.checksum.value",ckSumbuf,strlen(ckSumbuf),0)) {
	    XrdxCastor2OfsFS.Emsg(epname,error, EIO, "set checksum", "");
	    rc=SFS_ERROR;
	    // anyway this is not returned to the client
	  }
	}
      }
    } else {
      if ((!XrdxCastor2OfsFS.doChecksumUpdates) && (!hasadler)) {
	// remove checksum - we don't check errors here
	removexattr(newpath.c_str(),"user.castor.checksum.type");
	removexattr(newpath.c_str(),"user.castor.checksum.value");
      }
    }
  }

  if (XrdxCastor2OfsFS.ThirdPartyCopy) {
    if (IsThirdPartyStream) {
      // terminate the transfer to set the final state and wait for the termination
      rc = ThirdPartyTransferClose(envOpaque->Get("xferuuid"));
	
      if (!IsThirdPartyStreamCopy) {
	// remove mappings for non third party streams
	XrdxCastor2OfsFS.FileNameMapLock.Lock();
	XrdxCastor2OfsFS.FileNameMap.Del(castorlfn.c_str());
	XrdxCastor2OfsFS.FileNameMapLock.UnLock();  
	// comment: if there is no mapping defined, no 3rd party copy can be run towards this file
      }
    }
    XrdOfsFile::close();
  }

  // inform the StagerJob
  if (!IsThirdPartyStreamCopy) {
    if (viaDestructor) {
      // this means the file was not properly closed
      Unlink();
    }    
    
    if (StagerJob && (!StagerJob->Close(true, hasWrite))) {
      // for the moment, we cannot send this back, but soon we will
      ZTRACE(close,"StagerJob close failed: got rc=" << StagerJob->ErrCode << " msg=" <<StagerJob->ErrMsg);
      XrdxCastor2OfsFS.Emsg(epname,error, StagerJob->ErrCode, StagerJob->ErrMsg.c_str(), "");
      rc=SFS_ERROR;
    } 
  }

  return rc;
}

/******************************************************************************/
/*                   v e r i f y c h e c k s u m                              */
/******************************************************************************/
bool
XrdxCastor2OfsFile::verifychecksum() {
  EPNAME("verifychecksum");
  bool rc = true;
  
  XrdOucString CalcChecksum;
  XrdOucString CalcChecksumAlgorithm;
  
  if ( (DiskChecksum!="") && 
       (DiskChecksumAlgorithm !="") &&
       VerifyChecksum ) {
    char ckSumbuf[32+1];
    sprintf(ckSumbuf,"%x",adler);
    CalcChecksum = ckSumbuf;
    CalcChecksumAlgorithm = "ADLER32";
    if (CalcChecksum != DiskChecksum) {
      XrdxCastor2OfsFS.Emsg(epname,error, EIO, "verify checksum - checksum wrong!", "");
      rc = false;
    }
    
    if (CalcChecksumAlgorithm != DiskChecksumAlgorithm) {
      XrdxCastor2OfsFS.Emsg(epname,error, EIO, "verify checksum - checksum type wrong!","");
      rc = false;
    }
    if (!rc) {
      ZTRACE(read,"Checksum ERROR: " << CalcChecksum.c_str() << " != " << DiskChecksum.c_str() << " [ " << CalcChecksumAlgorithm.c_str() << " <=> " << DiskChecksumAlgorithm.c_str() << " ]");
    } else {
      ZTRACE(read,"Checksum OK   : " << CalcChecksum.c_str());
    }
  }
  return rc;
}

/******************************************************************************/
/*                         r e a d                                            */
/******************************************************************************/
int
XrdxCastor2OfsFile::read(XrdSfsFileOffset   fileOffset,   // Preread only
		    XrdSfsXferSize     amount)
{
  int rc = XrdOfsFile::read(fileOffset,amount);
  if (rc>0) {if(!IsAdminStream)ADDTOTALREADBYTES(rc) else ADDTOTALSTREAMREADBYTES(rc);FileReadBytes+=rc;}

  hasadler = false;

  if (IsAdminStream) {
    DOREADDELAY();
  } else 
    if (XrdxCastor2OfsFS.ReadDelay) XrdSysTimer::Wait(XrdxCastor2OfsFS.ReadDelay);

  return rc;
}

/******************************************************************************/
/*                         r e a d                                            */
/******************************************************************************/
XrdSfsXferSize 
XrdxCastor2OfsFile::read(XrdSfsFileOffset   fileOffset,
		      char              *buffer,
		      XrdSfsXferSize     buffer_size)
{
  // if we once got an adler checksum error, we fail all reads.
  if (hasadlererror) {
    return SFS_ERROR;
  }

  int rc = XrdOfsFile::read(fileOffset,buffer,buffer_size);

  // computation of adler checksum - we disable it if seeks happen
  if (fileOffset != adleroffset) {
    hasadler=false;
  }
  
  if (hasadler && XrdxCastor2OfsFS.doChecksumStreaming) {
    adler = adler32(adler, (const Bytef*) buffer, rc);
    if (rc>0)
      adleroffset += rc;
  }

  if (rc>0) {if(!IsAdminStream)ADDTOTALREADBYTES(buffer_size) else ADDTOTALSTREAMREADBYTES(buffer_size);FileReadBytes+=rc;}
  
  if (IsAdminStream) {
    DOREADDELAY();
  } else
    if (XrdxCastor2OfsFS.ReadDelay) XrdSysTimer::Wait(XrdxCastor2OfsFS.ReadDelay);
  
  if (hasadler && (fileOffset+buffer_size >= statinfo.st_size)) {
    // invoke the checksum verification
    if (!verifychecksum()) {
      hasadlererror=true;
      rc = SFS_ERROR;
    }
  }
  
  return rc;
}
  
/******************************************************************************/
/*                         r e a d                                            */
/******************************************************************************/
int
XrdxCastor2OfsFile::read(XrdSfsAio *aioparm)
{
  
  int rc = XrdOfsFile::read(aioparm);

  hasadler = false;

  if (rc==SFS_OK) {if(!IsAdminStream)ADDTOTALREADBYTES(aioparm->sfsAio.aio_nbytes) else ADDTOTALSTREAMREADBYTES(aioparm->sfsAio.aio_nbytes);FileReadBytes+=aioparm->sfsAio.aio_nbytes;}
  if (IsAdminStream) {
    DOREADDELAY();
  } else
    if (XrdxCastor2OfsFS.ReadDelay) XrdSysTimer::Wait(XrdxCastor2OfsFS.ReadDelay);

  return rc;
}
  
/******************************************************************************/
/*                         w r i t e                                          */
/******************************************************************************/
XrdSfsXferSize
XrdxCastor2OfsFile::write(XrdSfsFileOffset   fileOffset,
		     const char        *buffer,
		     XrdSfsXferSize     buffer_size)
{
  EPNAME("write");
  // if we are a transfer - check if we are still in running state,
  if (Transfer) {
    if (Transfer->State != XrdTransfer::kRunning) {
      // this transfer has been canceled, we have terminate
      XrdxCastor2OfsFS.Emsg(epname,error, ECANCELED, "write - the transfer has been canceled already ", XrdTransferStateAsString[Transfer->State]);
      // we truncate this file to 0!
      if (!IsClosed) {
	truncate(0);
	sync();
      }
      return SFS_ERROR;
    }
  }

  if (StagerJob && StagerJob->Connected()) {
    // if we have a stagerJob watching, check it is still alive
    if (!StagerJob->StillConnected()) {
      // oh oh, the stagerJob died ... reject any write now
      TRACES("error:write => StagerJob has been canceled");
      // we truncate this file to 0!
      if (!IsClosed) {
	truncate(0);
	sync();
      }
      if (Transfer && (Transfer->State == XrdTransfer::kRunning) ) {
	// if we are a transfer we terminate the transfer and close the file ;-)
	Transfer->SetState(XrdTransfer::kTerminate);
	close();
      }
      XrdxCastor2OfsFS.Emsg(epname,error, ECANCELED, "write - the LSF write slot has been canceled ", "");
      return SFS_ERROR;
    } else {
      ZTRACE(write,"my StagerJob is alive");
    }
  }
  
  int rc = XrdOfsFile::write(fileOffset,buffer,buffer_size);

  if (rc>0) {if (!IsAdminStream)ADDTOTALWRITEBYTES(buffer_size) else ADDTOTALSTREAMWRITEBYTES(buffer_size);FileWriteBytes+= buffer_size;}

  // computation of adler checksum - we disable it if seek happened
  if (fileOffset != adleroffset) {
    hasadler=false;
  }

  if (hasadler && XrdxCastor2OfsFS.doChecksumStreaming) {
    adler = adler32(adler, (const Bytef*) buffer, buffer_size);
    if (rc>0) {
      adleroffset += rc;
    }
  }

  if (IsAdminStream) {
    DOWRITEDELAY();
  } else
    if (XrdxCastor2OfsFS.WriteDelay) XrdSysTimer::Wait(XrdxCastor2OfsFS.WriteDelay);

  hasWrite=true;

  if (firstWrite) {
    if (UpdateMeta()!=SFS_OK) {
      return SFS_ERROR;
    }
    firstWrite=false;
  }

  return rc;
}

/******************************************************************************/
/*                         w r i t e                                          */
/******************************************************************************/
int
XrdxCastor2OfsFile::write(XrdSfsAio *aioparm)
{
  EPNAME("write");

  // if we are a transfer - check if we are still in running state,
  if (Transfer) {
    if (Transfer->State != XrdTransfer::kRunning) {
      // this transfer has been canceled, we have terminate
      XrdxCastor2OfsFS.Emsg(epname,error, ECANCELED, "write - the transfer has been canceld already ", XrdTransferStateAsString[Transfer->State]);
      // we truncate this file to 0!
      if (!IsClosed){
	truncate(0);
	sync();
      }
      return SFS_ERROR;
    }
  }

  if (StagerJob && StagerJob->Connected()) {
    if (!StagerJob->StillConnected()) {
      // oh oh, the stagerJob died ... reject any write now
      TRACES("error:write => StagerJob has been canceled");
      // we truncate this file to 0!
      if (!IsClosed) {
	truncate(0);
	sync();
      }
      if (Transfer && (Transfer->State == XrdTransfer::kRunning) ) {
	// if we are a transfer we terminate the transfer and close the file ;-)
	Transfer->SetState(XrdTransfer::kTerminate);
	close();
      }
      XrdxCastor2OfsFS.Emsg(epname,error, ECANCELED, "write - the LSF write slot has been canceled ", "");
      return SFS_ERROR;
    } else {
      ZTRACE(write,"my StagerJob is alive");
    }
  }
  
  int rc = XrdOfsFile::write(aioparm);

  if (rc==SFS_OK) {if (!IsAdminStream)ADDTOTALWRITEBYTES(aioparm->sfsAio.aio_nbytes) else ADDTOTALSTREAMWRITEBYTES(aioparm->sfsAio.aio_nbytes);FileWriteBytes+=aioparm->sfsAio.aio_nbytes;}

  if (IsAdminStream) {
    DOWRITEDELAY();
  } else
    if (XrdxCastor2OfsFS.WriteDelay) XrdSysTimer::Wait(XrdxCastor2OfsFS.WriteDelay);

  hasWrite=true;

  if (firstWrite) {
    if (UpdateMeta()!=SFS_OK) {
      return SFS_ERROR;
    }
    firstWrite=false;
  }

  return rc;
}

int          
XrdxCastor2OfsFile::sync()
{
  return XrdOfsFile::sync();
}

int          
XrdxCastor2OfsFile::sync(XrdSfsAio *aiop)
{
  return XrdOfsFile::sync();
}

int          
XrdxCastor2OfsFile::truncate(XrdSfsFileOffset   fileOffset)
{
  return XrdOfsFile::truncate(fileOffset);
}

/******************************************************************************/
/*                         x C a s t o r O f s                                */
/******************************************************************************/
/******************************************************************************/
/*                                  r m                                       */
/******************************************************************************/

int
XrdxCastor2Ofs::rem(const char             *path,
		   XrdOucErrInfo          &error,
		   const XrdSecEntity     *client,
		   const char             *info) 
{
  EPNAME("rem");
  const char *tident = error.getErrUser();
  bool allowed = false;
  
  XrdOucString reducedTident,user, stident;
  reducedTident=""; user = ""; stident = tident;
  int dotpos = stident.find(".");
  reducedTident.assign(tident,0,dotpos-1);reducedTident += "@";
  int adpos  = stident.find("@"); user.assign(tident,adpos+1); reducedTident += user;
  
  for (int i = 0 ; i < XrdxCastor2OfsFS.procUsers.GetSize(); i++) {
    if (XrdxCastor2OfsFS.procUsers[i] == reducedTident) {
      allowed = true;
      break;
    }
  }
  
  if (!allowed) {
    return XrdxCastor2OfsFS.Emsg(epname,error,EPERM,"allow access to unlink",path);
  }

  if (unlink(path) ) {
    // we don't report deletion of a not existing file as error
    if (errno != ENOENT) {
      return XrdxCastor2OfsFS.Emsg(epname, error, errno, "remove", path);
    }
  }
  
  XrdOfsHandle::Hide(path);
  return SFS_OK;
}


/******************************************************************************/
/*                         x C a s t o r O f s                                */
/******************************************************************************/
/******************************************************************************/
/*                         StagerJob Interface                                */
/******************************************************************************/


XrdxCastor2Ofs2StagerJob::XrdxCastor2Ofs2StagerJob(const char* sjobuuid, int port) 
{
  Port     = port;
  SjobUuid = sjobuuid;
  Socket   = 0;
  // select timeout is 0
  to.tv_sec  = 0;
  to.tv_usec = 1;
  IsConnected=false;
  ErrCode=0;
  ErrMsg ="undef";
}

XrdxCastor2Ofs2StagerJob::~XrdxCastor2Ofs2StagerJob() 
{
  if (Socket) {
    delete Socket;
    Socket = 0;
  }
  IsConnected=false;
}


bool
XrdxCastor2Ofs2StagerJob::Open()
{
  // if there shouldn't be a port, we don't inform anybode
  if (!Port) 
    return true;

  Socket = new XrdNetSocket();
  if ((Socket->Open("localhost",Port,0,0))<0) {
    return false;
  }

  //int on = 1;
  Socket->setOpts(Socket->SockNum(), XRDNET_KEEPALIVE |XRDNET_DELAY );

  // build IDENT message
  XrdOucString ident = "IDENT "; ident += SjobUuid.c_str(); ident += "\n";
  
  // send IDENT message
  int nwrite = write(Socket->SockNum(),ident.c_str(),ident.length());
  if (nwrite != ident.length()) {
    return false;
  }

  
  IsConnected=true;
  // everything ok
  return true;
}

bool 
XrdxCastor2Ofs2StagerJob::StillConnected() 
{
  int sval;

  if (!IsConnected) 
    return false;

  if (!Socket) 
    return false;

  //char dummy;

  // add the socket to the select set
  FD_ZERO(&check_set);
  FD_SET(Socket->SockNum(),&check_set);
  
  if((sval = select(Socket->SockNum() + 1,&check_set,0,0,&to)) < 0) {
    return false;
  }
  
  if (FD_ISSET(Socket->SockNum(), &check_set)) {
    return false;
  }

  return true;
}

bool 
XrdxCastor2Ofs2StagerJob::Close(bool ok, bool haswrite) 
{
  EPNAME("Close");
  const char* tident = "";

  if (!Port) 
    return true;

  if (!Socket)
    return false;

  if (!IsConnected)
    return false;

  XrdOucString ident = "CLOSE ";
  if (ok) {
    ident += "0 ";
  } else {
    ident += "1 ";
  }
  if (haswrite) {
    ident += "1 \n";
  } else {
    ident += "0 \n";
  }

  // send CLOSE message
  ZTRACE(debug,"Trying to send CLOSE message to StagerJob");
  int nwrite = write(Socket->SockNum(),ident.c_str(),ident.length());
  if (nwrite != ident.length()) {
    ErrCode = EIO;
    ErrMsg = "Couldn't write CLOSE request to StagerJob!";
    return false;
  } else {
    ZTRACE(debug,"Sent CLOSE message to StagerJob");
  }
  // read CLOSE response
  char closeresp[16384];
  int  respcode;
  char respmesg[16384];
  int nread=0;

  ZTRACE(debug,"Trying to read CLOSE response from StagerJob");
  nread = ::read(Socket->SockNum(),closeresp,sizeof(closeresp));
  ZTRACE(debug,"This read gave " << nread << " errno: " << errno);

  if (nread >0) {
    ZTRACE(debug,"Read CLOSE response from StagerJob");
  
    // ok, try to parse it 
    if ( (sscanf(closeresp,"%d %[^\n]",&respcode, respmesg)) == 2) {
      ErrMsg = respmesg;
      ErrCode = respcode;
      // check the code
      if (ErrCode != 0) {
	return false;
      } 
      // yes, everything worked!
    } else {
      ErrCode = EINVAL;
      ErrMsg  = "StagerJob returned illegal response: "; ErrMsg += (char*)closeresp;
      return false;
    }
  } else {
    // that was an error
    ErrCode = errno;
    ErrMsg = "Couldn't read response from StagerJob!";
    return false;
  }
  return true;
}
