//          $Id: XrdxCastor2Ofs.cc,v 1.7 2009/06/08 19:15:41 apeters Exp $

#include "XrdOfs/XrdOfsTrace.hh"
#include "XrdClient/XrdClientAdmin.hh"
#include "XrdOfs/XrdOfs.hh"
#include "XrdSfs/XrdSfsAio.hh"
#include "XrdSys/XrdSysTimer.hh"
#include "XrdOss/XrdOssApi.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "XrdOuc/XrdOucHash.hh"
#include "XrdxCastor2Fs/XrdxCastor2Ofs.hh"
#include "XrdxCastor2Fs/XrdxCastor2FsConstants.hh"
#include "XrdxCastor2Fs/XrdxCastor2Timing.hh"
#include "XrdxCastor2Fs/XrdxCastor2ClientAdmin.hh"
#include <sys/types.h>
#include <sys/stat.h>
#include <pwd.h>
#include <grp.h>
#include <fcntl.h>
#include <attr/xattr.h>
#include "Cns_api.h"
#include <shift/serrno.h>

class XrdxCastor2Ofs;

XrdxCastor2Ofs XrdOfsFS;

// extern symbols
extern XrdSysError OfsEroute;
extern XrdOssSys *XrdOfsOss;
extern XrdOss    *XrdOssGetSS(XrdSysLogger *, const char *, const char *);
extern XrdOucTrace OfsTrace;

// global singletons
XrdOucHash<struct passwd> *XrdxCastor2Ofs::passwdstore;
XrdOucHash<XrdxCastor2ClientAdmin> *XrdxCastor2Ofs::clientadmintable;

#define ADDTOTALREADBYTES(__bytes) \
  do {									\
    XrdOfsFS.StatisticMutex.Lock();					\
    XrdOfsFS.TotalReadBytes += (long long)__bytes;			\
    XrdOfsFS.StatisticMutex.UnLock();					\
  } while (0);

#define ADDTOTALWRITEBYTES(__bytes) \
  do {									\
    XrdOfsFS.StatisticMutex.Lock();					\
    XrdOfsFS.TotalWriteBytes += (long long)__bytes;			\
    XrdOfsFS.StatisticMutex.UnLock();					\
  } while (0);

#define ADDTOTALSTREAMREADBYTES(__bytes) \
  do {									\
    XrdOfsFS.StatisticMutex.Lock();					\
    XrdOfsFS.TotalStreamReadBytes += (long long)__bytes;			\
    XrdOfsFS.StatisticMutex.UnLock();					\
  } while (0);

#define ADDTOTALSTREAMWRITEBYTES(__bytes) \
  do {									\
    XrdOfsFS.StatisticMutex.Lock();					\
    XrdOfsFS.TotalStreamWriteBytes += (long long)__bytes;			\
    XrdOfsFS.StatisticMutex.UnLock();					\
  } while (0);

#define DOREADDELAY() \
  do {                                                                  \
    double period=0; RateTimer.Report(period);                          \
    double realreadtime = FileReadBytes/1000.0/1000.0/ ReadRateLimit;   \
    double missingrate =  ReadRateLimit - ( FileReadBytes / 1000.0 / 1000.0 / period);\
    if (realreadtime > period) {                                        \
      XrdSysTimer::Wait( (int)((realreadtime - period)*1000.0) );       \
    } else {                                                            \
      if (RateMissingCnt != XrdOfsFS.RateMissingCnt) {                  \
          XrdOfsFS.ReadRateMissing += (unsigned int)missingrate;        \
          RateMissingCnt = XrdOfsFS.RateMissingCnt;                     \
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
   XrdOfsFS.ConfigFN = (configfn && *configfn ? strdup(configfn) : 0);
   XrdOfsFS.passwdstore = new XrdOucHash<struct passwd> ();
   XrdOfsFS.clientadmintable = new XrdOucHash<XrdxCastor2ClientAdmin> ();

   if ( XrdOfsFS.Configure(OfsEroute) ) return 0;
// Initialize the target storage system
//
   if (!(XrdOfsOss = (XrdOssSys*) XrdOssGetSS(lp, configfn, XrdOfsFS.OssLib))) return 0;

// All done, we can return the callout vector to these routines.
//
   return &XrdOfsFS;
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

  XrdOfsFS.doChecksumStreaming = true;
  XrdOfsFS.doChecksumUpdates   = false;
  XrdOfsFS.WriteStateDirectory = "/var/log/xroot/server/openrw";

  Procfilesystem = "/tmp/xcastor2-ofs/";
  ProcfilesystemSync = false;

  if( !ConfigFN || !*ConfigFN) {
    // this error will be reported by XrdOfsFS.Configure
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

	if (!strcmp("checksum",var)) {
	  if ((val = Config.GetWord())) {
	    XrdOucString sval = val;
	    if (sval == "always") {
	      XrdOfsFS.doChecksumStreaming = true;
	      XrdOfsFS.doChecksumUpdates   = true;
	    } else {
	      if (sval == "never") {
		XrdOfsFS.doChecksumStreaming = false;
		XrdOfsFS.doChecksumUpdates   = false;
	      } else {
		if (sval == "streaming") {
		  XrdOfsFS.doChecksumStreaming = true;
		  XrdOfsFS.doChecksumUpdates   = false;
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
	
	if (!strcmp("openstatedirectory", var)) {
	  if (( val = Config.GetWord())) {
	    WriteStateDirectory = val;
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
  makeProc+= "; chown stage.st "; makeProc+=Procfilesystem;

  system(makeProc.c_str());

if ( access(Procfilesystem.c_str(),R_OK | W_OK )) {
    Eroute.Emsg("Config","Cannot use given third proc directory ",Procfilesystem.c_str());
    exit(-1);
    
  }

  XrdOucString makeWriteState;
  makeWriteState="mkdir -p ";
  makeWriteState+=WriteStateDirectory;
  makeWriteState+= "; chown stage.st "; makeWriteState+=WriteStateDirectory.c_str();;
  system(makeWriteState.c_str());

  for (int i=0; i< procUsers.GetSize(); i++) {
    Eroute.Say("=====> xcastor2.procuser: ", procUsers[i].c_str());
  }

  if (RunRateLimiter) {
    Eroute.Say("=====> xcastor2.ratelimiter: yes","","");
  } else {
    Eroute.Say("=====> xcastor2.ratelimiter: no ","","");
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

void
XrdxCastor2OfsFile::SignalJob(bool onclose) {
  EPNAME("SignalJob");

  // don't deal with proc requests
  if (procRequest != kProcNone) 
    return;

  const char *tident = error.getErrUser();
  XrdOucString writestate = XrdOfsFS.WriteStateDirectory.c_str(); writestate += "/";
  writestate += reqid;
  
  if (reqid != "0") {
    ZTRACE(open,"Signalling x2castorjob with reqid " << reqid);
    int fd=0;
    if (!onclose) {
      if ( (fd = ::creat(writestate.c_str(), S_IRWXU)) >0) {
	// good, we put a state file
	::close(fd);
      }  else {
	ZTRACE(open,"error: SignalJob => couldn't create write state file");
      }
    } else {
      XrdOucString writestatedone=writestate;
      writestatedone += ".done";
      // rename the request file to '.done'
      if (!rename(writestate.c_str(),writestatedone.c_str())) {
	// wait that the .done state file disappears
	struct stat buf;
	int ntry=0;
	while (!(::stat(writestatedone.c_str(),&buf))) {
	  ntry++;
	  if (ntry> 150) {
	    // after 30 seconds give up
	    break;
	  } 
	  usleep(200000);
	}
	// try to read the pid file
	XrdOucString writestatepid=writestate;
	writestatepid += ".pid";
	int pfd=0;
	int sjpid = 0;
	if ( (pfd = ::open(writestatepid.c_str(),O_RDONLY) ) ) {
	  char contents[1024];
	  memset(contents,0,sizeof(contents));
	  int nb = ::read(pfd,contents,sizeof(contents));
	  ::close(pfd);
	  unlink(writestatepid.c_str());
	  int ncycles=0;
	  if (nb>=0) {
	    sjpid = atoi(contents);
	    if (sjpid >0) {
	      // now hang until this stagerJob guy disappears
	      while (!(kill(sjpid,0))) {
		// still there
		usleep(100000);
		ncycles++;
	      }
	      // now the guy is gone ;-) ... let's return
	      ZTRACE(open,"info: SignalJob => we waited " << (ncycles * 100) << " ms for stagerJob to terminate");
	    } else {
	      ZTRACE(open,"error: SignalJob => stagerJob pid file contained PID 0!");
	      unlink(writestatedone.c_str());
	    }
	  } else {
	    ZTRACE(open,"error: SignalJob => stagerJob pid file couldn't be read - the x2castorjob script took to long or crashed");
	    unlink(writestatedone.c_str());
	  }

	} else {
	  ZTRACE(open,"error: SignalJob => stagerJob pid didn't appear - the x2castorjob script took to long or crashed");
	  unlink(writestatedone.c_str());
	}
	
	if (ntry>150) {
	  ZTRACE(open,"error: SignalJob => open write state file didn't get removed within 30s - the x2castorjob script took to long or crashed");
	  unlink(writestatedone.c_str());
	}
      } else {
	ZTRACE(open,"error: SignalJob => open write state file didn't exist (rename failed)");
	unlink(writestate.c_str());
      }
    }
  }
}

int
XrdxCastor2OfsFile::UpdateMeta() {
  EPNAME("UpdateMeta");

  ZTRACE(open,"haswrite=" <<hasWrite << " firstWrite="<< firstWrite );

  XrdOucString preparename = envOpaque->Get("castor2fs.sfn");

  // don't deal with proc requests
  if (procRequest != kProcNone) 
    return SFS_OK;
      
  const char *tident = error.getErrUser();

  XrdOucString mdsaddress = "root://";
  mdsaddress += envOpaque->Get("castor2fs.manager");
  mdsaddress += "//dummy";

  XrdOfsFS.MetaMutex.Lock();

  XrdxCastor2ClientAdmin* mdsclient;
  if ( (mdsclient = XrdOfsFS.clientadmintable->Find(mdsaddress.c_str())) ) {
  } else {
    mdsclient = new XrdxCastor2ClientAdmin(mdsaddress.c_str());
    mdsclient->GetAdmin()->Connect();
    XrdOfsFS.clientadmintable->Add(mdsaddress.c_str(),mdsclient);
  }

  if (!mdsclient) {
    if (mdsclient) {
      XrdOfsFS.clientadmintable->Del(mdsaddress.c_str());
    }
    XrdOfsFS.MetaMutex.UnLock();
    return XrdOfsFS.Emsg(epname,error, EHOSTUNREACH, "connect to the mds host (normally the manager) ", "");
  }

  preparename += "?";

  if (firstWrite && hasWrite) 
    preparename += "&firstbytewritten=true";
  else {
    XrdOfsFS.MetaMutex.UnLock();
    return SFS_OK;
  }
  
  preparename += "&reqid=";
  preparename += reqid;
  preparename += "&stagehost=";
  preparename += stagehost;
  preparename += "&serviceclass=";
  preparename += serviceclass;

  ZTRACE(open,"informing about " << preparename.c_str());

  if (!mdsclient->GetAdmin()->Prepare(preparename.c_str(),0,0)) {
    XrdOfsFS.clientadmintable->Del(mdsaddress.c_str());
    XrdOfsFS.MetaMutex.UnLock();
    return XrdOfsFS.Emsg(epname,error,ESHUTDOWN,"update mds size/modtime",FName());
  }

  XrdOfsFS.MetaMutex.UnLock();
  return SFS_OK;
}


/******************************************************************************/
/*                         C a t a l o g O f s F i l e                        */
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
  time_t now = time(NULL);
  procRequest = kProcNone;

  const char *tident = error.getErrUser();

  XrdxCastor2Timing opentiming("ofsf::open");
  
  TIMING(OfsTrace,"START",&opentiming);

  XrdOucString spath = path;
  bool isRW=false;
  castorlfn = path;

  XrdOucString newopaque = opaque;
  // if thereis explicit user opaque information we find two ?, so we just replace it with a seperator.
  
  newopaque.replace("?","&");
  newopaque.replace("&&","&");
  int triedpos = newopaque.find("tried=");
  if (triedpos != STR_NPOS) {
    newopaque.erase(triedpos);
  }

  envOpaque = new XrdOucEnv(newopaque.c_str());

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
    XrdOucString prependedspath = XrdOfsFS.Procfilesystem;
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
    for (int i = 0 ; i < XrdOfsFS.procUsers.GetSize(); i++) {
      if (XrdOfsFS.procUsers[i] == reducedTident) {
	allowed = true;
	break;
      }
    }

    if (!allowed) {
      return XrdOfsFS.Emsg(epname,error,EPERM,"allow access to /proc/ fs in xrootd",path);
    }
    if (procRequest == kProcWrite) {
      open_mode = SFS_O_RDWR | SFS_O_TRUNC;
      prependedspath += ".__proc_update__";
    } else {
      // depending on the desired file, we update the information in that file
      if (!XrdOfsFS.UpdateProc(prependedspath.c_str()))
      	return XrdOfsFS.Emsg(epname,error,EIO,"update /proc information",path);
    }
     
    // we have to open the rerooted proc path!
    int rc = XrdOfsFile::open(prependedspath.c_str(),open_mode,create_mode,client,newopaque.c_str());
    return rc;
  }

  TIMING(OfsTrace,"PROCBLOCK",&opentiming);

  XrdOucString newpath="";

  newpath = envOpaque->Get("castor2fs.pfn1");

  int retc;

  if ((retc = XrdOfsOss->Stat(newpath.c_str(), &statinfo))) {
    // file does not exist, keep the create lfag
    firstWrite=false;
  } else {
    if (open_mode & SFS_O_CREAT) 
      open_mode -= SFS_O_CREAT;
    

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
    ZTRACE(open,"Checksum-Algorithm: " << DiskChecksumAlgorithm.c_str() << " Checksum: " << DiskChecksum.c_str());
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
  char* val;
  val = envOpaque->Get("castor2fs.pfn2");
  
  if (!(val) || (!strlen(val))) {
    return XrdOfsFS.Emsg(epname,error,ESHUTDOWN,"reqid/pfn2 is missing",FName());
  }

  XrdOucString reqtag = val;
  reqid ="0";
  stagehost ="none";
  serviceclass ="none";

  int pos1;
  int pos2;

  if ( (pos1 = reqtag.find(":") ) != STR_NPOS) {
    reqid.assign(reqtag,0,pos1-1);
    if ( ( pos2 = reqtag.find(":",pos1+1)) != STR_NPOS)  {
      stagehost.assign(reqtag,pos1+1,pos2-1);
      serviceclass.assign(reqtag,pos2+1);
    }
  }

  // send the sigusr1 to the loca xcastor2job to signal the open
  SignalJob(false);

  int rc = XrdOfsFile::open(newpath.c_str(),open_mode,create_mode,client,newopaque.c_str()); 

  TIMING(OfsTrace,"FILEOPEN",&opentiming);

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
  time_t now = time(NULL);
  int rc = SFS_OK;

  XrdOucString spath = FName();

  if (procRequest == kProcWrite) {
    sync();
    XrdOucString newpath = spath;
    newpath.erase(".__proc_update__");
    
    XrdOfsFS.rem(newpath.c_str(),error,0,"");
    // we update the internal values e.g. the rate limiter should see the changed settings immedeatly!
    int rc =  ::rename(spath.c_str(),newpath.c_str()); //,error,0,"","");
    //    printf("rc is %d %s->%s\n",rc, spath.c_str(),newpath.c_str());
    XrdOfsFS.rem(spath.c_str(),error,0,"");
    XrdOfsFS.ReadAllProc();
    XrdOfsFS.UpdateProc(newpath.c_str());
    XrdOfsFile::close();
    return rc;
  } else {
    XrdOfsFS.ReadFromProc("trace");
  }

  char ckSumbuf[32+1];
  sprintf(ckSumbuf,"%x",adler);
  char *ckSumalg="ADLER32";

  XrdOucString newpath="";
  newpath = envOpaque->Get("castor2fs.pfn1");
  
  if (hasWrite) {
    if (XrdOfsFS.doChecksumUpdates && (!hasadler)) {
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
      if ((XrdOfsFS.doChecksumStreaming || XrdOfsFS.doChecksumUpdates) && hasadler) {
	sprintf(ckSumbuf,"%x",adler);
	ZTRACE(close,"Streaming    Checksum [" << FileWriteBytes << " bytes] : " << ckSumbuf);
      }
    }
    
    if ((XrdOfsFS.doChecksumStreaming || XrdOfsFS.doChecksumUpdates) && hasadler) {     
      if (setxattr(newpath.c_str(),"user.castor.checksum.type",ckSumalg, strlen(ckSumalg),0)) {
	XrdOfsFS.Emsg(epname,error, EIO, "set checksum type", "");
	rc=SFS_ERROR;
	// anyway this is not returned to the client
      } else {
	if (setxattr(newpath.c_str(),"user.castor.checksum.value",ckSumbuf,strlen(ckSumbuf),0)) {
	  XrdOfsFS.Emsg(epname,error, EIO, "set checksum", "");
	  rc=SFS_ERROR;
	  // anyway this is not returned to the client
	}
      }
    } else {
      if ((!XrdOfsFS.doChecksumUpdates) && (!hasadler)) {
	// remove checksum - we don't check errors here
	removexattr(newpath.c_str(),"user.castor.checksum.type");
	removexattr(newpath.c_str(),"user.castor.checksum.value");
      }
    }
  }


  // send the sigusr2 signal to the local x2castorjob script 
  SignalJob(true);
  
  XrdOfsFile::close();

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
      XrdOfsFS.Emsg(epname,error, EIO, "verify checksum - checksum wrong!", "");
      rc = false;
    }
    
    if (CalcChecksumAlgorithm != DiskChecksumAlgorithm) {
      XrdOfsFS.Emsg(epname,error, EIO, "verify checksum - checksum type wrong!","");
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
    if (XrdOfsFS.ReadDelay) XrdSysTimer::Wait(XrdOfsFS.ReadDelay);

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
  
  if (hasadler && XrdOfsFS.doChecksumStreaming) {
    adler = adler32(adler, (const Bytef*) buffer, rc);
    if (rc>0)
      adleroffset += rc;
  }

  if (rc>0) {if(!IsAdminStream)ADDTOTALREADBYTES(buffer_size) else ADDTOTALSTREAMREADBYTES(buffer_size);FileReadBytes+=rc;}
  
  if (IsAdminStream) {
    DOREADDELAY();
  } else
    if (XrdOfsFS.ReadDelay) XrdSysTimer::Wait(XrdOfsFS.ReadDelay);
  
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
    if (XrdOfsFS.ReadDelay) XrdSysTimer::Wait(XrdOfsFS.ReadDelay);

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
  //  printf(" I am writing here !!!!!!!!!!!!\n");
  int rc = XrdOfsFile::write(fileOffset,buffer,buffer_size);

  if (rc>0) {if (!IsAdminStream)ADDTOTALWRITEBYTES(buffer_size) else ADDTOTALSTREAMWRITEBYTES(buffer_size);FileWriteBytes+= buffer_size;}

  // computation of adler checksum - we disable it if seek happened
  if (fileOffset != adleroffset) {
    hasadler=false;
  }

  if (hasadler && XrdOfsFS.doChecksumStreaming) {
    adler = adler32(adler, (const Bytef*) buffer, buffer_size);
    if (rc>0) {
      adleroffset += rc;
    }
  }

  if (IsAdminStream) {
    DOWRITEDELAY();
  } else
    if (XrdOfsFS.WriteDelay) XrdSysTimer::Wait(XrdOfsFS.WriteDelay);

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
  int rc = XrdOfsFile::write(aioparm);

  if (rc==SFS_OK) {if (!IsAdminStream)ADDTOTALWRITEBYTES(aioparm->sfsAio.aio_nbytes) else ADDTOTALSTREAMWRITEBYTES(aioparm->sfsAio.aio_nbytes);FileWriteBytes+=aioparm->sfsAio.aio_nbytes;}

  if (IsAdminStream) {
    DOWRITEDELAY();
  } else
    if (XrdOfsFS.WriteDelay) XrdSysTimer::Wait(XrdOfsFS.WriteDelay);

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
/*                         C a t a l o g O f s                                */
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
  
  for (int i = 0 ; i < XrdOfsFS.procUsers.GetSize(); i++) {
    if (XrdOfsFS.procUsers[i] == reducedTident) {
      allowed = true;
      break;
    }
  }
  
  if (!allowed) {
    return XrdOfsFS.Emsg(epname,error,EPERM,"allow access to unlink",path);
  }

  if (unlink(path) ) {
    // we don't report deletion of a not existing file as error
    if (errno != ENOENT) {
      return XrdOfsFS.Emsg(epname, error, errno, "remove", path);
    }
  }
  
  XrdOfsHandle::Hide(path);
  return SFS_OK;
}
  
