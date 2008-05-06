#include "XrdCS2Fs/XrdCS2Ofs.hh"
#include "XrdClient/XrdClientAdmin.hh"
#include "XrdOfs/XrdOfsTrace.hh"
#include "XrdSfs/XrdSfsAio.hh"
#include "XrdSys/XrdSysTimer.hh"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

class XrdCS2Ofs;

XrdCS2Ofs XrdOfsFS;

// extern symbols
extern XrdSysError OfsEroute;
extern XrdOss *XrdOfsOss;
extern XrdOss    *XrdOssGetSS(XrdSysLogger *, const char *, const char *);


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
   OfsEroute.SetPrefix("cs2ofs_");
   OfsEroute.logger(lp);
   OfsEroute.Say("++++++ (c) 2008 CERN/IT-DM-SMD ",
		"CS2Ofs (CS2 File System) v 1.0");

// Initialize the subsystems
//
   XrdOfsFS.ConfigFN = (configfn && *configfn ? strdup(configfn) : 0);

   

   if ( XrdOfsFS.Configure(OfsEroute) ) return 0;
// Initialize the target storage system
//
   if (!(XrdOfsOss = XrdOssGetSS(lp, configfn, XrdOfsFS.OssLib))) return 0;

// All done, we can return the callout vector to these routines.
//
   return &XrdOfsFS;
}
}

/******************************************************************************/
/*                         C o n f i g  u r e                                 */
/******************************************************************************/
int XrdCS2Ofs::Configure(XrdSysError& Eroute)
{
  char *var;
  const char *val;
  int  cfgFD;
  // extract the manager from the config file
  XrdOucStream Config(&Eroute, getenv("XRDINSTANCE"));

  Procfilesystem = "/tmp/cs2-ofs/proc";
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
      if (!strncmp(var, "cs2.",4)) {
	var += 4;
	if (!strcmp("procuser",var)) {
	  if ((val = Config.GetWord())) {
	    XrdOucString sval = val;
	    procUsers.Push_back(sval);
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
		Eroute.Emsg("Config","argument for proc invalid. Specify cs2.proc <path> [sync|async]");
		exit(-1);
	      }
	    }
	    XrdOucString makeProc;
	    makeProc="mkdir -p ";
	    makeProc+=Procfilesystem;
	    makeProc+="/proc";
	    system(makeProc.c_str());
	    if (ProcfilesystemSync) 
	      Eroute.Say("=====> cs2.proc: ", Procfilesystem.c_str()," sync");
	    else 
	      Eroute.Say("=====> cs2.proc: ", Procfilesystem.c_str()," async");
	  }


	if (!strcmp("ratelimiter",var)) {
	  if ((val = Config.GetWord())) {
	    if ((!strcmp(val,"1")) || (!strcmp(val,"yes")) || (!strcmp(val,"true"))) {
	      RunRateLimiter=true;
	    }
	  }
	}
      }
    }
    
    Config.Close();
  }
  for (int i=0; i< procUsers.GetSize(); i++) {
    Eroute.Say("=====> cs2.procuser: ", procUsers[i].c_str());
  }

  if (RunRateLimiter) {
    Eroute.Say("=====> cs2.ratelimiter: yes","","");
  } else {
    Eroute.Say("=====> cs2.ratelimiter: no ","","");
  }

  // read in /proc variables at startup time to reset to previous values
  ReadAllProc();
  UpdateProc("*");
  ReadAllProc();
  // start eventually the rate limiter thread
  if (RunRateLimiter) {
    XrdCS2OfsRateLimit* Limiter = new XrdCS2OfsRateLimit();
    if (!Limiter->Init(Eroute)) {
      int retc = ENOMEM;
      Eroute.Emsg("Configure", retc, "create rate limiter thread");
      _exit(3);
    } else {
      Eroute.Say("++++++ CS2Fs RateLimiter running. ","","");
    }
  }

  int rc = XrdOfs::Configure(Eroute);

  return rc;
}



/******************************************************************************/
/*                         C a t a l o g O f s F i l e                        */
/******************************************************************************/
/*                         o p e n                                            */
/******************************************************************************/
int          
XrdCS2OfsFile::open(const char                *path,
			XrdSfsFileOpenMode   open_mode,
			mode_t               create_mode,
			const XrdSecEntity        *client,
			const char                *opaque)
{
  EPNAME("cs2ofsfile:open");
  // the OFS open is catched to set the access/modify time in the nameserver
  time_t now = time(NULL);
  procRequest = kProcNone;

  const char *tident = error.getErrUser();

  XrdOucString spath = path;

  // store opaque information
  XrdOucEnv envOpaque(opaque);

  if (spath.beginswith("/proc/")) {
    procRequest = kProcRead;
    if ( (open_mode & (SFS_O_RDONLY | SFS_O_WRONLY | SFS_O_RDWR |
		       SFS_O_CREAT  | SFS_O_TRUNC) ) != 0) 
      procRequest = kProcWrite;
  }

  if (!procRequest) {
    // send an acc/mod update to the server
    
    const char* streamrate;
    if ((streamrate=envOpaque.Get("streamin"))) {
      IsAdminStream = true;
      WriteRateLimit = atoi(streamrate);
    } else {
      WriteRateLimit = 99999;
    }

    if ((streamrate=envOpaque.Get("streamout"))) {
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
    // check the allowed proc users
    // this is not a high performance implementation but the usage of /proc is very rare anyway
    // 
    // clients are allowed to read/modify /proc, when they appear in the configuration file f.e. as
    // -> cs2.procUsers xrootd@pcitmeta.cern.ch
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
      spath += ".__proc_update__";
    } else {
      // depending on the desired file, we update the information in that file
      if (!XrdOfsFS.UpdateProc(spath.c_str()))
	return XrdOfsFS.Emsg(epname,error,EIO,"update /proc information",path);
    }

    return XrdOfsFile::open(spath.c_str(),open_mode,create_mode,client,opaque); 
  }

  return XrdOfsFile::open(path,open_mode,create_mode,client,opaque); 
}

/******************************************************************************/
/*                         c l o s e                                          */
/******************************************************************************/
int
XrdCS2OfsFile::close()
{
  EPNAME("cs2ofsfile:close");
  time_t now = time(NULL);

  XrdOucString spath = FName();
  // send an acc/mod update to the server

  if (procRequest == kProcWrite) {
    XrdOucString newpath = spath;
    newpath.erase(".__proc_update__");
    XrdOfsFS.rem(newpath.c_str(),error,0,"");
    // we update the internal values e.g. the rate limiter should see the changed settings immedeatly!
    XrdOfsFS.UpdateProc(newpath.c_str());
    return XrdOfsFS.rename(spath.c_str(),newpath.c_str(),error,0,"","");
  }
  return SFS_OK;
}


/******************************************************************************/
/*                         r e a d                                            */
/******************************************************************************/
int
XrdCS2OfsFile::read(XrdSfsFileOffset   fileOffset,   // Preread only
		    XrdSfsXferSize     amount)
{
  int rc = XrdOfsFile::read(fileOffset,amount);
  if (rc>0) {if(!IsAdminStream)ADDTOTALREADBYTES(rc) else ADDTOTALSTREAMREADBYTES(rc);FileReadBytes+=rc;}

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
XrdCS2OfsFile::read(XrdSfsFileOffset   fileOffset,
		      char              *buffer,
		      XrdSfsXferSize     buffer_size)
{
  int rc = XrdOfsFile::read(fileOffset,buffer,buffer_size);

  if (rc>0) {if(!IsAdminStream)ADDTOTALREADBYTES(buffer_size) else ADDTOTALSTREAMREADBYTES(buffer_size);FileReadBytes+=rc;}
  if (IsAdminStream) {
    DOREADDELAY();
  } else
    if (XrdOfsFS.ReadDelay) XrdSysTimer::Wait(XrdOfsFS.ReadDelay);

  return rc;
}
  
/******************************************************************************/
/*                         r e a d                                            */
/******************************************************************************/
int
XrdCS2OfsFile::read(XrdSfsAio *aioparm)
{
  int rc = XrdOfsFile::read(aioparm);
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
XrdCS2OfsFile::write(XrdSfsFileOffset   fileOffset,
		     const char        *buffer,
		     XrdSfsXferSize     buffer_size)
{

  int rc = XrdOfsFile::write(fileOffset,buffer,buffer_size);

  if (rc>0) {if (!IsAdminStream)ADDTOTALWRITEBYTES(buffer_size) else ADDTOTALSTREAMWRITEBYTES(buffer_size);FileWriteBytes+= buffer_size;}

  if (IsAdminStream) {
    DOWRITEDELAY();
  } else
    if (XrdOfsFS.WriteDelay) XrdSysTimer::Wait(XrdOfsFS.WriteDelay);

  return rc;
}

/******************************************************************************/
/*                         w r i t e                                          */
/******************************************************************************/
int
XrdCS2OfsFile::write(XrdSfsAio *aioparm)
{
  int rc = XrdOfsFile::write(aioparm);

  if (rc==SFS_OK) {if (!IsAdminStream)ADDTOTALWRITEBYTES(aioparm->sfsAio.aio_nbytes) else ADDTOTALSTREAMWRITEBYTES(aioparm->sfsAio.aio_nbytes);FileWriteBytes+=aioparm->sfsAio.aio_nbytes;}

  if (IsAdminStream) {
    DOWRITEDELAY();
  } else
    if (XrdOfsFS.WriteDelay) XrdSysTimer::Wait(XrdOfsFS.WriteDelay);

  return rc;
}

