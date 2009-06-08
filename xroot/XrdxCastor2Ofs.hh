//         $Id: XrdxCastor2Ofs.hh,v 1.6 2009/06/08 19:15:41 apeters Exp $

#ifndef __XCASTOR2OFS_H__
#define __XCASTOR2OFS_H__

#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdOfs/XrdOfs.hh"
#include "XrdSfs/XrdSfsInterface.hh"
#include "XrdSys/XrdSysTimer.hh"
#include "XrdSys/XrdSysPthread.hh"
#include "XrdClient/XrdClientAdmin.hh"
#include "XrdClient/XrdClient.hh"
#include "XrdxCastor2Fs/XrdxCastor2OfsRateLimit.hh"
#include "XrdxCastor2Fs/XrdxCastor2ClientAdmin.hh"
#include "pwd.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <zlib.h>


extern void *ofsRateLimiter(void *parg); // source in XrdxCastor2OfsRateLimiter.cc

class XrdxCastor2OfsFile : public XrdOfsFile {
public:
  
  enum eProcRequest {kProcNone, kProcRead, kProcWrite};

  int          open(const char                *fileName,
		    XrdSfsFileOpenMode   openMode,
		    mode_t               createMode,
		    const XrdSecEntity        *client,
		    const char                *opaque = 0);
  
  int          close();

  int          read(XrdSfsFileOffset   fileOffset,   // Preread only
		      XrdSfsXferSize     amount);
  
  XrdSfsXferSize read(XrdSfsFileOffset   fileOffset,
		      char              *buffer,
		      XrdSfsXferSize     buffer_size);
  
  int          read(XrdSfsAio *aioparm);
  
  XrdSfsXferSize write(XrdSfsFileOffset   fileOffset,
		       const char        *buffer,
		       XrdSfsXferSize     buffer_size);
  
  int          write(XrdSfsAio *aioparm);

  int          sync();

  int          sync(XrdSfsAio *aiop);

  int          truncate(XrdSfsFileOffset   fileOffset);

  int          UpdateMeta();

  bool         verifychecksum();

  void         SignalJob(bool onclose);

  long long           FileReadBytes;  // byte count for read
  long long           FileWriteBytes; // byte count for write
  unsigned int        ReadRateLimit;   // read rate limit in mb/s
  unsigned int        WriteRateLimit;  // write rate limit in mb/s  
  unsigned int        ReadDelay;       // delay time in ms used in read requests
  unsigned int        WriteDelay;      // delay time in ms used in write requests
  unsigned int        RateMissingCnt;  // rate missing cnt in OfsFile
  
  XrdSysTimer         RateTimer;

  bool                IsAdminStream;   //
  time_t              LastMdsUpdate;   //
  XrdxCastor2OfsFile(const char* user) : XrdOfsFile(user){ envOpaque=NULL;FileReadBytes = FileWriteBytes = ReadRateLimit = WriteRateLimit = ReadDelay = WriteDelay = 0; IsAdminStream=false; RateMissingCnt = 0;firstWrite=true;hasWrite=false;stagehost="none";serviceclass="none";reqid="0";hasadler=1; adler = adler32(0L, Z_NULL, 0);adleroffset=0; DiskChecksum=""; DiskChecksumAlgorithm="";VerifyChecksum=true;hasadlererror=false;castorlfn="";}
  virtual ~XrdxCastor2OfsFile() {close();if (envOpaque) delete envOpaque;envOpaque=NULL;}

private:
  int                   procRequest;
  XrdOucEnv*            envOpaque;   
  bool                  hasWrite;
  bool                  firstWrite;
  XrdOucString          reqid;
  XrdOucString          stagehost;
  XrdOucString          serviceclass;
  XrdOucString          castorlfn;
  unsigned int          adler;
  bool                  hasadler;
  bool                  hasadlererror;
  XrdSfsFileOffset      adleroffset;
  struct stat           statinfo;
  XrdOucString          DiskChecksum;
  XrdOucString          DiskChecksumAlgorithm;
  bool                  VerifyChecksum;

  XrdxCastor2OfsRateLimit* Limiter;   // thread to calculate rate limiting parameters
};

class XrdxCastor2OfsDirectory : public XrdOfsDirectory {
public:
  XrdxCastor2OfsDirectory(const char *user) : XrdOfsDirectory(user){};
  virtual            ~XrdxCastor2OfsDirectory() {}
};


class XrdxCastor2Ofs : public XrdOfs {
  friend class XrdxCastor2OfsDirectory;
  friend class XrdxCastor2OfsFile;

private:
  vecString           procUsers;       // vector of users (reduced tidents <user>@host which can write into /proc

  // source in XrdxCastor2OfsProc.cc
  bool                Write2ProcFile(const char* name, long long value);

  // source in XrdxCastor2OfsProc.cc
  bool                ReadAllProc();   // read values out of the /proc file system
  bool                ReadFromProc(const char* entryname);   // read values out of the /proc file system

  // source in XrdxCastor2OfsProc.cc
protected:
  XrdSysMutex         StatisticMutex;  // mutex for read/write statistic

  bool                RunRateLimiter;  // indicates to run the rate limiting thread or not


public:
  XrdSfsDirectory *newDir(char *user=0) {return (XrdSfsDirectory *) new XrdxCastor2OfsDirectory(user);}
  XrdSfsFile *newFile(char *user=0) {return (XrdSfsFile *) new XrdxCastor2OfsFile(user);}
 
  int Configure(XrdSysError &error);

  XrdxCastor2Ofs() { TotalReadBytes=0; TotalWriteBytes=0; ReadDelay=0; WriteDelay=0; RunRateLimiter=false;ReadRateLimit=99999;WriteRateLimit=99999; ReadRateMissing = WriteRateMissing = 0; RateMissingCnt = 1 ; MeasuredReadRate = MeasuredWriteRate = 0; TotalStreamReadBytes = TotalStreamWriteBytes = 0;};

  XrdOucString        Procfilesystem;     // -> location of the proc file system directory
  bool                ProcfilesystemSync; // -> sync every access on disk

  long long           TotalReadBytes;  // byte count for read
  long long           TotalWriteBytes; // byte count for write
  long long           TotalStreamReadBytes;  // byte count for read
  long long           TotalStreamWriteBytes; // byte count for write
  unsigned int        ReadDelay;       // delay time in ms used in read requests
  unsigned int        WriteDelay;      // delay time in ms used in write requests

  unsigned int        ReadRateLimit;   // read rate limit in mb/s
  unsigned int        ReadRateMissing; // read rate missing for io in mb/s
  unsigned int        WriteRateLimit;  // write rate limit in mb/s
  unsigned int        WriteRateMissing;// write rate missing for io in mb/s
  
  unsigned int        MeasuredReadRate;// read rate measured by the limiter thread
  unsigned int        MeasuredWriteRate;// write rate measured by the limiter thread
  unsigned int        RateMissingCnt;  // rate missing cnt 

  XrdSysError*        Eroute;          // used by the 
  const char*         LocalRoot;       // we need this to write /proc variables directly to the FS

  bool                UpdateProc(const char* name);
  bool                doChecksumStreaming;
  bool                doChecksumUpdates;

  XrdOucString        WriteStateDirectory;         // default from Configure

  // here we mask all illegal operations
  int            chmod(const char             *Name,
		       XrdSfsMode        Mode,
		       XrdOucErrInfo    &out_error,
		       const XrdSecEntity     *client,
		       const char             *opaque = 0) { return SFS_OK;}
  
  int            exists(const char                *fileName,
			XrdSfsFileExistence &exists_flag,
			XrdOucErrInfo       &out_error,
			const XrdSecEntity        *client,
			const char                *opaque = 0) { return SFS_OK;}


  int            fsctl(const int               cmd,
		       const char             *args,
		       XrdOucErrInfo    &out_error,
		       const XrdSecEntity     *client) { return SFS_OK; } 

  int            mkdir(const char             *dirName,
		       XrdSfsMode        Mode,
		       XrdOucErrInfo    &out_error,
		       const XrdSecEntity     *client,
		       const char             *opaque = 0) { return SFS_OK;}
  
  int            prepare(      XrdSfsPrep       &pargs,
			       XrdOucErrInfo    &out_error,
			       const XrdSecEntity     *client = 0) { return SFS_OK;}
  
  
  int            rem(const char             *path,
		     XrdOucErrInfo    &out_error,
		     const XrdSecEntity     *client,
		     const char             *info = 0);
  
  int            remdir(const char             *dirName,
			XrdOucErrInfo    &out_error,
			const XrdSecEntity     *client,
			const char             *info = 0) {return SFS_OK;}
  
  int            rename(const char             *oldFileName,
			const char             *newFileName,
			XrdOucErrInfo    &out_error,
			const XrdSecEntity     *client,
			const char             *infoO = 0,
			const char            *infoN = 0) {return SFS_OK;}

  // this function we need to work without authorization
  int            stat(const char             *Name,
		      struct stat      *buf,
		      XrdOucErrInfo    &out_error,
		      const XrdSecEntity     *client,
		      const char             *opaque = 0) { 
    int fd = ::open("/etc/castor/status",O_RDONLY);
    if (fd) { 
      char buffer[4096];int nread = ::read(fd,buffer,4095);
      if (nread>0) {
	buffer[nread] = 0; 
	XrdOucString status = buffer;
	if ( (status.find("DiskServerStatus=DISKSERVER_PRODUCTION")) == STR_NPOS) {
	  close(fd); 
	  return SFS_ERROR;
	}
	int fpos=1;
	XrdOucString name = Name;
	while ( (fpos = name.find("/",fpos)) != STR_NPOS) {
	  XrdOucString fsprod;
	  fsprod.assign(name,0,fpos-1);
	  XrdOucString tag1=fsprod;
	  tag1 += "=FILESYSTEM_PRODUCTION";
	  XrdOucString tag2=fsprod; tag2+="/";
	  tag2 += "=FILESYSTEM_PRODUCTION";
	  if ( (status.find(tag1)!= STR_NPOS) || (status.find(tag2)!= STR_NPOS) ) {
	    // yes in production
	    break;
	  } 
	  fpos++;
	}
	if (fpos == STR_NPOS) {
	  close(fd);
	  return SFS_ERROR;
	}
      }
      close(fd);
    }

    if (!::stat(Name,buf)) return SFS_OK; else return SFS_ERROR;}


 
  XrdSysMutex         MetaMutex;             // mutex to protect the UpdateMeta function
  XrdSysMutex         PasswdMutex;           // mutex to protect the passwd expiry hash

  static XrdOucHash<XrdxCastor2ClientAdmin> *clientadmintable;
  static XrdOucHash<struct passwd> *passwdstore;

  virtual ~XrdxCastor2Ofs() {};
};


extern XrdxCastor2Ofs XrdOfsFS;

#endif
