#ifndef __CS2OFS_H__
#define __CS2OFS_H__

#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucString.hh"
#include "XrdOfs/XrdOfs.hh"
#include "XrdSfs/XrdSfsInterface.hh"
#include "XrdSys/XrdSysTimer.hh"
#include "XrdClient/XrdClientAdmin.hh"
#include "XrdCS2Fs/XrdCS2OfsRateLimit.hh"

extern void *ofsRateLimiter(void *parg); // source in XrdCS2OfsRateLimiter.cc

class XrdCS2OfsFile : public XrdOfsFile {
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


  long long           FileReadBytes;  // byte count for read
  long long           FileWriteBytes; // byte count for write
  unsigned int        ReadRateLimit;   // read rate limit in mb/s
  unsigned int        WriteRateLimit;  // write rate limit in mb/s  
  unsigned int        ReadDelay;       // delay time in ms used in read requests
  unsigned int        WriteDelay;      // delay time in ms used in write requests
  unsigned int        RateMissingCnt;  // rate missing cnt in OfsFile
  
  XrdSysTimer         RateTimer;

  bool                IsAdminStream;   //

  XrdCS2OfsFile(const char* user) : XrdOfsFile(user){ envOpaque=NULL;FileReadBytes = FileWriteBytes = ReadRateLimit = WriteRateLimit = ReadDelay = WriteDelay = 0; IsAdminStream=false; RateMissingCnt = 0;};
  virtual ~XrdCS2OfsFile() {close();if (envOpaque) delete envOpaque;envOpaque=NULL;}

private:
  int                   procRequest;
  XrdOucEnv*              envOpaque;   

  XrdCS2OfsRateLimit* Limiter;   // thread to calculate rate limiting parameters
};

class XrdCS2OfsDirectory : public XrdOfsDirectory {
public:
  XrdCS2OfsDirectory(const char *user) : XrdOfsDirectory(user){};
  virtual            ~XrdCS2OfsDirectory() {}
};


class XrdCS2Ofs : public XrdOfs {
  friend class XrdCS2OfsDirectory;
  friend class XrdCS2OfsFile;

private:
  vecString           procUsers;       // vector of users (reduced tidents <user>@host which can write into /proc

  // source in XrdCS2OfsProc.cc
  bool                Write2ProcFile(const char* name, long long value);

  // source in XrdCS2OfsProc.cc
  bool                ReadAllProc();   // read values out of the /proc file system
  bool                ReadFromProc(const char* entryname);   // read values out of the /proc file system


  XrdOucString        Procfilesystem;     // -> location of the proc file system directory
  bool                ProcfilesystemSync; // -> sync every access on disk

  // source in XrdCS2OfsProc.cc
protected:
  XrdSysMutex         StatisticMutex;  // mutex for read/write statistic

  bool                RunRateLimiter;  // indicates to run the rate limiting thread or not


public:
  XrdSfsDirectory *newDir(char *user=0) {return (XrdSfsDirectory *) new XrdCS2OfsDirectory(user);}
  XrdSfsFile *newFile(char *user=0) {return (XrdSfsFile *) new XrdCS2OfsFile(user);}
 
  int Configure(XrdSysError &error);

  XrdCS2Ofs() { TotalReadBytes=0; TotalWriteBytes=0; ReadDelay=0; WriteDelay=0; RunRateLimiter=false;ReadRateLimit=99999;WriteRateLimit=99999; ReadRateMissing = WriteRateMissing = 0; RateMissingCnt = 1 ; MeasuredReadRate = MeasuredWriteRate = 0; TotalStreamReadBytes = TotalStreamWriteBytes = 0;};

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

  virtual ~XrdCS2Ofs() {};
};


extern XrdCS2Ofs XrdOfsFS;

#endif
