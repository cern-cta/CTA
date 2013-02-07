//         $Id: XrdxCastor2Proc.hh,v 1.1 2008/09/15 10:04:02 apeters Exp $

#ifndef __XCASTOR2_PROC__
#define __XCASTOR2_PROC__

#include "XrdOuc/XrdOucString.hh"
#include "XrdOuc/XrdOucHash.hh"

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <dirent.h>

class XrdxCastor2ProcFile
{
private:
  int fd;
  XrdOucString fname;
  bool procsync;
  time_t lastwrite;

public:
  bool Open();
  bool Close() { if (fd>=0) close(fd); return true; }
  bool Write(long long val, int writedelay=0);
  bool Write(double val, int writedelay=0);
  bool Write(const char* str, int writedelay=0);
  bool WriteKeyVal(const char* key, unsigned long long value, int writedelay, bool truncate=0);
  long long Read();
  bool Read(XrdOucString &str);
  

  XrdxCastor2ProcFile(const char* name, bool syncit=false){fname = name;fd=0;procsync = syncit;lastwrite=0;};
  virtual ~XrdxCastor2ProcFile() {Close();};
};

class XrdxCastor2Proc
{
private:
  bool procsync;
  XrdOucString procdirectory;
  XrdOucHash<XrdxCastor2ProcFile> files;

public:
  
  XrdxCastor2ProcFile* Handle(const char* name);

  XrdxCastor2Proc(const char* procdir, bool syncit) { 
    procdirectory = procdir; 
    procsync = syncit;
  };

  bool Open() {
    XrdOucString doit="mkdir -p ";
    doit+=procdirectory;
    system(doit.c_str());
    DIR* pd=opendir(procdirectory.c_str());
    if (!pd) {
      return false;
    } else {
      closedir(pd);
      return true;
    }
  }

  virtual ~XrdxCastor2Proc() {};
};
#endif

