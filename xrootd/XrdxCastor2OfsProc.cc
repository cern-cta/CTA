//          $Id: XrdxCastor2OfsProc.cc,v 1.2 2009/07/06 08:27:11 apeters Exp $

#include "XrdxCastor2Ofs.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include <string.h>

extern XrdOucTrace OfsTrace;

#define READLONGLONGFROMFILE(_name_,_val_)	\
  do {						\
    FILE* fp = fopen(_name_,"r");		\
    if (!fp)					\
      _val_=-1;					\
    else {					\
      char line[4096];				\
      if ((fscanf(fp,"%s",line)) != 1)		\
	_val_=-1;				\
      _val_=strtoll(line,(char**)NULL,0);	\
      fclose(fp);				\
    }						\
  } while (0);

#define WRITELONGLONGTOFILE(_name_,_val_)		\
  do {							\
    FILE* fp = fopen(_name_,"w+");			\
    if (!fp)						\
      _val_=-1;						\
    else {						\
      _val_ = (int) fprintf(fp,"%lld\n",(long long)_val_);\
       fclose(fp);					\
    }                                                   \
  } while (0);




/******************************************************************************/
/*                   W r i t e 2 P r o c F i l e                              */
/******************************************************************************/
bool
XrdxCastor2Ofs::Write2ProcFile(const char* name, long long val)
{
  XrdOucString procname;
  procname = Procfilesystem;
  procname += "proc/";
  procname += name;

  //  printf("Writing %s => %lld\n",procname.c_str(),val);
  WRITELONGLONGTOFILE(procname.c_str(),val);

  return (val > 0);
}

/******************************************************************************/
/*                         U p d a t e P r o c                                */
/******************************************************************************/

bool
XrdxCastor2Ofs::UpdateProc(const char *inname)
{
  XrdOucString modname = inname;
  modname.assign(modname,modname.find("/proc"));
  const char* name = modname.c_str();

  if (ThirdPartyCopy) {
    if (!strcmp(name, "/proc/thirdpartycopyslots")) {
      return Write2ProcFile("thirdpartycopyslots",(long long)ThirdPartyCopySlots);
    }
    
    if (!strcmp(name, "/proc/thirdpartycopyslotrate")) {
      return Write2ProcFile("thirdpartycopyslotrate",(long long)ThirdPartyCopySlotRate);
    }
  }
  
  if (!strcmp(name,"/proc/trace")) {
    return true;
  }

  if (!strcmp(name,"*")) {
    bool result=true;
    if (ThirdPartyCopy) {
      result *=Write2ProcFile("thirdpartycopyslots", ThirdPartyCopySlots);
      result *=Write2ProcFile("thirdpartycopyslotrate", ThirdPartyCopySlotRate);
    }

    return result;
  }
  return false;
}

/******************************************************************************/
/*                       R e a d A l l P r o c                                */
/******************************************************************************/
bool
XrdxCastor2Ofs::ReadAllProc() 
{
  bool result=true;
  if (ThirdPartyCopy) {
    if (!ReadFromProc("thirdpartycopyslots")) result=false;
    if (!ReadFromProc("thirdpartycopyslotrate")) result=false;
  }
  
  ReadFromProc("trace");
  return result;
}

/******************************************************************************/
/*                       R e a d F r o m P r o c                              */
/******************************************************************************/
bool
XrdxCastor2Ofs::ReadFromProc(const char* entryname) {
  XrdOucString procname = Procfilesystem;
  XrdOucString oucentry = entryname;
  long long val = 0;
  procname += "proc/";
  procname += entryname;
  READLONGLONGFROMFILE(procname.c_str(),val);
  //  printf("Reading %s => %lld\n",procname.c_str(),val);
  if (val == -1)
    return false;

  if (entryname == "thirdpartycopyslots") {
    ThirdPartyCopySlots = (unsigned int)val;
    return true;
  }
  if (entryname == "thirdpartycopyslotrate") {
    ThirdPartyCopySlotRate = (unsigned int) val;
    return true;
  }
  if (entryname == "trace") {
    OfsTrace.What = val;
    return true;
  }

  return false;
}

