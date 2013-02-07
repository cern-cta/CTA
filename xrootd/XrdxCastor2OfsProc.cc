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

  if (!strcmp(name,"/proc/totalreadbytes")) {
    return Write2ProcFile("totalreadbytes", TotalReadBytes);
  }

  if (!strcmp(name,"/proc/totalwritebytes")) {
    return Write2ProcFile("totalwritebytes", TotalWriteBytes);
  }

  if (!strcmp(name,"/proc/totalstreamreadbytes")) {
    return Write2ProcFile("totalstreamreadbytes", TotalStreamReadBytes);
  }

  if (!strcmp(name,"/proc/totalstreamwritebytes")) {
    return Write2ProcFile("totalstreamwritebytes", TotalStreamWriteBytes);
  }
  
  if (!strcmp(name,"/proc/readdelay")) {
    return Write2ProcFile("readdelay",(long long)ReadDelay);
  }
  
  if (!strcmp(name,"/proc/writedelay")) {
    return Write2ProcFile("writedelay",(long long)WriteDelay);
  }

  if (RunRateLimiter){
    if (!strcmp(name,"/proc/readratelimit")) {
      return Write2ProcFile("readratelimit",(long long)ReadRateLimit);
    }
    
    if (!strcmp(name,"/proc/writeratelimit")) {
      return Write2ProcFile("writeratelimit",(long long)WriteRateLimit);
    }
  }

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
    result *=Write2ProcFile("totalreadbytes" , TotalReadBytes);
    result *=Write2ProcFile("totalwritebytes", TotalWriteBytes);
    result *=Write2ProcFile("totalstreamreadbytes" , TotalStreamReadBytes);
    result *=Write2ProcFile("totalstreamwritebytes", TotalStreamWriteBytes);
    result *=Write2ProcFile("readdelay"      , ReadDelay);
    result *=Write2ProcFile("writedelay"     , WriteDelay);
    if (RunRateLimiter){
      result *=Write2ProcFile("readratelimit"  , ReadRateLimit);
      result *=Write2ProcFile("writeratelimit" , WriteRateLimit);
    }

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
  if (!ReadFromProc("totalreadbytes")) result=false;
  if (!ReadFromProc("totalwritebytes")) result=false;
  if (!ReadFromProc("totalstreamreadbytes")) result=false;
  if (!ReadFromProc("totalstreamwritebytes")) result=false;
  if (!ReadFromProc("readdelay")) result=false;
  if (!ReadFromProc("writedelay")) result=false;
  if (RunRateLimiter){
    if (!ReadFromProc("readratelimit")) result=false;
    if (!ReadFromProc("writeratelimit")) result=false;
  }
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

  if (entryname == "totalreadbytes") {
    TotalReadBytes = (long long)val;
    return true;
  }
  if (entryname == "totalwritebytes") {
    TotalWriteBytes = (long long)val;
    return true;
  }
  if (entryname == "totalstreamreadbytes") {
    TotalStreamReadBytes = (long long)val;
    return true;
  }
  if (entryname == "totalstreamwritebytes") {
    TotalStreamWriteBytes = (long long)val;
    return true;
  }
  if (entryname == "readdelay") {
    ReadDelay = (unsigned int)val;
    return true;
  }
  if (entryname == "writedelay") {
    WriteDelay = (unsigned int)val;
    return true;
  }
  if (entryname == "readratelimit") {
    ReadRateLimit = (unsigned int)val;
    return true;
  }
  if (entryname == "writeratelimit") {
    WriteRateLimit = (unsigned int) val;
    return true;
  }
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

