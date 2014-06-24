/*******************************************************************************
 *                      XrdxCastor2Ofs.cc
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2012  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 *
 * @author Andreas Peters <apeters@cern.ch>
 * @author Elvin Sindrilaru <esindril@cern.ch>
 *
 ******************************************************************************/

/*-----------------------------------------------------------------------------*/
#include <sys/types.h>
#include <sys/stat.h>
#include <grp.h>
#include <attr/xattr.h>
#include "h/Cns_api.h"
#include "h/serrno.h"
#include <string.h>
/*----------------------------------------------------------------------------*/
#include "XrdVersion.hh"
#include "XrdAcc/XrdAccAuthorize.hh"
#include "XrdOfs/XrdOfs.hh"
#include "XrdSfs/XrdSfsAio.hh"
#include "XrdSys/XrdSysDNS.hh"
#include "XrdOss/XrdOssApi.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "XrdOuc/XrdOucHash.hh"
#include "XrdNet/XrdNetOpts.hh"
#include "XrdCl/XrdClFileSystem.hh"
/*----------------------------------------------------------------------------*/
#include "XrdxCastor2Ofs.hpp"
#include "XrdxCastor2FsConstants.hpp"
#include "XrdxCastorTiming.hpp"
/*----------------------------------------------------------------------------*/

XrdxCastor2Ofs* gSrv; ///< global diskserver OFS handle

// extern symbols
extern XrdOfs*     XrdOfsFS;
extern XrdSysError OfsEroute;
extern XrdOssSys*  XrdOfsOss;
extern XrdOss*     XrdOssGetSS(XrdSysLogger*, const char*, const char*);

XrdVERSIONINFO(XrdSfsGetFileSystem, xCastor2Ofs);

//------------------------------------------------------------------------------
// SfsGetFileSystem
//------------------------------------------------------------------------------
extern "C"
{
  XrdSfsFileSystem* XrdSfsGetFileSystem(XrdSfsFileSystem* native_fs,
                                        XrdSysLogger*     lp,
                                        const char*       configfn)
  {
    static XrdxCastor2Ofs myFS;
    // Do the herald thing
    OfsEroute.SetPrefix("castor2ofs_");
    OfsEroute.logger(lp);
    OfsEroute.Say("++++++ (c) 2008 CERN/IT-DM-SMD ",
                  "xCastor2Ofs (extended Castor2 File System) v 1.0");
    // Initialize the subsystems
    gSrv = &myFS;
    gSrv->ConfigFN = (configfn && *configfn ? strdup(configfn) : 0);

    if (gSrv->Configure(OfsEroute)) return 0;

    // All done, we can return the callout vector to these routines
    XrdOfsFS = static_cast<XrdOfs*>(gSrv);
    return XrdOfsFS;
  }
}


/******************************************************************************/
/*                         x C a s t o r O f s                                */
/******************************************************************************/

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
XrdxCastor2Ofs::XrdxCastor2Ofs():
  XrdOfs(),
  LogId(),
  mLogLevel(LOG_INFO)
{ }


//------------------------------------------------------------------------------
// Configure
//------------------------------------------------------------------------------
int XrdxCastor2Ofs::Configure(XrdSysError& Eroute)
{
  char* var;
  const char* val;
  int  cfgFD;
  // Extract the manager from the config file
  XrdOucStream config_stream(&Eroute, getenv("XRDINSTANCE"));
  doPOSC = false;
  mProcFs = "/tmp/xcastor2-ofs/";

  if (ConfigFN && *ConfigFN)
  {
    // Try to open the configuration file.
    if ((cfgFD = open(ConfigFN, O_RDONLY, 0)) < 0)
      return Eroute.Emsg("Config", errno, "open config file fn=", ConfigFN);
    
    config_stream.Attach(cfgFD);
    
    // Now start reading records until eof.
    while ((var = config_stream.GetMyFirstWord()))
    {
      if (!strncmp(var, "xcastor2.", 9))
      {
        var += 9;
        
        // Get the debug level
        if (!strcmp("loglevel", var))
        {
          if (!(val = config_stream.GetWord()))
          {
            Eroute.Emsg("Config", "argument for debug level invalid set to INFO.");
            mLogLevel = LOG_INFO;
          }
          else
          {
            std::string str_val(val);

            // Loglevel can be a number or a string 
            if (isdigit(str_val[0]))
              mLogLevel = atoi(val);
            else
              mLogLevel = Logging::GetPriorityByString(val);

            Logging::SetLogPriority(mLogLevel);
            Eroute.Say("=====> xcastor2.loglevel: ",
                       Logging::GetPriorityString(mLogLevel), "");
          }
        }

        // Get any debug filter name
        if (!strcmp("debugfilter", var))
        {
          if (!(val = config_stream.GetWord()))
          {
            Eroute.Emsg("Config", "argument for debug filter invalid set to none.");
          }
          else
          {
            Logging::SetFilter(val);
            Eroute.Say("=====> xcastor2.debugfileter: ", val, "");
          }
        }

        if (!strcmp("procuser", var))
        {
          if ((val = config_stream.GetWord()))
          {
            XrdOucString sval = val;
            procUsers.Push_back(sval);
          }
        }

        if (!strcmp("posc", var))
        {
          if ((val = config_stream.GetWord()))
          {
            XrdOucString sval = val;

            if ((sval == "true") || (sval == "1") || (sval == "yes"))
              doPOSC = true;
          }
        }

        if (!strcmp("proc", var))
        {
          if (!(val = config_stream.GetWord()))
          {
            Eroute.Emsg("Config", "argument for proc invalid");
            exit(-1);
          }
          else
          {
            mProcFs = val;

            while (mProcFs.replace("//", "/")) {}

            Eroute.Say("=====> xcastor2.proc: ", mProcFs.c_str());
          }
        }
      }
    }
    config_stream.Close();
  }

  // Setup the circular in-memory logging buffer
  XrdOucString unit = "rdr@";
  unit += XrdSysDNS::getHostName();
  unit += ":1094";
  Logging::Init();
  Logging::SetLogPriority(mLogLevel);
  Logging::SetUnit(unit.c_str());
  xcastor_info("info=\"logging configured\" ");

  // Create the /proc/ directory
  std::ostringstream oss;
  oss << "mkdir -p " << mProcFs << "/proc";
#ifdef XFS_SUPPORT
  oss << "/xfs";
#endif // XFS_SUPPORT
  oss << "; chown stage.st " << mProcFs << "/proc" << std::endl;
  system(oss.str().c_str());
          
  if (access(mProcFs.c_str(), R_OK | W_OK))
  {
    Eroute.Emsg("Config", "Cannot use given proc directory ", mProcFs.c_str());
    exit(-1);
  }

  for (int i = 0; i < procUsers.GetSize(); i++)
    Eroute.Say("=====> xcastor2.procuser: ", procUsers[i].c_str(), "");

  Eroute.Say("=====> xcastor2.posc: ", (doPOSC ? "yes" : "no"), "");

  // Read in /proc variables at startup time to reset to previous values
  ReadFromProc("trace");
  UpdateProc("*");
  ReadFromProc("trace");
  int rc = XrdOfs::Configure(Eroute);

  // Set the effective user for all the XrdClients used to issue 'prepares' 
  // to redirector
  setenv("XrdClientEUSER", "stage", 1);
  return rc;
}


/******************************************************************************/
/*                         x C a s t o r O f s F i l e                        */
/******************************************************************************/


//------------------------------------------------------------------------------
// Constuctor
//------------------------------------------------------------------------------
XrdxCastor2OfsFile::XrdxCastor2OfsFile(const char* user, int MonID) :
    XrdOfsFile(user, MonID),
    mStagerJob(NULL),
    mProcRequest(kProcNone),
    mEnvOpaque(NULL),
    mIsRW(false),
    mIsTruncate(false),
    mHasWrite(false),
    mViaDestructor(false),
    mReqId("0"),
    mHasAdlerErr(false),
    mHasAdler(true),
    mAdlerOffset(0),
    mXsValue(""),
    mXsType(""),
    mIsClosed(false),
    mTpcKey("")
{
  mAdlerXs = adler32(0L, Z_NULL, 0);
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
XrdxCastor2OfsFile::~XrdxCastor2OfsFile()
{
  mViaDestructor = true;
  close();

  if (mEnvOpaque) delete mEnvOpaque;
  if (mStagerJob) delete mStagerJob;

  mEnvOpaque = NULL;
  mStagerJob = 0;
}


//------------------------------------------------------------------------------
// Open file
//------------------------------------------------------------------------------
int
XrdxCastor2OfsFile::open(const char*         path,
                         XrdSfsFileOpenMode  open_mode,
                         mode_t              create_mode,
                         const XrdSecEntity* client,
                         const char*         opaque)
{
  xcastor_debug("path=%s", path);
  const char* tident = error.getErrUser();
  xcastor::Timing opentiming("ofsf::open");
  TIMING("START", &opentiming);
  XrdOucString spath = path;
  xcastor_debug("castorlfn=%s, opaque=%s", path, opaque);
  XrdOucString newpath = "";
  XrdOucString newopaque = opaque;
  // If there is explicit user opaque information we find two ?,
  // so we just replace it with a seperator.
  newopaque.replace("?", "&");
  newopaque.replace("&&", "&");

  // Check if there are several redirection tokens
  int firstpos = 0;
  int lastpos = 0;
  int newpos = 0;
  firstpos = newopaque.find("castor2fs.sfn", 0);
  lastpos = firstpos + 1;

  while ((newpos = newopaque.find("castor2fs.sfn", lastpos)) != STR_NPOS)
  {
    lastpos = newpos + 1;
  }

  // Erase from the beginning to the last token start
  if (lastpos > (firstpos + 1))
    newopaque.erase(firstpos, lastpos - 2 - firstpos);

  // Erase the tried parameter from the opaque information
  int tried_pos = newopaque.find("tried=");
  int amp_pos = newopaque.find('&', tried_pos);

  if (tried_pos != STR_NPOS)
    newopaque.erase(tried_pos, amp_pos - tried_pos);

  // This prevents 'clever' users from faking internal opaque information
  newopaque.replace("ofsgranted=", "notgranted=");
  newopaque.replace("source=", "nosource=");
  mEnvOpaque = new XrdOucEnv(newopaque.c_str());
  newpath = mEnvOpaque->Get("castor2fs.pfn1");

  if (newopaque.endswith("&"))
    newopaque += "source=";
  else
    newopaque += "&source=";

  newopaque += newpath.c_str();

  if (!mEnvOpaque->Get("target"))
  {
    if (newopaque.endswith("&"))
      newopaque += "target=";
    else
      newopaque += "&target=";

    newopaque += newpath.c_str();
  }

  if (mEnvOpaque->Get("castor2ofsproc"))
  {
    // that is strange ;-) ....
    newopaque.replace("castor2ofsproc", "castor2ofsfake");
  }

  open_mode |= SFS_O_MKPTH;
  create_mode |= SFS_O_MKPTH;
  mIsTruncate = open_mode & SFS_O_TRUNC;

  if ((open_mode & (SFS_O_CREAT | SFS_O_TRUNC |
                    SFS_O_WRONLY | SFS_O_RDWR)) != 0)
  {
    mIsRW = true;
  }

  if (spath.beginswith("/proc/"))
  {
    mProcRequest = kProcRead;

    if (mIsRW)
      mProcRequest = kProcWrite;

    // Declare this as a proc path to the authorization plugin
    newopaque += "&";
    newopaque += "castor2ofsproc=true";

    // Re-root the /proc/ location
    XrdOucString proc_path = gSrv->mProcFs;
    proc_path += "/";
    proc_path += spath;

    while (proc_path.replace("//", "/")) {};

    // Check the allowed proc users - this is not a high performance implementation
    // but the usage of /proc is very rare anyway.Clients are allowed to read/modify
    // /proc, when they appear in the configuration file f.e. as
    // -> xcastor2.procUsers xrootd@pcitmeta.cern.ch
    // where the user name in front of @ is the UID name on the client side
    // and the host name after the @ is the hostname without domain.
    XrdOucString reducedTident, user, stident;
    reducedTident = "";
    user = "";
    stident = tident;
    int pos = stident.find(".");
    reducedTident.assign(tident, 0, pos - 1);
    reducedTident += "@";
    pos = stident.find("@");
    user.assign(tident, pos + 1);
    reducedTident += user;
    bool allowed = false;

    for (int i = 0 ; i < gSrv->procUsers.GetSize(); i++)
    {
      if (gSrv->procUsers[i] == reducedTident)
      {
        allowed = true;
        break;
      }
    }

    if (!allowed)
    {
      return gSrv->Emsg("open", error, EPERM, "allow access to /proc/ fs in xrootd",
                        path);
    }

    if (mProcRequest == kProcWrite)
    {
      open_mode = SFS_O_RDWR | SFS_O_TRUNC;
      proc_path += ".__proc_update__";
    }
    else
    {
      // Depending on the desired file, we update the information in that file
      if (!gSrv->UpdateProc(proc_path.c_str()))
        return gSrv->Emsg("open", error, EIO, "update /proc information", path);
    }

    // We have to open the re-rooted proc path!
    int rc = XrdOfsFile::open(proc_path.c_str(), open_mode, create_mode,
                              client, newopaque.c_str());
    return rc;
  }
  else
  {
    // Deny this as a proc path to the authorization plugin
    newopaque += "&";
    newopaque += "castor2ofsproc=false";
  }

  char* val = 0;

  // Deal with native TPC transfers which are passed directly to the OFS layer
  if (newopaque.find("tpc.dst") != STR_NPOS)
  {
    // This is a source TPC file and we just received the second open reuqest from the initiator.
    // We save the mapping between the tpc.key and the castor lfn for future open requests from
    // the destination of the TPC transfer.
    if ((val = mEnvOpaque->Get("tpc.key")))
    {
      mTpcKey = val;
      struct TpcInfo transfer = (struct TpcInfo)
      {
        std::string(newpath.c_str()), time(NULL)
      };
      
      gSrv->mTpcMapMutex.Lock();     // -->
      std::pair< std::map<std::string, struct TpcInfo>::iterator, bool> pair =
        gSrv->mTpcMap.insert(std::make_pair(mTpcKey, transfer));

      if (pair.second == false)
      {
        xcastor_err("tpc.key:%s is already in the map", mTpcKey.c_str());
        gSrv->mTpcMap.erase(pair.first);
        gSrv->mTpcMapMutex.UnLock(); // <--
        return gSrv->Emsg("open", error, EINVAL,
                          "tpc.key already in the map for file:  ",
                          newpath.c_str());
      }

      gSrv->mTpcMapMutex.UnLock();   // <--
      int rc = XrdOfsFile::open(newpath.c_str(), open_mode, create_mode, client,
                                newopaque.c_str());
      return rc;
    }

    xcastor_err("no tpc.key in the opaque information");
    return gSrv->Emsg("open", error, ENOENT,
                      "no tpc.key in the opaque info for file: ",
                      newpath.c_str());
  }
  else if (newopaque.find("tpc.org") != STR_NPOS)
  {
    // This is a source TPC file and we just received the third open request which
    // comes directly from the destination of the transfer. Here we need to retrieve
    // the castor lfn from the map which we saved previously as the destination has
    // no knowledge of the castor mapping.
    if ((val = mEnvOpaque->Get("tpc.key")))
    {
      std::string key = val;
      gSrv->mTpcMapMutex.Lock();     // -->
      std::map<std::string, struct TpcInfo>::iterator iter = gSrv->mTpcMap.find(key);

      if (iter != gSrv->mTpcMap.end())
      {
        std::string saved_lfn = iter->second.path;
        gSrv->mTpcMapMutex.UnLock(); // <--
        int rc = XrdOfsFile::open(saved_lfn.c_str(), open_mode, create_mode,
                                  client, newopaque.c_str());
        return rc;
      }

      gSrv->mTpcMapMutex.UnLock(); // <--
      return gSrv->Emsg("open", error, EINVAL,
                        "can not find tpc.key in map for file: ",
                        newpath.c_str());
    }

    xcastor_err("no tpc.key in the opaque information");
    return gSrv->Emsg("open", error, ENOENT,
                      "no tpc.key in the opaque info for file: ",
                      newpath.c_str());
  }
  else if (newopaque.find("tpc.src") != STR_NPOS)
  {
    // This is a TPC destination and we force the recomputation of the checksum
    // at the end since all writes go directly to the file without passing through
    // the write method in OFS.
    mHasWrite = true;
    mHasAdler = false;
  }
  
  TIMING("PROCBLOCK", &opentiming);

  // Extract the stager information
  val = mEnvOpaque->Get("castor2fs.pfn2");

  if (!val || !strlen(val))
    return gSrv->Emsg("open", error, ESHUTDOWN, "reqid/pfn2 is missing", FName());

  XrdOucString reqtag = val;
  XrdOucString sjob_uuid = "";
  int sjob_port = 0;
  int pos1, pos2;

  // Syntax for request is: <reqid:stagerJobPort:stagerJobUuid>
  if ((pos1 = reqtag.find(":")) != STR_NPOS)
  {
    mReqId.assign(reqtag, 0, pos1 - 1);

    if ((pos2 = reqtag.find(":", pos1 + 1)) != STR_NPOS)
    {
      XrdOucString sport;
      sport.assign(reqtag, pos1 + 1, pos2 - 1);
      sjob_port = atoi(sport.c_str());
      sjob_uuid.assign(reqtag.c_str(), pos2 + 1);
    }
  }

  xcastor_debug("StagerJob uuid=%s, port=%i ", sjob_uuid.c_str(), sjob_port);
  mStagerJob = new XrdxCastor2Ofs2StagerJob(sjob_uuid.c_str(), sjob_port);

  // Send the sigusr1 to the local xcastor2job to signal the open
  if (!mStagerJob->Open())
  {
    xcastor_err("error: open => couldn't run the Open towards StagerJob: "
                "reqid=%s, stagerjobport=%i, stagerjobuuid=%s",
                mReqId.c_str(), sjob_port, sjob_uuid.c_str());
    return gSrv->Emsg("open", error, EIO,
                      "signal open to the corresponding stagerjob");
  }

  TIMING("FILEOPEN", &opentiming);
  int rc = XrdOfsFile::open(newpath.c_str(), open_mode, create_mode, client,
                            newopaque.c_str());

  if (rc == SFS_OK)
  {
    // Try to get the file checksum from the filesystem
    int nattr = 0;
    char disk_xs_buf[32 + 1];
    char disk_xs_type[32];
    disk_xs_buf[0] = disk_xs_type[0] = '\0';
    
    // Get existing checksum - we don't check errors here
    nattr = getxattr(newpath.c_str(), "user.castor.checksum.type", disk_xs_type,
                     sizeof(disk_xs_type));
    
    if (nattr) disk_xs_type[nattr] = '\0';
    
    nattr = getxattr(newpath.c_str(), "user.castor.checksum.value", disk_xs_buf,
                     sizeof(disk_xs_buf));
    
    if (nattr) disk_xs_buf[nattr] = '\0';
    
    mXsValue = disk_xs_buf;
    mXsType = disk_xs_type;
    xcastor_debug("xs_type=%s, xs_val=%s", mXsType.c_str(),  mXsValue.c_str());
    
    // Get also the size of the file 
    if (XrdOfsOss->Stat(newpath.c_str(), &mStatInfo))
    {
      xcastor_err("error: getting file stat information");
      rc = SFS_ERROR;
    }
  }
  
  TIMING("DONE", &opentiming);
  opentiming.Print();
  return rc;
}


//------------------------------------------------------------------------------
// Close file
//------------------------------------------------------------------------------
int
XrdxCastor2OfsFile::close()
{
  int rc = SFS_OK;

  if (mIsClosed)
    return true;

  // If this is a TPC transfer then we can drop the key from the map
  if (mTpcKey != "")
  {
    xcastor_debug("drop from map tpc.key=%s", mTpcKey.c_str());
    XrdSysMutexHelper lock(gSrv->mTpcMapMutex);
    gSrv->mTpcMap.erase(mTpcKey);
    mTpcKey = "";

    // Remove keys which are older than one hour
    std::map<std::string, struct TpcInfo>::iterator iter = gSrv->mTpcMap.begin();
    time_t now = time(NULL);

    while (iter != gSrv->mTpcMap.end())
    {
      if (now - iter->second.expire > 3600)
      {
        xcastor_info("expire tpc.key=%s", iter->first.c_str());
        gSrv->mTpcMap.erase(iter++);
      }
      else
      {
        ++iter;
      }
    }
  }

  mIsClosed = true;
  XrdOucString spath = FName();

  if (mProcRequest == kProcWrite)
  {
    sync();
    XrdOucString newpath = spath;
    newpath.erase(".__proc_update__");
    gSrv->rem(newpath.c_str(), error, 0, "");
    int rc =  ::rename(spath.c_str(), newpath.c_str());
    // printf("rc is %d %s->%s\n",rc, spath.c_str(),newpath.c_str());
    gSrv->rem(spath.c_str(), error, 0, "");
    gSrv->ReadFromProc("trace");
    gSrv->UpdateProc(newpath.c_str());
    XrdOfsFile::close();
    return rc;
  }
  else
  {
    gSrv->ReadFromProc("trace");
  }

  char ckSumbuf[32 + 1];
  sprintf(ckSumbuf, "%x", mAdlerXs);
  char* ckSumalg = "ADLER32";
  XrdOucString newpath = "";
  newpath = mEnvOpaque->Get("castor2fs.pfn1");

  if (mHasWrite)
  {
    if (!mHasAdler)
    {
      // Rescan the file and compute checksum
      xcastor_debug("rescan file and compute checksum");
      xcastor::Timing xs_timing("ofsf::checksum");
      TIMING("START", &xs_timing);
      char blk_xs_buf[64 * 1024];
      mAdlerXs = adler32(0L, Z_NULL, 0);
      mAdlerOffset = 0;
      XrdSfsFileOffset xs_offset = 0;
      XrdSfsXferSize xs_size = 0;

      while ((xs_size = read(xs_offset, blk_xs_buf, sizeof(blk_xs_buf))) > 0)
      {
        mAdlerXs = adler32(mAdlerXs, (const Bytef*)blk_xs_buf, xs_size);
        xs_offset += xs_size;
      }

      TIMING("STOP", &xs_timing);
      xs_timing.Print();
      mHasAdler = true;
    }

    sprintf(ckSumbuf, "%x", mAdlerXs);
    xcastor_debug("file xs=%s", ckSumbuf);
    
    if (setxattr(newpath.c_str(), "user.castor.checksum.type", ckSumalg,
                 strlen(ckSumalg), 0))
    {
      gSrv->Emsg("close", error, EIO, "set checksum type");
      rc = SFS_ERROR; // Anyway this is not returned to the client
    }
    else
    {
      if (setxattr(newpath.c_str(), "user.castor.checksum.value", ckSumbuf,
                   strlen(ckSumbuf), 0))
      {
        gSrv->Emsg("close", error, EIO, "set checksum");
        rc = SFS_ERROR; // Anyway this is not returned to the client
      }
    }
  }

  bool file_ok = true;

  if ((rc = XrdOfsFile::close()))
  {
    gSrv->Emsg("close", error, EIO, "closing ofs file", "");
    file_ok = false;
    rc = SFS_ERROR;
  }

  // This means the file was not properly closed
  if (mViaDestructor)
  {
    Unlink();
    file_ok = false;
  }

  if (mStagerJob && (!mStagerJob->Close(file_ok, mHasWrite)))
  {
    // For the moment, we cannot send this back, but soon we will
    xcastor_debug("StagerJob close failed, got rc=%i and msg=%s",
                  mStagerJob->ErrCode, mStagerJob->ErrMsg.c_str());
    gSrv->Emsg("close", error, mStagerJob->ErrCode, mStagerJob->ErrMsg.c_str());
    rc = SFS_ERROR;
  }

  return rc;
}


//------------------------------------------------------------------------------
// Verify checksum
//------------------------------------------------------------------------------
bool
XrdxCastor2OfsFile::VerifyChecksum()
{
  bool rc = true;
  XrdOucString xs_val;
  XrdOucString xs_type;

  if ((mXsValue != "") && (mXsType != ""))
  {
    char ckSumbuf[32 + 1];
    sprintf(ckSumbuf, "%x", mAdlerXs);
    xs_val = ckSumbuf;
    xs_type = "ADLER32";

    if (xs_val != mXsValue)
    {
      gSrv->Emsg("VerifyChecksum", error, EIO, "checksum value wrong");
      rc = false;
    }

    if (xs_type != mXsType)
    {
      gSrv->Emsg("VerifyChecksum", error, EIO, "wrong checksum type");
      rc = false;
    }

    if (!rc)
    {
      xcastor_err("error: checksum %s != %s with algorithms [ %s  <=> %s ]",
                  xs_val.c_str(), mXsValue.c_str(),
                  xs_type.c_str(), mXsType.c_str());
    }
    else
    {
      xcastor_debug("checksum OK: %s", xs_val.c_str());
    }
  }

  return rc;
}


//------------------------------------------------------------------------------
// Read
//------------------------------------------------------------------------------
int
XrdxCastor2OfsFile::read(XrdSfsFileOffset fileOffset,   // Preread only
                         XrdSfsXferSize   amount)
{
  int rc = XrdOfsFile::read(fileOffset, amount);
  mHasAdler = false;
  return rc;
}


//------------------------------------------------------------------------------
// Read
//------------------------------------------------------------------------------
XrdSfsXferSize
XrdxCastor2OfsFile::read(XrdSfsFileOffset fileOffset,
                         char*            buffer,
                         XrdSfsXferSize   buffer_size)
{
  // If we once got an adler checksum error, we fail all reads.
  if (mHasAdlerErr)
  {
    xcastor_err("error: found xs error, fail read at off=%ll, size=%i", 
                fileOffset, buffer_size);
    return SFS_ERROR;
  }

  int rc = XrdOfsFile::read(fileOffset, buffer, buffer_size);

  // Computation of adler checksum - we disable it if seeks happen
  if (fileOffset != mAdlerOffset)
    mHasAdler = false;

  if (mHasAdler)
  {
    mAdlerXs = adler32(mAdlerXs, (const Bytef*) buffer, rc);

    if (rc > 0)
      mAdlerOffset += rc;
  }

  if (mHasAdler && (fileOffset + buffer_size >= mStatInfo.st_size))
  {
    // Invoke the checksum verification
    if (!VerifyChecksum())
    {
      mHasAdlerErr = true;
      rc = SFS_ERROR;
    }
  }

  return rc;
}


//------------------------------------------------------------------------------
// Read
//------------------------------------------------------------------------------
int
XrdxCastor2OfsFile::read(XrdSfsAio* aioparm)
{
  int rc = XrdOfsFile::read(aioparm);
  mHasAdler = false;
  return rc;
}


//------------------------------------------------------------------------------
// Write
//------------------------------------------------------------------------------
XrdSfsXferSize
XrdxCastor2OfsFile::write(XrdSfsFileOffset fileOffset,
                          const char*      buffer,
                          XrdSfsXferSize   buffer_size)
{
  if (mStagerJob && mStagerJob->Connected())
  {
    // If we have a stagerJob watching, check it is still alive
    if (!mStagerJob->StillConnected())
    {
      // StagerJob died ... reject any writes
      xcastor_err("error:write => StagerJob has been canceled");

      // We truncate this file to 0!
      if (!mIsClosed)
      {
        truncate(0);
        sync();
      }

      gSrv->Emsg("write", error, ECANCELED,
                 "write - the stagerjob write slot has been canceled ");
      return SFS_ERROR;
    }
  }

  int rc = XrdOfsFile::write(fileOffset, buffer, buffer_size);

  // Computation of adler checksum - we disable it if seek happened
  if (fileOffset != mAdlerOffset)
    mHasAdler = false;

  if (mHasAdler)
  {
    mAdlerXs = adler32(mAdlerXs, (const Bytef*) buffer, buffer_size);

    if (rc > 0)
      mAdlerOffset += rc;
  }

  mHasWrite = true;
  return rc;
}


//------------------------------------------------------------------------------
// Write
//------------------------------------------------------------------------------
int
XrdxCastor2OfsFile::write(XrdSfsAio* aioparm)
{
  if (mStagerJob && mStagerJob->Connected())
  {
    if (!mStagerJob->StillConnected())
    {
      // StagerJob died ... reject any writes
      xcastor_err("error:write => StagerJob has been canceled");

      // We truncate this file to 0!
      if (!mIsClosed)
      {
        truncate(0);
        sync();
      }

      gSrv->Emsg("write", error, ECANCELED,
                 "write - the stagerjob write slot has been canceled");
      return SFS_ERROR;
    }
    else
    {
      xcastor_debug("StagerJob is alive");
    }
  }

  int rc = XrdOfsFile::write(aioparm);
  mHasWrite = true;
  return rc;
}


//------------------------------------------------------------------------------
// Unlink
//------------------------------------------------------------------------------
int
XrdxCastor2OfsFile::Unlink()
{
  if (!gSrv->doPOSC || !mIsRW)
  {
    return SFS_OK;
  }

  XrdOucString preparename = mEnvOpaque->Get("castor2fs.sfn");
  XrdOucString secuid      = mEnvOpaque->Get("castor2fs.client_sec_uid");
  XrdOucString secgid      = mEnvOpaque->Get("castor2fs.client_sec_gid");
  xcastor_debug("unlink lfn=%s", preparename.c_str());

  // Don't deal with proc requests
  if (mProcRequest != kProcNone)
    return SFS_OK;

  XrdOucString mdsaddress = "root://";
  mdsaddress += mEnvOpaque->Get("castor2fs.manager");
  mdsaddress += "//dummy";
  XrdCl::URL url(mdsaddress.c_str());
  XrdCl::Buffer* buffer = 0;
  std::vector<std::string> list;

  if (!url.IsValid())
  {
    xcastor_debug("URL is not valid: %s", mdsaddress.c_str());
    return gSrv->Emsg("Unlink", error, ENOENT, "URL is not valid ");
  }

  XrdCl::FileSystem* mdsclient = new XrdCl::FileSystem(url);

  if (!mdsclient)
  {
    xcastor_err("Could not create XrdCl::FileSystem object.");
    return gSrv->Emsg("Unlink", error, EHOSTUNREACH,
                      "connect to the mds host (normally the manager)");
  }

  preparename += "?";
  preparename += "&unlink=true";
  preparename += "&reqid=";
  preparename += mReqId;
  preparename += "&uid=";
  preparename += secuid;
  preparename += "&gid=";
  preparename += secgid;
  list.push_back(preparename.c_str());
  xcastor_debug("Informing about %s", preparename.c_str());
  XrdCl::XRootDStatus status = mdsclient->Prepare(list,
                               XrdCl::PrepareFlags::Stage, 1, buffer);
  delete mdsclient;

  if (!status.IsOK())
  {
    xcastor_err("Prepare response invalid.");
    delete buffer;
    return gSrv->Emsg("Unlink", error, ESHUTDOWN, "unlink ", FName());
  }

  delete buffer;
  return SFS_OK;
}


/******************************************************************************/
/*              xCastorOfs -  StagerJob Interface                             */
/******************************************************************************/

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
XrdxCastor2Ofs2StagerJob::XrdxCastor2Ofs2StagerJob(const char* sjobuuid,
                                                   int port):
    LogId(),
    ErrCode(0),
    ErrMsg("undef"),
    mPort(port),
    mSjobUuid(sjobuuid),
    mSocket(0),
    mIsConnected(false)
{ }


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
XrdxCastor2Ofs2StagerJob::~XrdxCastor2Ofs2StagerJob()
{
  if (mSocket)
  {
    delete mSocket;
    mSocket = 0;
  }

  mIsConnected = false;
}


//------------------------------------------------------------------------------
// StagerJob open
//------------------------------------------------------------------------------
bool
XrdxCastor2Ofs2StagerJob::Open()
{
  mSocket = new XrdNetSocket();

  if ((mSocket->Open("localhost", mPort, 0, 0)) < 0)
  {
    xcastor_err("can not open socket for port=%i", mPort);
    return false;
  }

  mSocket->setOpts(mSocket->SockNum(), XRDNET_KEEPALIVE | XRDNET_DELAY);
  // Build IDENT message
  XrdOucString ident = "IDENT ";
  ident += mSjobUuid.c_str();
  ident += "\n";
  // Send IDENT message
  int nwrite = write(mSocket->SockNum(), ident.c_str(), ident.length());

  if (nwrite != ident.length())
  {
    xcastor_err("writing to sjob socket failed");
    return false;
  }

  mIsConnected = true;
  return true;
}


//------------------------------------------------------------------------------
// StagerJob still connected
//------------------------------------------------------------------------------
bool
XrdxCastor2Ofs2StagerJob::StillConnected()
{
  int sval;
  fd_set check_set;

  if (!mIsConnected || !mSocket)
    return false;

  // Add the socket to the select set
  FD_ZERO(&check_set);
  FD_SET(mSocket->SockNum(), &check_set);
  
  // Set select timeout to 0
  struct timeval time;
  time.tv_sec  = 0;
  time.tv_usec = 1;

  if ((sval = select(mSocket->SockNum() + 1, &check_set, 0, 0, &time)) < 0)
    return false;

  if (FD_ISSET(mSocket->SockNum(), &check_set))
    return false;

  return true;
}


//------------------------------------------------------------------------------
// StagerJob close
//------------------------------------------------------------------------------
bool
XrdxCastor2Ofs2StagerJob::Close(bool ok, bool haswrite)
{
  if (!mSocket || !mIsConnected)
    return true;

  XrdOucString ident = "CLOSE ";

  if (ok)
    ident += "0 ";
  else
    ident += "1 ";

  if (haswrite)
    ident += "1 \n";
  else
    ident += "0 \n";

  // Send CLOSE message
  xcastor_debug("Send CLOSE message to StagerJob");
  int nwrite = ::write(mSocket->SockNum(), ident.c_str(), ident.length());

  if (nwrite != ident.length())
  {
    ErrCode = EIO;
    ErrMsg = "Error writing CLOSE request to StagerJob";
    return false;
  }

  // Read CLOSE response
  char closeresp[16384];
  int  respcode;
  char respmesg[16384];
  int nread = 0;
  xcastor_debug("Read CLOSE response from StagerJob");
  nread = ::read(mSocket->SockNum(), closeresp, sizeof(closeresp));
  xcastor_debug("This read gave %i bytes and errno=%i", nread, errno);

  if (nread > 0)
  {
    xcastor_debug("Parse StagerJob CLOSE response");

    // Try to parse the response
    if ((sscanf(closeresp, "%d %[^\n]", &respcode, respmesg)) == 2)
    {
      ErrMsg = respmesg;
      ErrCode = respcode;

      if (ErrCode != 0)
        return false;
    }
    else
    {
      ErrCode = EINVAL;
      ErrMsg  = "StagerJob returned illegal response: ";
      ErrMsg += (char*)closeresp;
      return false;
    }
  }
  else
  {
    // That was an error
    ErrCode = errno;
    ErrMsg = "Error reading StagerJob response";
    return false;
  }

  return true;
}
