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
 * @author Castor Dev team, castor-dev@cern.ch
 *
 ******************************************************************************/

/*-----------------------------------------------------------------------------*/
#include <sstream>
#include <sys/types.h>
#include <sys/stat.h>
#include <grp.h>
#include <fcntl.h>
#include "Cns_api.h"
#include "serrno.h"
#include <string.h>
#include <zlib.h>
#include "ceph/ceph_posix.h"
#include "movers/moveropenclose.h"
/*----------------------------------------------------------------------------*/
#include "XrdVersion.hh"
#include "XrdAcc/XrdAccAuthorize.hh"
#include "XrdOfs/XrdOfs.hh"
#include "XrdSys/XrdSysDNS.hh"
#include "XrdSys/XrdSysTimer.hh"
#include "XrdOss/XrdOssApi.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "XrdNet/XrdNetSocket.hh"
#include "XrdSec/XrdSecEntity.hh"
/*----------------------------------------------------------------------------*/
#include "XrdxCastor2Ofs.hpp"
#include "XrdxCastor2FsConstants.hpp"
#include "XrdxCastor2Timing.hpp"
/*----------------------------------------------------------------------------*/

XrdxCastor2Ofs* gSrv; ///< global diskserver OFS handle

// Extern symbols
extern XrdOfs*     XrdOfsFS;
extern XrdSysError OfsEroute;
extern XrdOssSys*  XrdOfsOss;
extern XrdOss*     XrdOssGetSS(XrdSysLogger*, const char*, const char*);

XrdVERSIONINFO(XrdSfsGetFileSystem2, xCastor2Ofs);

// One minute for destination to contact us for tpc.key rendez-vous
const int XrdxCastor2OfsFile::sKeyExpiry = 60;


//------------------------------------------------------------------------------
// XrdSfsGetFileSystem2 version 2 also passes the envP info pointer
//------------------------------------------------------------------------------
extern "C"
{
  XrdSfsFileSystem* XrdSfsGetFileSystem2(XrdSfsFileSystem* native_fs,
                                         XrdSysLogger* lp,
                                         const char* configfn,
                                         XrdOucEnv* envP)
  {
    static XrdxCastor2Ofs myFS;
    // Do the herald thing
    OfsEroute.SetPrefix("castor2ofs_");
    OfsEroute.logger(lp);
    OfsEroute.Say("++++++ (c) 2014 CERN/IT-DSS xCastor2Ofs v1.0");

    // Initialize the subsystems
    gSrv = &myFS;
    gSrv->ConfigFN = (configfn && *configfn ? strdup(configfn) : 0);

    if (gSrv->Configure(OfsEroute, envP)) return 0;

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
int XrdxCastor2Ofs::Configure(XrdSysError& Eroute, XrdOucEnv* envP)
{
  char* var;
  const char* val;
  int  cfgFD;
  // Extract the manager from the config file
  XrdOucStream config_stream(&Eroute, getenv("XRDINSTANCE"));

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

        // Get the log level
        if (!strcmp("loglevel", var))
        {
          if (!(val = config_stream.GetWord()))
          {
            Eroute.Emsg("Config", "argument for debug level invalid set to INFO.");
            mLogLevel = LOG_INFO;
          }
          else
          {
            long int log_level = Logging::GetPriorityByString(val);

            if (log_level == -1)
            {
              // Maybe the log level is specified as an int from 0 to 7
              errno = 0;
              char* end;
              log_level = (int) strtol(val, &end, 10);

              if ((errno == ERANGE && ((log_level == LONG_MIN) || (log_level == LONG_MAX))) ||
                  ((errno != 0) && (log_level == 0)) ||
                  (end == val))
              {
                // There was an error default to LOG_INFO
                log_level = 6;
              }
            }

            mLogLevel = log_level;
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
      }
    }

    config_stream.Close();
  }

  // Setup the circular in-memory logging buffer
  XrdOucString unit = "ds@";
  unit += XrdSysDNS::getHostName();
  unit += ":1094";
  Logging::Init();
  Logging::SetLogPriority(mLogLevel);
  Logging::SetUnit(unit.c_str());
  xcastor_info("logging configured");
  // Parse the default XRootD directives
  int rc = XrdOfs::Configure(Eroute, envP);
  // Set the effective user for all the XrdClients used to issue 'prepares'
  // to redirector
  setenv("XrdClientEUSER", "stage", 1);
  return rc;
}


//------------------------------------------------------------------------------
// Set the log level for the XRootD server daemon
//------------------------------------------------------------------------------
void
XrdxCastor2Ofs::SetLogLevel(int logLevel)
{
  if (mLogLevel != logLevel)
  {
    xcastor_notice("update log level from=%s to=%s",
                   Logging::GetPriorityString(mLogLevel),
                   Logging::GetPriorityString(logLevel));
    mLogLevel = logLevel;
    Logging::SetLogPriority(mLogLevel);
  }
}


//------------------------------------------------------------------------------
// Stat file path
//------------------------------------------------------------------------------
int
XrdxCastor2Ofs::stat(const char* path,
                     struct stat* buf,
                     XrdOucErrInfo& einfo,
                     const XrdSecEntity* client,
                     const char* opaque)
{
  xcastor_debug("path=%s, opaque=%s", path, opaque);
  return XrdOfs::stat(path, buf, einfo, client, opaque);
}


//----------------------------------------------------------------------------
// Perform a filesystem control operation - in our case it returns the list
// of ongoing transfers in the current diskserver
//
// struct XrdSfsFSctl //!< SFS_FSCTL_PLUGIN/PLUGIO parameters
// {
//   const char            *Arg1;      //!< PLUGIO & PLUGIN
//         int              Arg1Len;   //!< Length
//         int              Arg2Len;   //!< Length
//   const char            *Arg2;      //!< PLUGIN opaque string
// };
//----------------------------------------------------------------------------
int
XrdxCastor2Ofs::FSctl(const int cmd,
                      XrdSfsFSctl& args,
                      XrdOucErrInfo& eInfo,
                      const XrdSecEntity* client)
{
  xcastor_debug("Calling FSctl with cmd=%i, arg1=%s, arg1len=%i, arg2=%s, "
                "arg2len=%i", cmd,
                (args.Arg1 ? args.Arg1: ""), args.Arg1Len,
                (args.Arg2 ? args.Arg2 : ""), args.Arg2Len);

  if (!client || !client->host || strncmp(client->host, "localhost", 9))
  {
    eInfo.setErrInfo(ENOTSUP, "operation possible only from localhost");
    return SFS_ERROR;
  }

  if (cmd == SFS_FSCTL_PLUGIO)
  {
    if (strncmp(args.Arg1, "/transfers", args.Arg1Len) == 0)
    {
      /*
      // Create authz object used to check the signature - requires authz
      // object which currently is not accessibe from XrdOfs i.e it's private
      XrdOucEnv tx_env(args.Arg2 ? args.Arg2 : 0, 0, client);

      if (client && ((XrdOfs*)this)->Authorization &&
          !((XrdOfs*)this)->Authorization->Access(client, args.Arg1, AOP_Read, tx_env))
       {
         return gSrv->Emsg(epname, eInfo, EACCES, "list transfers", pathp);
       }
      */
      std::ostringstream oss;
      oss << "[";

      {
        // Dump all the running transfers
        XrdSysMutexHelper scope_lock(mMutexTransfers);

        if (!mSetTransfers.empty())
        {
          std::set<std::string>::const_iterator iter = mSetTransfers.begin();
          oss << *iter ;

          for (++iter; iter != mSetTransfers.end(); ++iter)
            oss << ", " << *iter;
        }
      }

      oss << "]";
      eInfo.setErrInfo(oss.str().length(), oss.str().c_str());
      return SFS_DATA;
    }
    else
    {
      xcastor_err("Unkown type of command requested:%s", args.Arg1);
      eInfo.setErrInfo(ENOTSUP, "unknown query command");
      return SFS_ERROR;
    }
  }
  else
  {
    eInfo.setErrInfo(ENOTSUP, "operation not implemented");
    return SFS_ERROR;
  }
}


//------------------------------------------------------------------------------
// Add entry to the set of transfers
//------------------------------------------------------------------------------
bool
XrdxCastor2Ofs::AddTransfer(const std::string transfer_id)
{
  std::pair<std::set<std::string>::iterator, bool> ret;
  XrdSysMutexHelper scope_lock(mMutexTransfers);
  ret = mSetTransfers.insert(transfer_id);
  return ret.second;
}

//------------------------------------------------------------------------------
// Remove entry from the set of transfers
//------------------------------------------------------------------------------
size_t
XrdxCastor2Ofs::RemoveTransfer(const std::string transfer_id)
{
  if (!transfer_id.empty())
  {
    XrdSysMutexHelper scope_lock(mMutexTransfers);
    return mSetTransfers.erase(transfer_id);
  }

  return 0;
}


/******************************************************************************/
/*                         x C a s t o r O f s F i l e                        */
/******************************************************************************/


//------------------------------------------------------------------------------
// Constuctor
//------------------------------------------------------------------------------
XrdxCastor2OfsFile::XrdxCastor2OfsFile(const char* user, int MonID) :
  XrdOfsFile(user, MonID),
  mIsClosed(false),
  mIsRW(false),
  mHasWrite(false),
  mViaDestructor(false),
  mHasAdlerErr(false),
  mHasAdler(true),
  mAdlerOffset(0),
  mDiskMgrPort(0),
  mXsValue(""),
  mXsType(""),
  mTpcKey(""),
  mReqId("0"),
  mTransferId(""),
  mTpcFlag(TpcFlag::kTpcNone),
  mEnvOpaque(NULL)
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

  if (mEnvOpaque)
  {
    delete mEnvOpaque;
    mEnvOpaque = NULL;
  }
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
  xcastor::Timing open_timing("open");
  TIMING("START", &open_timing);
  XrdOucString spath = path;
  XrdOucString newpath = "";
  XrdOucString newopaque = opaque;

  // If there is explicit user opaque information we find two ?,
  // so we just replace it with a seperator.
  while (newopaque.replace("?", "&")) { };
  while (newopaque.replace("&&", "&")) { };

  // Check if there are several redirection tokens
  int firstpos = 0;
  int lastpos = 0;
  int newpos = 0;
  firstpos = newopaque.find("castor.sfn", 0);
  lastpos = firstpos + 1;

  while ((newpos = newopaque.find("castor.sfn", lastpos)) != STR_NPOS)
    lastpos = newpos + 1;

  // Erase from the beginning to the last token start
  if (lastpos > (firstpos + 1))
    newopaque.erase(firstpos, lastpos - 2 - firstpos);

  // Erase the tried parameter from the opaque information
  int tried_pos = newopaque.find("tried=");
  int amp_pos = newopaque.find('&', tried_pos);

  if (tried_pos != STR_NPOS)
  {
    if (amp_pos != STR_NPOS)
      newopaque.erase(tried_pos, amp_pos - tried_pos);
    else
      newopaque.erase(tried_pos, newopaque.length() - tried_pos);
  }

  // Set the open flags and type of operation rd_only/rdwr
  mEnvOpaque = new XrdOucEnv(newopaque.c_str());
  newpath = (mEnvOpaque->Get("castor.pfn1") ?
             mEnvOpaque->Get("castor.pfn1") : "");
  open_mode |= SFS_O_MKPTH;
  create_mode |= SFS_O_MKPTH;

  if (open_mode & (SFS_O_CREAT | SFS_O_TRUNC | SFS_O_WRONLY | SFS_O_RDWR))
    mIsRW = true;

  xcastor_info("path=%s, opaque=%s, isRW=%d, open_mode=%x", path, opaque,
               mIsRW, open_mode);

  // Deal with native TPC transfers which are passed directly to the OFS layer
  if (newopaque.find("tpc.") != STR_NPOS)
  {
    if (PrepareTPC(newpath, newopaque, client))
      return SFS_ERROR;
  }
  else
  {
    if (ExtractTransferInfo(*mEnvOpaque))
      return SFS_ERROR;
  }

  // Build the transfer id if either this a normal transfer or a TPC transfer
  // in TpcSrcRead or TpcDstSetup state
  if ((mTpcFlag == TpcFlag::kTpcNone) ||
      (mTpcFlag == TpcFlag::kTpcSrcRead || mTpcFlag == TpcFlag::kTpcDstSetup))
  {
    BuildTransferId(client, mEnvOpaque);
  }

  // Try to get the file checksum type and value if file exists
  if (!XrdOfsOss->Stat(newpath.c_str(), &mStatInfo, 0, mEnvOpaque))
  {
    int nattr = 0;
    char buf[32];
    // Deal with ceph pool if any
    XrdOucString poolAndPath = newpath;

    if (mEnvOpaque->Get("castor.pool"))
      poolAndPath = mEnvOpaque->Get("castor.pool") + '/' + newpath;

    // Get existing checksum - we don't check errors here
    nattr = ceph_posix_getxattr(poolAndPath.c_str(), "user.castor.checksum.type",
                                buf, 32);
    if (nattr >= 0)
    {
      buf[nattr] = '\0';
      mXsType = buf;
    }

    nattr = ceph_posix_getxattr(poolAndPath.c_str(), "user.castor.checksum.value",
                                buf, 32);
    if (nattr >= 0)
    {
      buf[nattr] = '\0';
      mXsValue = buf;
    }

    if (!mXsType.empty() || !mXsValue.empty())
      xcastor_debug("xs_type=%s, xs_val=%s", mXsType.c_str(), mXsValue.c_str());
  }
  else
  {
    // Initial file size is 0
    mStatInfo.st_size = 0;
  }

  TIMING("OFS_OPEN", &open_timing);
  int rc = XrdOfsFile::open(newpath.c_str(), open_mode, create_mode, client,
                            newopaque.c_str());

  // Notify the diskmanager
  if (!mTransferId.empty())
  {
    TIMING("NOTIFY_DM", &open_timing);
    char* errmsg = (char*)0;
    xcastor_debug("send_dm errc=%i, tx_id=%s", rc, mTransferId.c_str());
    int dm_errno = mover_open_file(mDiskMgrPort, mTransferId.c_str(), &rc, &errmsg);

    // If failed to commit to diskmanager then return error
    if (dm_errno)
    {
      rc = gSrv->Emsg("open", error, dm_errno, "open due to diskmanager error: ",
		      errmsg);
    }
    else
    {
      TIMING("ADD_TX", &open_timing);

      if (!gSrv->AddTransfer(mTransferId))
      {
        xcastor_err("Failed to insert tx_id=%s", mTransferId.c_str());
        rc = gSrv->Emsg("open", error, EIO, "add entry to transfer set");
      }
    }

    // Free memory
    if (errmsg) free(errmsg);
  }

  if (gSrv->mLogLevel == LOG_DEBUG)
  {
    TIMING("DONE", &open_timing);
    open_timing.Print();
  }

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
    return SFS_OK;

  mIsClosed = true;
  xcastor::Timing close_timing("close");
  TIMING("START", &close_timing);

  // If this is a TPC transfer then we can drop the key from the map
  if (mTpcKey != "")
  {
    xcastor_debug("drop from map tpc.key=%s", mTpcKey.c_str());
    XrdSysMutexHelper tpc_lock(gSrv->mTpcMapMutex);
    gSrv->mTpcMap.erase(mTpcKey);
    mTpcKey.clear();

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

  TIMING("CLEAN_TPC", &close_timing);
  char ckSumbuf[32 + 1];
  sprintf(ckSumbuf, "%x", mAdlerXs);
  char* ckSumalg = "ADLER32";
  std::string newpath = mEnvOpaque->Get("castor.pfn1");

  if (mHasWrite)
  {
    if (!mHasAdler)
    {
      // Rescan the file and compute checksum
      xcastor_debug("rescan file and compute checksum");
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

      mHasAdler = true;
    }

    // Deal with ceph pool if any
    std::string poolAndPath = newpath;
    char* pool = mEnvOpaque->Get("castor.pool");

    if (pool)
    {
      poolAndPath = pool;
      poolAndPath += '/';
      poolAndPath += newpath;
    }

    sprintf(ckSumbuf, "%x", mAdlerXs);
    xcastor_debug("file xs=%s", ckSumbuf);

    if (ceph_posix_setxattr(poolAndPath.c_str(), "user.castor.checksum.type",
                            ckSumalg, strlen(ckSumalg), 0))
    {
      xcastor_err("path=%s unable to set xs type", newpath.c_str());
      rc = gSrv->Emsg("close", error, EIO, "set checksum type");
    }
    else
    {
      if (ceph_posix_setxattr(poolAndPath.c_str(), "user.castor.checksum.value",
                              ckSumbuf, strlen(ckSumbuf), 0))
      {
        xcastor_err("path=%s unable to set xs value", newpath.c_str());
        rc = gSrv->Emsg("close", error, EIO, "set checksum");
      }
    }
  }

  TIMING("DONE_XS", &close_timing);

  if ((rc = XrdOfsFile::close()))
  {
    xcastor_err("path=%s failed closing ofs file", newpath.c_str());
    rc = gSrv->Emsg("close", error, EIO, "closing ofs file");
  }

  // This means the file was not properly closed
  if (mViaDestructor)
  {
    xcastor_err("path=%s closed via destructor", newpath.c_str());
    rc = gSrv->Emsg("close", error, EIO, "close file - delete via destructor");
  }

  // If we have contact info for the diskmanager then we contact it to pass the
  // status otherwise this is a d2d or an rtcpd transfer and we don't need to
  // contact the diskmanager.
  if (mDiskMgrPort)
  {
    TIMING("NOTIFY_DM", &close_timing);
    uint64_t sz_file = 0;

    // Get the size of the file for writes
    if (mHasWrite)
    {
      if (XrdOfsOss->Stat(newpath.c_str(), &mStatInfo, 0, mEnvOpaque))
      {
        xcastor_err("path=%s failed stat", newpath.c_str());
        rc = gSrv->Emsg("close", error, EIO, "stat file");
      }
      else
        sz_file = mStatInfo.st_size;
    }

    int errc = (rc ? error.getErrInfo() : rc);
    char* errmsg = (rc ? strdup(error.getErrText()) : (char*)0);
    xcastor_info("send_dm errc=%i, errmsg=\"%s\"", errc, (errmsg ? errmsg : ""));
    int dm_errno = mover_close_file(mDiskMgrPort, mReqId.c_str(), sz_file,
                                    const_cast<const char*>(ckSumalg),
                                    const_cast<const char*>(ckSumbuf),
                                    &errc, &errmsg);

    // If failed to commit to diskmanager then return error
    if (dm_errno)
    {
      xcastor_err("error_dm path=%s errmsg=\"%s\"", newpath.c_str(), errmsg);
      rc = gSrv->Emsg("close", error, dm_errno, "close due to diskmanager error");
    }

    // Free errmsg memory
    if (errmsg)
      free(errmsg);
  }

  // Remove entry from the set of transfers
  if (!mTransferId.empty() && !gSrv->RemoveTransfer(mTransferId))
    xcastor_warning("Tx_id=\"%s\" not in the set of transfers", mTransferId.c_str());

  if (gSrv->mLogLevel == LOG_DEBUG)
  {
    TIMING("DONE", &close_timing);
    close_timing.Print();
  }

  return rc;
}


//------------------------------------------------------------------------------
// Build transfer identifier
//------------------------------------------------------------------------------
void
XrdxCastor2OfsFile::BuildTransferId(const XrdSecEntity* client, XrdOucEnv* env)
{
  std::string castor_path;
  std::string tx_type = "unknown";
  std::string tident = client->tident;

  if (env && env->Get("castor.txtype"))
    tx_type = env->Get("castor.txtype");

  if (env && env->Get("castor.pfn1"))
    castor_path = env->Get("castor.pfn1");

  // Try to use the FQDN of the client origin if it is present
  if (client->host)
  {
    tident = tident.erase(tident.find("@") + 1);
    tident += client->host;
  }

  std::ostringstream oss;
  oss << "("
      << "\"" << tident << "\", "
      << "\"" << castor_path << "\", "
      << "\"" << tx_type << "\", "
      << "\"" << mIsRW << "\", "
      << "\"" << mReqId << "\""
      << ")";

  mTransferId = oss.str();
}


//------------------------------------------------------------------------------
// Prepare TPC transfer - save info in the global TPC map, do checks etc.
//------------------------------------------------------------------------------
int
XrdxCastor2OfsFile::PrepareTPC(XrdOucString& path,
                               XrdOucString& opaque,
                               const XrdSecEntity* client)
{
  char* val;
  xcastor_info("path=%s", path.c_str());

  // Get the current state of the TPC transfer
  if ((val = mEnvOpaque->Get("tpc.stage")))
  {
    if (strncmp(val, "placement", 9) == 0)
    {
      mTpcFlag = TpcFlag::kTpcSrcCanDo;
    }
    else if (strncmp(val, "copy", 4) == 0)
    {
      if (!(val = mEnvOpaque->Get("tpc.key")))
      {
        xcastor_err("missing tpc.key information");
        return gSrv->Emsg("open", error, EACCES, "open - missing tpc.key info");
      }

      if ((val = mEnvOpaque->Get("tpc.lfn")))
      {
        mTpcFlag = TpcFlag::kTpcDstSetup;
      }
      else if ((val = mEnvOpaque->Get("tpc.dst")))
      {
        mTpcFlag = TpcFlag::kTpcSrcSetup;
        mTpcKey = mEnvOpaque->Get("tpc.key"); // save key only at this stage
      }
      else
      {
        xcastor_err("missing tpc options in copy stage");
        return gSrv->Emsg("open", error, EACCES, "open - missing tpc options",
                          "in copy stage");
      }
    }
    else
    {
      xcastor_err("unknown tpc.stage=", val);
      return gSrv->Emsg("open", error, EACCES, "open - unknown tpc.stage=", val);
    }
  }
  else if ((val = mEnvOpaque->Get("tpc.key")) &&
           (val = mEnvOpaque->Get("tpc.org")))
  {
    mTpcFlag = TpcFlag::kTpcSrcRead;
  }

  if (mTpcFlag == TpcFlag::kTpcSrcSetup)
  {
    // This is a source TPC file and we just received the second open reuqest
    // from the initiator. We save the mapping between the tpc.key and the
    // castor pfn for future open requests from the destination of the TPC transfer.
    std::string castor_pfn = mEnvOpaque->Get("castor.pfn1");
    std::string tpc_org = client->tident;
    tpc_org.erase(tpc_org.find(":"));
    tpc_org += "@";
    tpc_org += client->addrInfo->Name();;
    struct TpcInfo transfer = (struct TpcInfo)
    {
      castor_pfn,
      tpc_org,
      std::string(opaque.c_str()),
      time(NULL) + sKeyExpiry
    };
    {
      // Insert new key-transfer pair in the map
      XrdSysMutexHelper tpc_lock(gSrv->mTpcMapMutex);
      std::pair< std::map<std::string, struct TpcInfo>::iterator, bool> pair =
        gSrv->mTpcMap.insert(std::make_pair(mTpcKey, transfer));

      if (pair.second == false)
      {
        xcastor_err("tpc.key=%s is already in the map", mTpcKey.c_str());
        gSrv->mTpcMap.erase(pair.first);
        return gSrv->Emsg("open", error, EINVAL,
                          "tpc.key already in the map for file=", path.c_str());
      }
    }
  }
  else if (mTpcFlag == TpcFlag::kTpcSrcRead)
  {
    // This is a source TPC file and we just received the third open request which
    // comes directly from the destination of the transfer. Here we need to retrieve
    // the castor lfn from the map which we saved previously as the destination has
    // no knowledge of the castor physical file name.
    std::string check_key =  mEnvOpaque->Get("tpc.key");
    // Get init opaque just for stager job information and don't touch the current
    // opaque info as it contains the tpc.key and tpc.org info needed by the OFS
    // layer to perform the actual tpc transfer
    std::string init_opaque;
    {
      bool found_key = false;
      int num_tries = 150;
      XrdSysMutexHelper tpc_lock(gSrv->mTpcMapMutex);
      std::map<std::string, struct TpcInfo>::iterator iter =
        gSrv->mTpcMap.find(check_key);

      while ((num_tries > 0) && !found_key)
      {
        num_tries--;

        if (iter != gSrv->mTpcMap.end())
        {
          // Check if the key is still valid
          if (iter->second.expire > time(NULL))
          {
            std::string tpc_org = mEnvOpaque->Get("tpc.org");

            if (tpc_org != iter->second.org)
            {
              xcastor_err("tpc.org from destination=%s, not matching initiator orig.=%s",
                          tpc_org.c_str(), iter->second.org.c_str());
              return gSrv->Emsg("open", error, EPERM, "PrepareTPC - destination "
                                "tpc.org not matching initiator origin");
            }

            path = iter->second.path.c_str();
            init_opaque = iter->second.opaque;
            found_key = true;
          }
          else
          {
            xcastor_err("tpc key expired");
            return gSrv->Emsg("open", error, EKEYEXPIRED, "PrepareTPC - "
                              "tpc key expired");
          }
        }
        else
        {
          // Wait for the initiator to show up with the proper key and unlock
          // the map so that he can add the key
          gSrv->mTpcMapMutex.UnLock(); // <--
          XrdSysTimer timer;
          timer.Wait(100);
          xcastor_debug("wait for initator to come with the tpc.key");
          gSrv->mTpcMapMutex.Lock();  // -->
        }
      }

      if (!found_key)
      {
        xcastor_err("tpc key from destination not in map");
        return gSrv->Emsg("open", error, EINVAL, "PrepareTPC - can not find "
                          "tpc.key in map");
      }
    }

    // Now we can extract the transfer info and update the mEnvOpaque to the
    // inital value from the first request where we have the CASTOR dependent
    // information from the headnode
    delete mEnvOpaque;
    mEnvOpaque = new XrdOucEnv(init_opaque.c_str());

    if (ExtractTransferInfo(*mEnvOpaque))
      return SFS_ERROR;
  }
  else if (mTpcFlag == TpcFlag::kTpcDstSetup)
  {
    // This is a TPC destination and we force the recomputation of the checksum
    // at the end since all writes go directly to the file without passing through
    // the write method in OFS.
    mHasWrite = true;
    mHasAdler = false;
    // Now we can extract the transfer info
    XrdOucEnv env_opaque(opaque.c_str());

    if (ExtractTransferInfo(env_opaque))
      return SFS_ERROR;
  }

  return SFS_OK;
}


//------------------------------------------------------------------------------
// Extract diskmanager info from the opaque data
//------------------------------------------------------------------------------
int
XrdxCastor2OfsFile::ExtractTransferInfo(XrdOucEnv& env_opaque)
{
  char* val = env_opaque.Get("castor.pfn2");

  if (!val || !strlen(val))
  {
    xcastor_err("no diskmanager opaque information i.e. castor.pfn2 missing");
    return gSrv->Emsg("open", error, EIO, "get diskmanager opaque info (pfn2)");
  }

  std::string connect_info = val;
  int pos1, pos2;

  // Syntax for request is: <id:diskmanagerport:subReqId> out of which only the
  // last two values are used for the moment
  if ((pos1 = connect_info.find(":")) != STR_NPOS)
  {
    if ((pos2 = connect_info.find(":", pos1 + 1)) != STR_NPOS)
    {
      std::string sport(connect_info, pos1 + 1, pos2 - 1);
      mDiskMgrPort = atoi(sport.c_str());
      mReqId.assign(connect_info, pos2 + 1, std::string::npos);
    }
  }

  return SFS_OK;
}


//------------------------------------------------------------------------------
// Verify checksum
//------------------------------------------------------------------------------
bool
XrdxCastor2OfsFile::VerifyChecksum()
{
  bool rc = true;
  std::string xs_val;
  std::string xs_type;

  if (!mXsValue.empty() && !mXsType.empty())
  {
    char ckSumbuf[32 + 1];
    sprintf(ckSumbuf, "%x", mAdlerXs);
    xs_val = ckSumbuf;
    xs_type = "ADLER32";

    if (xs_val != mXsValue)
    {
      gSrv->Emsg("VerifyChecksum", error, EIO, "access the file, checksum mismatch");
      rc = false;
    }

    if (xs_type != mXsType)
    {
      gSrv->Emsg("VerifyChecksum", error, EIO, "access the file, checksum type mismatch");
      rc = false;
    }

    if (!rc)
    {
      xcastor_err("error: checksum %s != %s with algorithms [ %s <=> %s ]",
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
  xcastor_debug("off=%llu, adler_off=%llu, len=%i", fileOffset, mAdlerOffset, buffer_size);

  // If we once got an adler checksum error, we fail all reads
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
    {
      mAdlerOffset += rc;

      if (fileOffset + rc >= mStatInfo.st_size)
      {
        // Invoke the checksum verification
        if (!VerifyChecksum())
        {
          mHasAdlerErr = true;
          rc = SFS_ERROR;
        }
      }
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
  int rc = XrdOfsFile::write(aioparm);
  mHasWrite = true;
  return rc;
}
