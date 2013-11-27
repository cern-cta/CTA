/*******************************************************************************
 *                      XrdxCastor2Fs.cc
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
 * @uathor Andreas Peters <apeters@cern.ch>
 * @author Elvin Sindrilaru <esindril@cern.ch>
 *
 ******************************************************************************/

/*-----------------------------------------------------------------------------*/
#include <pwd.h>
#include <grp.h>
/*-----------------------------------------------------------------------------*/
#include "XrdVersion.hh"
#include "XrdOss/XrdOss.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdOuc/XrdOucTokenizer.hh"
#include "XrdOuc/XrdOucTList.hh"
#include "XrdSys/XrdSysError.hh"
#include "XrdSys/XrdSysLogger.hh"
#include "XrdSys/XrdSysDNS.hh"
#include "XrdSec/XrdSecInterface.hh"
#include "XrdSfs/XrdSfsAio.hh"
#include "XrdClient/XrdClientEnv.hh"
#include "XrdCl/XrdClFileSystem.hh"
/*-----------------------------------------------------------------------------*/
#include "XrdxCastor2Fs.hpp"
#include "XrdxCastor2FsDirectory.hpp"
#include "XrdxCastorTiming.hpp"
#include "XrdxCastor2Stager.hpp"
#include "XrdxCastor2FsSecurity.hpp"
#include "XrdxCastorClient.hpp"
/*-----------------------------------------------------------------------------*/
#include "Cthread_api.h"

#ifdef AIX
#include <sys/mode.h>
#endif

/******************************************************************************/
/*       O S   D i r e c t o r y   H a n d l i n g   I n t e r f a c e        */
/******************************************************************************/

#ifndef S_IAMB
#define S_IAMB  0x1FF
#endif

/******************************************************************************/
/*                        G l o b a l   O b j e c t s                         */
/******************************************************************************/
// TODO: move this inside the class and make them non-static
XrdOucHash<XrdOucString>* XrdxCastor2Stager::msDelayStore;
xcastor::XrdxCastorClient* XrdxCastor2Fs::msCastorClient;

int XrdxCastor2Fs::msTokenLockTime = 5;
XrdxCastor2Fs* gMgr;
XrdSysError OfsEroute(0);

//------------------------------------------------------------------------------
// Get file system
//------------------------------------------------------------------------------
extern "C"
XrdSfsFileSystem* XrdSfsGetFileSystem(XrdSfsFileSystem* native_fs,
                                      XrdSysLogger*     lp,
                                      const char*       configfn)
{
  OfsEroute.SetPrefix("xcastor2fs_");
  OfsEroute.logger(lp);
  static XrdxCastor2Fs myFS;
  OfsEroute.Say("++++++ (c) 2013 CERN/IT-DSS-DT ",
                "xCastor2Fs (xCastor2 File System) v 1.0 ");

  // Initialize the subsystem
  if (!myFS.Init()) return 0;

  myFS.ConfigFN = (configfn && *configfn ? strdup(configfn) : 0);

  if (myFS.Configure(OfsEroute)) return 0;

  gMgr = &myFS;
  // Initialize authorization module ServerAcc
  gMgr->mServerAcc = (XrdxCastor2ServerAcc*) XrdAccAuthorizeObject(lp, configfn, NULL);

  if (!gMgr->mServerAcc)
  {
    xcastor_static_err("failed loading the authorization plugin");
    return 0;
  }

  // Initialize the CASTOR Cthread library
  Cthread_init();
  return &myFS;
}


//------------------------------------------------------------------------------
// Create new directory
//------------------------------------------------------------------------------
XrdSfsDirectory*
XrdxCastor2Fs::newDir(char* user, int MonID)
{
  return (XrdSfsDirectory*)new XrdxCastor2FsDirectory((const char*)user, MonID);
}


//------------------------------------------------------------------------------
//! Create new file
//------------------------------------------------------------------------------
XrdSfsFile*
XrdxCastor2Fs::newFile(char* user, int MonID)
{
  return (XrdSfsFile*)new XrdxCastor2FsFile((const char*)user, MonID);
}


//------------------------------------------------------------------------------
// Set the ACL for a file
//------------------------------------------------------------------------------
void
XrdxCastor2Fs::SetAcl(const char* path, uid_t uid, gid_t gid, bool isLink)
{
  xcastor_debug("file=%s, uid/gid=%i/%i", path, (int)uid, int(gid));

  if (isLink)
  {
    if (XrdxCastor2FsUFS::Lchown(path , uid, gid))
      xcastor_err("failed file=%s, uid/gid=%i/%i", path, (int)uid, (int) gid);
  }
  else
  {
    if (XrdxCastor2FsUFS::Chown(path, uid, gid))
      xcastor_err("failed file=%s, uid/gid=%i/%i", path, (int)uid, (int) gid);
  }
}


//------------------------------------------------------------------------------
// Get allowed service class for the requested path and desired svc
//------------------------------------------------------------------------------
std::string
XrdxCastor2Fs::GetAllowedSvc(const char* path,
                             const std::string& desired_svc)
{
  xcastor_debug("path=%s desired_svc=%s", path, desired_svc.c_str());
  std::string spath = path;
  std::string subpath;
  std::string allowed_svc = "";
  size_t pos = 0;
  bool found = false;
  std::map< std::string, std::set<std::string> >::iterator iter_map;
  std::set<std::string>::iterator iter_set;

  // Only mapping by path is supported
  while ((pos = spath.find("/", pos)) != std::string::npos)
  {
    subpath.assign(spath, 0, pos + 1);
    xcastor_debug("sub_path=%s", subpath.c_str());
    iter_map = mStageMap.find(subpath);

    if (iter_map != mStageMap.end())
    {
      if (desired_svc == "*")
      {
        // Found path mapping, just return first svc
        // TODO: prefer default ?
        allowed_svc = *iter_map->second.begin();
        found = true;
      }
      else
      {
        // Search for a specific service map
        iter_set = iter_map->second.find(desired_svc);

        if (iter_set != iter_map->second.end())
        {
          found = true;
          allowed_svc = desired_svc;
        }
      }
    }

    pos++;
  }

  // Not found using path mapping check default configuration
  if (!found)
  {
    iter_map = mStageMap.find("default");

    if (iter_map != mStageMap.end())
    {
      if (desired_svc == "*")
      {
        // TODO: return first svc, prefer default ?
        allowed_svc = *iter_map->second.begin();
      }
      else
      {
        // Search for a specific service class
        iter_set = iter_map->second.find(desired_svc);

        if (iter_set != iter_map->second.end())
          allowed_svc = desired_svc;
      }
    }
  }

  return allowed_svc;
}


//------------------------------------------------------------------------------
// Get all allowed service classes for the requested path
//------------------------------------------------------------------------------
const std::set<std::string>&
XrdxCastor2Fs::GetAllAllowedSvc(const char* path)
{
  xcastor_debug("path=%s", path);
  std::string spath = path;
  std::string subpath;
  std::set<std::string>* set_svc;
  size_t pos = 0;
  bool found = false;
  std::map< std::string, std::set<std::string> >::iterator iter_map;
  std::set<std::string>::iterator iter_set;

  // Only mapping by path is supported
  while ((pos = spath.find("/", pos)) != std::string::npos)
  {
    subpath.assign(spath, 0, pos + 1);
    iter_map = mStageMap.find(subpath);

    if (iter_map != mStageMap.end())
    {
      set_svc = &iter_map->second;
      found = true;
    }

    pos++;
  }

  // Not found using path mapping check default configuration
  if (!found)
  {
    iter_map = mStageMap.find("default");

    if (iter_map != mStageMap.end())
      set_svc = &iter_map->second;
  }

  return *set_svc;
}


/******************************************************************************/
/*         F i l e   S y s t e m   O b j e c t   I n t e r f a c e s          */
/******************************************************************************/

//------------------------------------------------------------------------------
// Get plugin version
//------------------------------------------------------------------------------
const char*
XrdxCastor2Fs::getVersion()
{
  return XrdVERSION;
}


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
XrdxCastor2Fs::XrdxCastor2Fs():
  LogId(),
  mIssueCapability(false)
{
  ConfigFN  = 0;
  mLogLevel = LOG_INFO; // default log level

  // Setup the circular in-memory logging buffer
  XrdOucString unit = "rdr@";
  unit += XrdSysDNS::getHostName();
  unit += ":";
  unit += xCastor2FsTargetPort;
  Logging::Init();
  Logging::SetLogPriority(mLogLevel);
  Logging::SetUnit(unit.c_str());
  xcastor_info("info=\"logging configured\" ");
}


//------------------------------------------------------------------------------
// Initialisation
//------------------------------------------------------------------------------
bool
XrdxCastor2Fs::Init()
{
  mPasswdStore = new XrdOucHash<struct passwd> ();
  XrdxCastor2Stager::msDelayStore = new XrdOucHash<XrdOucString> ();
  msCastorClient = xcastor::XrdxCastorClient::Create();

  if (!msCastorClient)
  {
    OfsEroute.Emsg("Config", "error=failed to create castor client object");
    return false;
  }

  return true;
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
XrdxCastor2Fs::~XrdxCastor2Fs()
{
  delete mPasswdStore;
  delete msCastorClient;
}


//------------------------------------------------------------------------------
// Do namespace mappping
//------------------------------------------------------------------------------
std::string
XrdxCastor2Fs::NsMapping(const std::string& input)
{
  std::string output = "";
  std::string prefix;
  std::string value;
  size_t pos = input.find('/', 1);

  if (pos == std::string::npos)
  {
    std::map<std::string, std::string>::iterator iter_ns = mNsMap.find("/");

    if (iter_ns == mNsMap.end())
      return output;

    value = iter_ns->second;
    prefix = "/";
  }
  else
  {
    prefix.assign(input, 0, pos + 1);
    std::map<std::string, std::string>::iterator iter_ns = mNsMap.find(prefix);

    // First check the namespace with deepness 1
    if (iter_ns == mNsMap.end())
    {
      // If not found then check the namespace with deepness 0
      // e.g. look for a root mapping
      iter_ns = mNsMap.find("/");

      if (iter_ns == mNsMap.end())
        return output;

      value = iter_ns->second;
      prefix = "/";
    }
    else
    {
      value = iter_ns->second;
    }
  }

  output = value;
  output += input.substr(prefix.length());
  xcastor_debug("input=%s, output=%s", input.c_str(), output.c_str());
  return output;
}


//------------------------------------------------------------------------------
// Chmod on a file
//------------------------------------------------------------------------------
int
XrdxCastor2Fs::chmod(const char*         path,
                     XrdSfsMode          Mode,
                     XrdOucErrInfo&      error,
                     const XrdSecEntity* client,
                     const char*         info)
{
  static const char* epname = "chmod";
  mode_t acc_mode = Mode & S_IAMB;
  XrdOucEnv chmod_Env(info);
  xcastor_debug("path=%s", path);
  AUTHORIZE(client, &chmod_Env, AOP_Chmod, "chmod", path, error)
  std::string map_path = NsMapping(path);

  if (map_path.empty())
  {
    error.setErrInfo(ENOMEDIUM, "No mapping for file name");
    return SFS_ERROR;
  }

  if (gMgr->Proc)
    gMgr->Stats.IncCmd();

  // Set client identity
  SetIdentity(client);

  if (XrdxCastor2FsUFS::Chmod(map_path.c_str(), acc_mode))
    return XrdxCastor2Fs::Emsg(epname, error, serrno, "change mode on", map_path.c_str());

  return SFS_OK;
}


//------------------------------------------------------------------------------
// Determine if file exists
//------------------------------------------------------------------------------
int
XrdxCastor2Fs::exists(const char* path,
                      XrdSfsFileExistence& file_exists,
                      XrdOucErrInfo& error,
                      const XrdSecEntity* client,
                      const char* info)
{
  static const char* epname = "exists";
  XrdOucEnv exists_Env(info);
  xcastor_debug("path=%s", path);
  AUTHORIZE(client, &exists_Env, AOP_Stat, "execute exists", path, error)
  std::string map_path = NsMapping(path);

  if (map_path.empty())
  {
    error.setErrInfo(ENOMEDIUM, "No mapping for file name");
    return SFS_ERROR;
  }

  if (gMgr->Proc)
    gMgr->Stats.IncCmd();

  return _exists(map_path.c_str(), file_exists, error, client, info);
}


//------------------------------------------------------------------------------
// Test if file exists - low level
//------------------------------------------------------------------------------
int
XrdxCastor2Fs::_exists(const char* path,
                       XrdSfsFileExistence& file_exists,
                       XrdOucErrInfo& error,
                       const XrdSecEntity* client,
                       const char* info)
{
  static const char* epname = "exists";
  struct Cns_filestatcs fstat;
  // Set client identity
  SetIdentity(client);

  // Now try to find the file or directory
  if (!XrdxCastor2FsUFS::Statfn(path, &fstat))
  {
    if (S_ISDIR(fstat.filemode))
      file_exists = XrdSfsFileExistIsDirectory;
    else if (S_ISREG(fstat.filemode))
      file_exists = XrdSfsFileExistIsFile;
    else
      file_exists = XrdSfsFileExistNo;

    return SFS_OK;
  }

  if (serrno == ENOENT)
  {
    file_exists = XrdSfsFileExistNo;
    return SFS_OK;
  }

  // An error occured, return the error info
  return XrdxCastor2Fs::Emsg(epname, error, serrno, "locate", path);
}


//------------------------------------------------------------------------------
// Mkdir
//------------------------------------------------------------------------------
int
XrdxCastor2Fs::mkdir(const char*         path,
                     XrdSfsMode          Mode,
                     XrdOucErrInfo&      error,
                     const XrdSecEntity* client,
                     const char*         info)
{
  XrdOucEnv mkdir_Env(info);
  xcastor_debug("path=%s", path);
  std::string map_path = NsMapping(path);

  if (map_path.empty())
  {
    error.setErrInfo(ENOMEDIUM, "No mapping for file name");
    return SFS_ERROR;
  }

  if (gMgr->Proc)
    gMgr->Stats.IncCmd();

  int r1 = _mkdir(map_path.c_str(), Mode, error, client, info);
  int r2 = SFS_OK;
  return (r1 | r2);
}


//------------------------------------------------------------------------------
// Mkdir - low level
//------------------------------------------------------------------------------
int
XrdxCastor2Fs::_mkdir(const char* path,
                      XrdSfsMode Mode,
                      XrdOucErrInfo& error,
                      const XrdSecEntity* client,
                      const char* /*info*/)

{
  static const char* epname = "mkdir";
  mode_t acc_mode = (Mode & S_IAMB) | S_IFDIR;
  // Set client identity
  uid_t client_uid;
  gid_t client_gid;
  GetIdMapping(client, client_uid, client_gid);

  // Create the path if it does not already exist
  if (Mode & SFS_O_MKPTH)
  {
    char actual_path[4096], *local_path, *next_path;
    unsigned int plen;
    struct Cns_filestatcs buf;

    // Extract out the path we should make
    if (!(plen = strlen(path))) return -ENOENT;

    if (plen >= sizeof(actual_path)) return -ENAMETOOLONG;

    strcpy(actual_path, path);

    if (actual_path[plen - 1] == '/') actual_path[plen - 1] = '\0';

    // Typically, the path exist. So, do a quick check before launching into it
    if (!(local_path = rindex(actual_path, (int)'/'))
        || (local_path == actual_path))
    {
      return 0;
    }

    *local_path = '\0';

    if (XrdxCastor2FsUFS::Statfn(actual_path, &buf))
    {
      *local_path = '/';
      // Start creating directories starting with the root. Notice that we will not
      // do anything with the last component. The caller is responsible for that.
      local_path = actual_path + 1;

      while ((next_path = index(local_path, int('/'))))
      {
        int rc;
        *next_path = '\0';

        if ((rc = XrdxCastor2FsUFS::Mkdir(actual_path, S_IRWXU)) &&
            (serrno != EEXIST))
        {
          return -serrno;
        }

        // Set acl on directory
        if (!rc && client)
          SetAcl(actual_path, client_uid, client_gid, 0);

        *next_path = '/';
        local_path = next_path + 1;
      }
    }
  }

  // Perform the actual creation
  if (XrdxCastor2FsUFS::Mkdir(path, acc_mode) && (serrno != EEXIST))
    return XrdxCastor2Fs::Emsg(epname, error, serrno, "create directory", path);

  // Set acl on directory
  if (client)
    SetAcl(path, client_uid, client_gid, 0);

  return SFS_OK;
}


//------------------------------------------------------------------------------
// Stage prepare
//------------------------------------------------------------------------------
int
XrdxCastor2Fs::stageprepare(const char*         path,
                            XrdOucErrInfo&      error,
                            const XrdSecEntity* client,
                            const char*         ininfo)
{
  static const char* epname = "stageprepare";
  XrdOucString sinfo = (ininfo ? ininfo : "");
  const char* info = 0;
  int qpos = 0;
  // Get client identity
  uid_t client_uid;
  gid_t client_gid;
  GetIdMapping(client, client_uid, client_gid);

  if ((qpos = sinfo.find("?")) != STR_NPOS)
    sinfo.erase(qpos);

  info = (ininfo ? sinfo.c_str() : ininfo);
  xcastor_debug("path=%s, info=%s", path, (info ? info : ""));
  XrdOucEnv Open_Env(info);
  xcastor::Timing preparetiming("stageprepare");
  std::string map_path = NsMapping(path);

  if (map_path.empty())
  {
    error.setErrInfo(ENOMEDIUM, "No mapping for file name");
    return SFS_ERROR;
  }

  struct Cns_filestatcs cstat;

  if (XrdxCastor2FsUFS::Statfn(map_path.c_str(), &cstat))
    return XrdxCastor2Fs::Emsg(epname, error, serrno, "stat", map_path.c_str());

  char* val;
  std::string desired_svc = "default";

  if ((val = Open_Env.Get("svcClass")))
    desired_svc = val;

  std::string allowed_svc = GetAllowedSvc(map_path.c_str(), desired_svc);

  if (allowed_svc.empty())
    return XrdxCastor2Fs::Emsg(epname, error, EINVAL, "stageprepare - cannot find any"
                               " valid service class mapping for fn = ", map_path.c_str());

  // Get the allowed service class, preference for the default one
  TIMING("STAGERQUERY", &preparetiming);
  struct XrdxCastor2Stager::RespInfo resp_info;
  int retc = (XrdxCastor2Stager::Prepare2Get(error, (uid_t) client_uid,
              (gid_t) client_gid, map_path.c_str(),
              allowed_svc.c_str(), resp_info)
              ? SFS_OK : SFS_ERROR);
  TIMING("END", &preparetiming);

  if (gMgr->mLogLevel == LOG_DEBUG)
    preparetiming.Print();

  return retc;
}


//------------------------------------------------------------------------------
// Prepare
//------------------------------------------------------------------------------
int
XrdxCastor2Fs::prepare(XrdSfsPrep&         pargs,
                       XrdOucErrInfo&      error,
                       const XrdSecEntity* client)
{
  static const char* epname = "prepare";
  const char* tident = error.getErrUser();
  xcastor::Timing preparetiming("prepare");
  TIMING("START", &preparetiming);
  // The security mechanism here is to allow only hosts which are defined by fsdisk
  std::string stident = tident;
  std::string hostname = stident.substr(stident.rfind('@') + 1);
  // Set client identity
  SetIdentity(client);

  if (gMgr->Proc)
    gMgr->Stats.IncCmd();

  xcastor_debug("checking tident=%s, hostname=%s", tident, hostname.c_str());

  if (!FindDiskServer(hostname.c_str()))
  {
    // This is not a trusted disk server, assume the 'traditional' xrootd prepare
    XrdOucTList* tp = pargs.paths;
    XrdOucTList* op = pargs.oinfo;

    // Run through the paths to make sure client can read each one
    while (tp)
    {
      int retc = stageprepare(tp->text, error, client, (op ? op->text : NULL));

      if (retc != SFS_OK)
        return retc;

      if (tp)
      {
        tp = tp->next;

        if (op) op = op->next;
      }
      else
        break;
    }

    return SFS_OK;
  }

  if (!pargs.paths->text || !pargs.oinfo->text)
  {
    xcastor_debug("ignoring prepare request");
    return SFS_OK;
  }

  xcastor_debug("updating opaque=%s", pargs.oinfo->text);
  XrdOucEnv oenv(pargs.oinfo->text);
  const char* path = pargs.paths->text;
  std::string map_path = NsMapping(path);
  TIMING("CNSID", &preparetiming);

  // Unlink files which are not properly closed on a disk server
  if (oenv.Get("unlink"))
  {
    char cds[4096];
    char nds[4096];
    char* acpath;
    TIMING("CNSSELSERV", &preparetiming);
    Cns_selectsrvr(map_path.c_str(), cds, nds, &acpath);
    xcastor_debug("nameserver is: %s", nds);
    TIMING("CNSUNLINK", &preparetiming);

    if ((!oenv.Get("uid")) || (!oenv.Get("gid")))
    {
      return XrdxCastor2Fs::Emsg(epname, error, EINVAL, "missing sec uid/gid", map_path.c_str());
    }

    xcastor_debug("running rm as uid:%i, gid:%i", oenv.Get("uid"), oenv.Get("gid"));
    // Do the unlink as the authorized user in the open
    XrdxCastor2FsUFS::SetId(atoi(oenv.Get("uid")), atoi(oenv.Get("gid")));

    if (XrdxCastor2FsUFS::Unlink(map_path.c_str()))
      return XrdxCastor2Fs::Emsg(epname, error, serrno, "unlink", map_path.c_str());
  }

  // This is currently needed
  if (oenv.Get("firstbytewritten"))
  {
    char cds[4096];
    char nds[4096];
    char* acpath;
    TIMING("CNSSELSERV", &preparetiming);
    Cns_selectsrvr(map_path.c_str(), cds, nds, &acpath);
    xcastor_debug("nameserver is: %s", nds);
    struct Cns_filestatcs cstat;
    TIMING("CNSSTAT", &preparetiming);

    if (XrdxCastor2FsUFS::Statfn(map_path.c_str(), &cstat))
      return XrdxCastor2Fs::Emsg(epname, error, serrno, "stat", map_path.c_str());

    XrdxCastor2FsUFS::SetId(geteuid(), getegid());
    char fileid[4096];
    sprintf(fileid, "%llu", cstat.fileid);
    TIMING("1STBYTEW", &preparetiming);

    if (!XrdxCastor2Stager::FirstWrite(error, map_path.c_str(), oenv.Get("reqid"),
                                       fileid, nds))
    {
      TIMING("END", &preparetiming);

      if (gMgr->mLogLevel == LOG_DEBUG)
        preparetiming.Print();

      return SFS_ERROR;
    }
  }

  TIMING("END", &preparetiming);

  if (gMgr->mLogLevel == LOG_DEBUG)
    preparetiming.Print();

  return SFS_OK;
}


//------------------------------------------------------------------------------
// Delete file - stager_rm
//------------------------------------------------------------------------------
int
XrdxCastor2Fs::rem(const char*         path,
                   XrdOucErrInfo&      error,
                   const XrdSecEntity* client,
                   const char*         info)
{
  static const char* epname = "rem";
  xcastor::Timing rmtiming("fileremove");
  TIMING("START", &rmtiming);
  XrdOucEnv env(info);
  AUTHORIZE(client, &env, AOP_Delete, "remove", path, error)
  std::string map_path = NsMapping(path);
  xcastor_debug("path=%s, map_path=%s", path, map_path.c_str());

  if (map_path.empty())
  {
    error.setErrInfo(ENOMEDIUM, "No mapping for file name");
    return SFS_ERROR;
  }

  // Set client identity
  uid_t client_uid;
  gid_t client_gid;
  GetIdMapping(client, client_uid, client_gid);

  if (gMgr->Proc)
    gMgr->Stats.IncRm();

  TIMING("Authorize", &rmtiming);

  if (env.Get("stagerm"))
  {
    char* val;
    std::string desired_svc = "";

    if ((val = env.Get("svcClass")))
      desired_svc = val;

    std::string allowed_svc = GetAllowedSvc(map_path.c_str(), desired_svc);

    if (allowed_svc.empty())
    {
      return XrdxCastor2Fs::Emsg(epname, error, EINVAL, "rem - cannot find a valid service "
                                 "class mapping for fn = ", map_path.c_str());
    }

    // Here we have the allowed stager/service class setting to issue the stage_rm request
    TIMING("STAGERRM", &rmtiming);

    if (!XrdxCastor2Stager::Rm(error, (uid_t) client_uid, (gid_t) client_gid,
                               map_path.c_str(), allowed_svc.c_str()))
    {
      TIMING("END", &rmtiming);

      if (gMgr->mLogLevel == LOG_DEBUG)
        rmtiming.Print();

      return SFS_ERROR;
    }

    // The stage_rm worked, proceed with the namespace deletion
  }

  int retc = 0;
  retc = _rem(map_path.c_str(), error, info);
  TIMING("RemoveNamespace", &rmtiming);

  if (gMgr->mLogLevel == LOG_DEBUG)
    rmtiming.Print();

  return retc;
}


//------------------------------------------------------------------------------
// Delete a file from the namespace.
//------------------------------------------------------------------------------
int
XrdxCastor2Fs::_rem(const char*         path,
                    XrdOucErrInfo&      error,
                    const char*         /*info*/)
{
  static const char* epname = "rem";
  xcastor_debug("path=%s", path);

  // Perform the actual deletion
  if (XrdxCastor2FsUFS::Unlink(path))
    return XrdxCastor2Fs::Emsg(epname, error, serrno, "remove", path);

  return SFS_OK;
}


//------------------------------------------------------------------------------
// Delete a directory
//------------------------------------------------------------------------------
int
XrdxCastor2Fs::remdir(const char* path,
                      XrdOucErrInfo& error,
                      const XrdSecEntity* client,
                      const char* info)
{
  static const char* epname = "remdir";
  XrdOucEnv remdir_Env(info);
  xcastor_debug("path=%s", path);
  AUTHORIZE(client, &remdir_Env, AOP_Delete, "remove", path, error)
  std::string map_path = NsMapping(path);

  if (map_path.empty())
  {
    error.setErrInfo(ENOMEDIUM, "No mapping for file name");
    return SFS_ERROR;
  }

  if (gMgr->Proc)
    gMgr->Stats.IncCmd();

  return _remdir(map_path.c_str(), error, client, info);
}


//------------------------------------------------------------------------------
// Delete a directory
//------------------------------------------------------------------------------
int
XrdxCastor2Fs::_remdir(const char* path,
                       XrdOucErrInfo& error,
                       const XrdSecEntity* client,
                       const char* /*info*/)
{
  static const char* epname = "remdir";
  SetIdentity(client);

  // Perform the actual deletion
  if (XrdxCastor2FsUFS::Remdir(path))
    return XrdxCastor2Fs::Emsg(epname, error, serrno, "remove", path);

  return SFS_OK;
}


//------------------------------------------------------------------------------
// Rename
//------------------------------------------------------------------------------
int
XrdxCastor2Fs::rename(const char* old_name,
                      const char* new_name,
                      XrdOucErrInfo& error,
                      const XrdSecEntity* client,
                      const char* infoO,
                      const char* infoN)

{
  static const char* epname = "rename";
  XrdOucString source, destination;
  XrdOucEnv renameo_Env(infoO);
  XrdOucEnv renamen_Env(infoN);
  AUTHORIZE(client, &renameo_Env, AOP_Update, "rename", old_name, error)
  AUTHORIZE(client, &renamen_Env, AOP_Update, "rename", new_name, error)
  std::string oldn = NsMapping(old_name);

  if (oldn == "")
  {
    error.setErrInfo(ENOMEDIUM, "No mapping for file name");
    return SFS_ERROR;
  }

  std::string newn = NsMapping(new_name);

  if (newn == "")
  {
    error.setErrInfo(ENOMEDIUM, "No mapping for file name");
    return SFS_ERROR;
  }

  if (gMgr->Proc)
    gMgr->Stats.IncCmd();

  // Check if dest is existing
  XrdSfsFileExistence file_exists;

  if (!_exists(newn.c_str(), file_exists, error, client, infoN))
  {
    if (file_exists == XrdSfsFileExistIsDirectory)
    {
      // We have to path the destination name since the target is a directory
      std::string sourcebase = oldn;
      size_t npos = oldn.rfind('/');

      if (npos == std::string::npos)
        return XrdxCastor2Fs::Emsg(epname, error, EINVAL, "rename", oldn.c_str());

      sourcebase = oldn.substr(0, npos);
      // Use XrdOucString for the replace functionality
      XrdOucString tmp_str = newn.c_str();
      tmp_str += "/";
      tmp_str += sourcebase.c_str();

      while (tmp_str.replace("//", "/")) {};

      newn = tmp_str.c_str();
    }

    if (file_exists == XrdSfsFileExistIsFile)
    {
      // Remove the target file first!
      int remrc = _rem(newn.c_str(), error, infoN);

      if (remrc)
        return remrc;
    }
  }

  if (XrdxCastor2FsUFS::Rename(oldn.c_str(), newn.c_str()))
    return XrdxCastor2Fs::Emsg(epname, error, serrno, "rename", oldn.c_str());

  xcastor_debug("namespace rename done: %s => %s", oldn.c_str(), newn.c_str());
  return SFS_OK;
}


//------------------------------------------------------------------------------
// Stat - get all information
//------------------------------------------------------------------------------
int
XrdxCastor2Fs::stat(const char* path,
                    struct stat* buf,
                    XrdOucErrInfo& error,
                    const XrdSecEntity* client,
                    const char* info)

{
  static const char* epname = "stat";
  xcastor::Timing stattiming("filestat");
  XrdOucEnv Open_Env(info);
  XrdOucString stage_status = "";
  TIMING("START", &stattiming);
  xcastor_debug("path=%s", path);
  AUTHORIZE(client, &Open_Env, AOP_Stat, "stat", path, error)
  std::string map_path = NsMapping(path);

  if (map_path.empty())
  {
    error.setErrInfo(ENOMEDIUM, "No mapping for file name");
    return SFS_ERROR;
  }

  // Set client identity
  uid_t client_uid;
  gid_t client_gid;
  GetIdMapping(client, client_uid, client_gid);
  TIMING("CNSSTAT", &stattiming);
  struct Cns_filestatcs cstat;

  if (XrdxCastor2FsUFS::Statfn(map_path.c_str(), &cstat))
    return XrdxCastor2Fs::Emsg(epname, error, serrno, "stat", map_path.c_str());

  if (!(Open_Env.Get("nostagerquery")))
  {
    if (!S_ISDIR(cstat.filemode))
    {
      // Loop over all allowed service classes to find an online one
      std::string desired_svc = "default";
      std::set<std::string> set_svc = GetAllAllowedSvc(map_path.c_str());

      for (std::set<std::string>::iterator allowed_svc = set_svc.begin();
           allowed_svc != set_svc.end(); ++allowed_svc)
      {
        if (!XrdxCastor2Stager::StagerQuery(error, (uid_t) client_uid, (gid_t) client_gid,
                                            map_path.c_str(), allowed_svc->c_str(), stage_status))
        {
          stage_status = "NA";
        }

        if ((stage_status == "STAGED") ||
            (stage_status == "CANBEMIGR") ||
            (stage_status == "STAGEOUT"))
        {
          break;
        }
      }
    }
  }

  if (gMgr->Proc)
    gMgr->Stats.IncStat();

  memset(buf, sizeof(struct stat), 0);
  buf->st_dev     = 0xcaff;
  buf->st_ino     = cstat.fileid;
  buf->st_mode    = cstat.filemode;
  buf->st_nlink   = cstat.nlink;
  buf->st_uid     = cstat.uid;
  buf->st_gid     = cstat.gid;
  buf->st_rdev    = 0;     /* device type (if inode device) */
  buf->st_size    = cstat.filesize;
  buf->st_blksize = 4096;
  buf->st_blocks  = cstat.filesize / 4096;
  buf->st_atime   = cstat.atime;
  buf->st_mtime   = cstat.mtime;
  buf->st_ctime   = cstat.ctime;

  if (!S_ISDIR(cstat.filemode))
  {
    if ((stage_status != "STAGED") &&
        (stage_status != "CANBEMIGR") &&
        (stage_status != "STAGEOUT"))
    {
      // This file is offline
      buf->st_mode = (mode_t) - 1;
      buf->st_dev   = 0;
    }

    xcastor_debug("map_path=%s, stage_status=%s", map_path.c_str(), stage_status.c_str());
  }

  TIMING("END", &stattiming);

  if (gMgr->mLogLevel == LOG_DEBUG)
    stattiming.Print();

  return SFS_OK;
}


//--------------------------------------------------------------------------
// Stat - get mode information
//--------------------------------------------------------------------------
int
XrdxCastor2Fs::stat(const char* Name,
                    mode_t& mode,
                    XrdOucErrInfo& out_error,
                    const XrdSecEntity* client,
                    const char* opaque)
{
  struct stat buf;
  int rc = stat(Name, &buf, out_error, client, opaque);

  if (!rc) mode = buf.st_mode;

  return rc;
}


//------------------------------------------------------------------------------
// Lstat
//------------------------------------------------------------------------------
int
XrdxCastor2Fs::lstat(const char* path,
                     struct stat* buf,
                     XrdOucErrInfo& error,
                     const XrdSecEntity* client,
                     const char* info)
{
  static const char* epname = "lstat";
  XrdOucEnv lstat_Env(info);
  xcastor::Timing stattiming("filelstat");
  TIMING("START", &stattiming);
  xcastor_debug("path=%s", path);
  AUTHORIZE(client, &lstat_Env, AOP_Stat, "lstat", path, error)
  std::string map_path = NsMapping(path);

  if (map_path.empty())
  {
    error.setErrInfo(ENOMEDIUM, "No mapping for file name");
    return SFS_ERROR;
  }

  if (!strcmp(map_path.c_str(), "/"))
  {
    buf->st_dev     = 0xcaff;
    buf->st_ino     = 0;
    buf->st_mode    = S_IFDIR | S_IRUSR | S_IRGRP | S_IROTH | S_IXUSR | S_IXGRP | S_IXOTH ;
    buf->st_nlink   = 1;
    buf->st_uid     = 0;
    buf->st_gid     = 0;
    buf->st_rdev    = 0;     /* device type (if inode device) */
    buf->st_size    = 0;
    buf->st_blksize = 4096;
    buf->st_blocks  = 0;
    buf->st_atime   = 0;
    buf->st_mtime   = 0;
    buf->st_ctime   = 0;
    return SFS_OK;
  }

  // Set client identity
  SetIdentity(client);
  struct Cns_filestat cstat;
  TIMING("CNSLSTAT", &stattiming);

  if (XrdxCastor2FsUFS::Lstatfn(map_path.c_str(), &cstat))
    return XrdxCastor2Fs::Emsg(epname, error, serrno, "lstat", map_path.c_str());

  if (gMgr->Proc)
    gMgr->Stats.IncStat();

  memset(buf, sizeof(struct stat), 0);
  buf->st_dev     = 0xcaff;
  buf->st_ino     = cstat.fileid;
  buf->st_mode    = cstat.filemode;
  buf->st_nlink   = cstat.nlink;
  buf->st_uid     = cstat.uid;
  buf->st_gid     = cstat.gid;
  buf->st_rdev    = 0;     /* device type (if inode device) */
  buf->st_size    = cstat.filesize;
  buf->st_blksize = 4096;
  buf->st_blocks  = cstat.filesize / 4096;
  buf->st_atime   = cstat.atime;
  buf->st_mtime   = cstat.mtime;
  buf->st_ctime   = cstat.ctime;
  TIMING("END", &stattiming);

  if (gMgr->mLogLevel == LOG_DEBUG)
    stattiming.Print();

  return SFS_OK;
}


//------------------------------------------------------------------------------
// Truncate file
//------------------------------------------------------------------------------
int
XrdxCastor2Fs::truncate(const char*,
                        XrdSfsFileOffset,
                        XrdOucErrInfo&,
                        const XrdSecEntity*,
                        const char*)
{
  xcastor_debug("not supported");
  return SFS_ERROR;
}


//------------------------------------------------------------------------------
// Read link
//------------------------------------------------------------------------------
int
XrdxCastor2Fs::readlink(const char* path,
                        XrdOucString& linkpath,
                        XrdOucErrInfo& error,
                        const XrdSecEntity* client,
                        const char* info)
{
  static const char* epname = "readlink";
  XrdOucEnv rl_Env(info);
  xcastor_debug("path=%s", path);
  AUTHORIZE(client, &rl_Env, AOP_Stat, "readlink", path, error)
  std::string map_path = NsMapping(path);

  if (map_path.empty())
  {
    error.setErrInfo(ENOMEDIUM, "No mapping for file name");
    return SFS_ERROR;
  }

  // Set client identity
  SetIdentity(client);
  char lp[4097];
  int nlen;

  if ((nlen = XrdxCastor2FsUFS::Readlink(map_path.c_str(), lp, 4096)) == -1)
    return XrdxCastor2Fs::Emsg(epname, error, serrno, "readlink", map_path.c_str());

  lp[nlen] = 0;
  linkpath = lp;
  return SFS_OK;
}


//------------------------------------------------------------------------------
// Symbolic link
//------------------------------------------------------------------------------
int
XrdxCastor2Fs::symlink(const char* path,
                       const char* linkpath,
                       XrdOucErrInfo& error,
                       const XrdSecEntity* client,
                       const char* info)
{
  static const char* epname = "symlink";
  XrdOucEnv sl_Env(info);
  xcastor_debug("path=%s", path);
  AUTHORIZE(client, &sl_Env, AOP_Create, "symlink", linkpath, error)
  std::string source = path;
  uid_t client_uid;
  gid_t client_gid;
  GetIdMapping(client, client_uid, client_gid);

  if (path[0] == '/')
  {
    source = NsMapping(path);

    if (source == "")
    {
      error.setErrInfo(ENOMEDIUM, "No mapping for file name");
      return SFS_ERROR;
    }
  }

  std::string destination = NsMapping(linkpath);

  if (destination == "")
  {
    error.setErrInfo(ENOMEDIUM, "No mapping for file name");
    return SFS_ERROR;
  }

  if (XrdxCastor2FsUFS::Symlink(source.c_str(), destination.c_str()))
    return XrdxCastor2Fs::Emsg(epname, error, serrno, "symlink", source.c_str());

  SetAcl(destination.c_str(), client_uid, client_gid, 1);
  return SFS_OK;
}

//------------------------------------------------------------------------------
// Access
//------------------------------------------------------------------------------
int
XrdxCastor2Fs::access(const char* path,
                      int mode,
                      XrdOucErrInfo& error,
                      const XrdSecEntity* client,
                      const char* info)
{
  static const char* epname = "access";
  XrdOucEnv access_Env(info);
  xcastor_debug("path=%s", path);
  AUTHORIZE(client, &access_Env, AOP_Stat, "access", path, error)
  std::string map_path = NsMapping(path);

  if (map_path.empty())
  {
    error.setErrInfo(ENOMEDIUM, "No mapping for file name");
    return SFS_ERROR;
  }

  return SFS_OK;
}


//------------------------------------------------------------------------------
// Utimes
//------------------------------------------------------------------------------
int XrdxCastor2Fs::utimes(const char* path,
                          struct timeval* tvp,
                          XrdOucErrInfo& error,
                          const XrdSecEntity* client,
                          const char* info)
{
  static const char* epname = "utimes";
  XrdOucEnv utimes_Env(info);
  xcastor_debug("path=%s", path);
  AUTHORIZE(client, &utimes_Env, AOP_Update, "set utimes", path, error)
  std::string map_path = NsMapping(path);

  if (map_path.empty())
  {
    error.setErrInfo(ENOMEDIUM, "No mapping for file name");
    return SFS_ERROR;
  }

  // Set client identity
  SetIdentity(client);

  if ((XrdxCastor2FsUFS::Utimes(map_path.c_str(), tvp)))
    return XrdxCastor2Fs::Emsg(epname, error, serrno, "utimes", map_path.c_str());

  return SFS_OK;
}


//------------------------------------------------------------------------------
//! Emsg - format error messages
//------------------------------------------------------------------------------
int
XrdxCastor2Fs::Emsg(const char* pfx,
                    XrdOucErrInfo& einfo,
                    int ecode,
                    const char* op,
                    const char* target)
{
  char* etext, buffer[4096], unkbuff[64];

  // Get the reason for the error
  if (ecode < 0) ecode = -ecode;

  if (!(etext = strerror(ecode)))
  {
    sprintf(unkbuff, "reason unknown (%d)", ecode);
    etext = unkbuff;
  }

  // Format the error message
  snprintf(buffer, sizeof(buffer), "Unable to %s %s; %s", op, target, etext);
#ifndef NODEBUG
  OfsEroute.Emsg(pfx, buffer);
#endif
  // Place the error message in the error object and return
  einfo.setErrInfo(ecode, buffer);
  return SFS_ERROR;
}


//------------------------------------------------------------------------------
// Stall
//------------------------------------------------------------------------------
int XrdxCastor2Fs::Stall(XrdOucErrInfo& error,
                         int stime,
                         const char* msg)
{
  XrdOucString smessage = msg;
  smessage += "; come back in ";
  smessage += stime;
  smessage += " seconds!";
  xcastor_debug("Stall %i : %s", stime, smessage.c_str());
  // Place the error message in the error object and return
  error.setErrInfo(0, smessage.c_str());
  return stime;
}


//------------------------------------------------------------------------------
// Fsctl
//------------------------------------------------------------------------------
int
XrdxCastor2Fs::fsctl(const int cmd,
                     const char* args,
                     XrdOucErrInfo& error,
                     const XrdSecEntity* client)

{
  XrdOucString path = args;
  XrdOucString opaque;
  opaque.assign(path, path.find("?") + 1);
  path.erase(path.find("?"));
  XrdOucEnv env(opaque.c_str());
  const char* scmd;
  xcastor_debug("args=%s", args);

  if ((cmd == SFS_FSCTL_LOCATE) || (cmd == 16777217))
  {
    if (path.beginswith("*"))
      path.erase(0, 1);

    // Check if this file exists
    XrdSfsFileExistence file_exists;

    if ((_exists(path.c_str(), file_exists, error, 0)) ||
        (file_exists == XrdSfsFileExistNo))
    {
      return SFS_ERROR;
    }

    char locResp[4096];
    char rType[3], *Resp[] = {rType, locResp};
    rType[0] = 'S';
    // We don't want to manage writes via global redirection - therefore we mark the files as 'r'
    rType[1] = 'r';
    rType[2] = '\0';
    sprintf(locResp, "%s", (char*)gMgr->ManagerLocation.c_str());
    error.setErrInfo(strlen(locResp) + 3, (const char**)Resp, 2);
    xcastor_debug("located at headnode: %s", locResp);
    return SFS_DATA;
  }

  if (cmd != SFS_FSCTL_PLUGIN)
    return SFS_ERROR;

  if ((scmd = env.Get("pcmd")))
  {
    XrdOucString execmd = scmd;

    if (execmd == "stat")
    {
      struct stat buf;
      int retc = lstat(path.c_str(), &buf, error, client, 0);

      if (retc == SFS_OK)
      {
        char statinfo[16384];
        // Convert into a char stream
        sprintf(statinfo, "stat: %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu\n",
                (unsigned long long) buf.st_dev,
                (unsigned long long) buf.st_ino,
                (unsigned long long) buf.st_mode,
                (unsigned long long) buf.st_nlink,
                (unsigned long long) buf.st_uid,
                (unsigned long long) buf.st_gid,
                (unsigned long long) buf.st_rdev,
                (unsigned long long) buf.st_size,
                (unsigned long long) buf.st_blksize,
                (unsigned long long) buf.st_blocks,
                (unsigned long long) buf.st_atime,
                (unsigned long long) buf.st_mtime,
                (unsigned long long) buf.st_ctime);
        error.setErrInfo(strlen(statinfo) + 1, statinfo);
        return SFS_DATA;
      }
    }

    if (execmd == "chmod")
    {
      char* smode;

      if ((smode = env.Get("mode")))
      {
        XrdSfsMode newmode = atoi(smode);
        int retc = chmod(path.c_str(), newmode, error, client, 0);
        XrdOucString response = "chmod: retc=";
        response += retc;
        error.setErrInfo(response.length() + 1, response.c_str());
        return SFS_DATA;
      }
      else
      {
        XrdOucString response = "chmod: retc=";
        response += EINVAL;
        error.setErrInfo(response.length() + 1, response.c_str());
        return SFS_DATA;
      }
    }

    if (execmd == "symlink")
    {
      char* destination;
      char* source;

      if ((destination = env.Get("linkdest")))
      {
        if ((source = env.Get("linksource")))
        {
          int retc = symlink(source, destination, error, client, 0);
          XrdOucString response = "sysmlink: retc=";
          response += retc;
          error.setErrInfo(response.length() + 1, response.c_str());
          return SFS_DATA;
        }
      }

      XrdOucString response = "symlink: retc=";
      response += EINVAL;
      error.setErrInfo(response.length() + 1, response.c_str());
      return SFS_DATA;
    }

    if (execmd == "readlink")
    {
      XrdOucString linkpath = "";
      int retc = readlink(path.c_str(), linkpath, error, client, 0);
      XrdOucString response = "readlink: retc=";
      response += retc;
      response += " link=";
      response += linkpath;
      error.setErrInfo(response.length() + 1, response.c_str());
      return SFS_DATA;
    }

    if (execmd == "access")
    {
      char* smode;

      if ((smode = env.Get("mode")))
      {
        int newmode = atoi(smode);
        int retc = access(path.c_str(), newmode, error, client, 0);
        XrdOucString response = "access: retc=";
        response += retc;
        error.setErrInfo(response.length() + 1, response.c_str());
        return SFS_DATA;
      }
      else
      {
        XrdOucString response = "access: retc=";
        response += EINVAL;
        error.setErrInfo(response.length() + 1, response.c_str());
        return SFS_DATA;
      }
    }

    if (execmd == "utimes")
    {
      char* tv1_sec;
      char* tv1_usec;
      char* tv2_sec;
      char* tv2_usec;
      tv1_sec  = env.Get("tv1_sec");
      tv1_usec = env.Get("tv1_usec");
      tv2_sec  = env.Get("tv2_sec");
      tv2_usec = env.Get("tv2_usec");
      struct timeval tvp[2];

      if (tv1_sec && tv1_usec && tv2_sec && tv2_usec)
      {
        tvp[0].tv_sec  = strtol(tv1_sec, 0, 10);
        tvp[0].tv_usec = strtol(tv1_usec, 0, 10);
        tvp[1].tv_sec  = strtol(tv2_sec, 0, 10);
        tvp[1].tv_usec = strtol(tv2_usec, 0, 10);
        int retc = utimes(path.c_str(), tvp, error, client, 0);
        XrdOucString response = "utimes: retc=";
        response += retc;
        error.setErrInfo(response.length() + 1, response.c_str());
        return SFS_DATA;
      }
      else
      {
        XrdOucString response = "utimes: retc=";
        response += EINVAL;
        error.setErrInfo(response.length() + 1, response.c_str());
        return SFS_DATA;
      }
    }
  }

  return SFS_ERROR;
}


//------------------------------------------------------------------------------
// Get delay for the current operation if any. The delay value is read from
// the xcastor2.proc value specified in the /etc/xrd.cf file, from the
// corresponding delayread or delaywrite file.
//------------------------------------------------------------------------------
int64_t
XrdxCastor2Fs::GetAdminDelay(XrdOucString& msg, bool isRW)
{
  int64_t delay = 0;
  msg = "";

  if (isRW)
  {
    // Send a delay response to the client
    delay = xCastor2FsDelayWrite;

    if (delay > 0)
    {
      // We don't delay for more than 4 hours
      if (delay > (4 * 3600))
        delay = (4 * 3600);

      if (Proc)
      {
        XrdxCastor2ProcFile* pf = Proc->Handle("sysmsg");

        if (pf)
          pf->Read(msg);
      }

      if (msg == "")
        msg = "system is in maintenance mode for WRITE";
    }
    else
      delay = 0;
  }
  else
  {
    // Send a delay response to the client
    delay = xCastor2FsDelayRead;

    if (delay > 0)
    {
      // We don't delay for more than 4 hours
      if (delay > (4 * 3600))
        delay = (4 * 3600);

      if (Proc)
      {
        XrdxCastor2ProcFile* pf = Proc->Handle("sysmsg");

        if (pf)
          pf->Read(msg);
      }

      if (msg == "")
        msg = "system is in maintenance mode for READ";
    }
    else
      delay = 0;
  }

  return delay;
}


//------------------------------------------------------------------------------
//! Set the log level for the manager xrootd server
//------------------------------------------------------------------------------
void
XrdxCastor2Fs::SetLogLevel(int logLevel)
{
  if (mLogLevel != logLevel)
  {
    xcastor_notice("update log level from:%s to:%s",
                   Logging::GetPriorityString(mLogLevel),
                   Logging::GetPriorityString(logLevel));
    mLogLevel = logLevel;
    Logging::SetLogPriority(mLogLevel);
  }
}


//------------------------------------------------------------------------------
// Cache disk server host name with and without the domain name
//------------------------------------------------------------------------------
void
XrdxCastor2Fs::CacheDiskServer(const std::string& hostname)
{
  XrdSysMutexHelper scope_lock(mMutexFsSet);
  mFsSet.insert(hostname);
  size_t pos = hostname.find('.');

  // Insert the hostname without the domain
  if (pos != std::string::npos)
  {
    std::string nodomain_host = hostname.substr(0, pos - 1);
    mFsSet.insert(nodomain_host);
  }
}


//------------------------------------------------------------------------------
// Check if the hostname is known at the redirector
//------------------------------------------------------------------------------
bool
XrdxCastor2Fs::FindDiskServer(const std::string& hostname)
{
  XrdSysMutexHelper scope_lock(mMutexFsSet);
  return (mFsSet.find(hostname) != mFsSet.end());
}


//------------------------------------------------------------------------------
// Get uid/gid mapping for client object
//------------------------------------------------------------------------------
void
XrdxCastor2Fs::GetIdMapping(const XrdSecEntity* client, uid_t& uid, gid_t& gid)
{
  xcastor_debug("client_name=%s, client_role=%s, client_tident=%s",
                client->name, client->role, client->tident);
  uid = 99; // nobody
  gid = 99; // nobody
  // Find role of current client, it can either be his name or a mapping
  std::string role = "";
  struct passwd* pw = 0;

  if (client && client->name)
    role = client->name;

  std::map<std::string, std::string>::iterator iter = mRoleMap.find(role);

  if (iter == mRoleMap.end())
  {
    // No mapping found using client name , try wildcard "*"
    iter = mRoleMap.find("*");

    if (iter != mRoleMap.end())
      role = iter->second;
  }
  else
    role = iter->second;

  // Find role to id mapping
  {
    XrdSysMutexHelper scope_lock(mMutexPasswd);
    pw = mPasswdStore->Find(role.c_str());
  }

  if (pw)
  {
    // Found client to id mapping
    uid = pw->pw_uid;
    gid = pw->pw_gid;
  }
  else
  {
    // Get uid/gid for current role
    pw = getpwnam(role.c_str());

    if (pw)
    {
      struct passwd* pwdcpy = (struct passwd*) malloc(sizeof(struct passwd));
      pwdcpy = (struct passwd*)memcpy(pwdcpy, pw, sizeof(struct passwd));
      uid = pwdcpy->pw_uid;
      gid = pwdcpy->pw_gid;
      xcastor_debug("passwd add mapping role=%s -> uid/gid=%i/%i",
                    role.c_str(), (int)uid, int(gid));
      XrdSysMutexHelper scope_lock(mMutexPasswd);
      mPasswdStore->Add(role.c_str(), pwdcpy, 60);
    }
    else
    {
      xcastor_warning("no passwd struct found for role=%s", role.c_str());
    }
  }

  XrdxCastor2FsUFS::SetId(uid, gid);
  xcastor_debug("uid/gid=%i/%i", (int)uid, int(gid));
}


//------------------------------------------------------------------------------
// Set the uid/gid mapping by calling Cns_SetId with the values obtained
// after doing the mapping of the XrdSecEntity object to uid/gid. This is
// a convenience method when we are do not want to return the uid/gid pair
// the the calling function
//------------------------------------------------------------------------------
void
XrdxCastor2Fs::SetIdentity(const XrdSecEntity* client)
{
  uid_t uid;
  gid_t gid;
  GetIdMapping(client, uid, gid);
}

