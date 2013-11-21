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
 * @author Elvin Sindrilaru & Andreas Peters - CERN
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
XrdOucHash<XrdOucString>* XrdxCastor2Fs::nsMap;
XrdOucHash<XrdOucString>* XrdxCastor2Fs::stagertable;
XrdOucHash<XrdOucString>* XrdxCastor2Fs::roletable;
XrdOucHash<XrdOucString>* XrdxCastor2Fs::stringstore;
XrdOucHash<XrdSecEntity>* XrdxCastor2Fs::secentitystore;
XrdOucHash<XrdOucString>* XrdxCastor2Fs::gridmapstore;
XrdOucHash<struct passwd>* XrdxCastor2Fs::passwdstore;
XrdOucHash<XrdxCastor2FsGroupInfo>*  XrdxCastor2Fs::groupinfocache;
XrdOucHash<XrdOucString>* XrdxCastor2Stager::msDelayStore;
xcastor::XrdxCastorClient* XrdxCastor2Fs::msCastorClient;

int XrdxCastor2Fs::TokenLockTime = 5;
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
  OfsEroute.Say("++++++ (c) 2008 CERN/IT-DM-SMD ",
                "xCastor2Fs (xCastor2 File System) v 1.0 ");

  // Initialize the subsystems
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
// This helps to avoid memory leaks by strdup, we maintain a string hash to
// keep all used user ids/group ids etc.
//------------------------------------------------------------------------------
char*
STRINGSTORE(const char* __charptr__)
{
  XrdOucString* yourstring;

  if (!__charptr__) return "";

  gMgr->StoreMutex.Lock();

  if ((yourstring = gMgr->stringstore->Find(__charptr__)))
  {
    gMgr->StoreMutex.UnLock();
    return (char*)yourstring->c_str();
  }
  else
  {
    XrdOucString* newstring = new XrdOucString(__charptr__);
    gMgr->stringstore->Add(__charptr__, newstring);
    gMgr->StoreMutex.UnLock();
    return (char*)newstring->c_str();
  }
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
// Get uid and gid for current path and client
//------------------------------------------------------------------------------
void
XrdxCastor2Fs::GetId(const char* path, XrdSecEntity client, uid_t& uid, gid_t& gid)
{
  MapMutex.Lock();
  uid = 99;
  gid = 99;
  struct passwd* pw = NULL;
  XrdxCastor2FsGroupInfo* ginfo = NULL;

  if (client.name)
  {
    if ((ginfo = groupinfocache->Find(client.name)))
    {
      pw = &(ginfo->Passwd);

      if (pw) uid = pw->pw_uid;

      if (pw) gid = pw->pw_gid;
    }
    else
    {
      if (!(pw = passwdstore->Find(client.name)))
      {
        pw = getpwnam(client.name);

        if (pw)
        {
          struct passwd* pwdcpy = (struct passwd*) malloc(sizeof(struct passwd));
          memcpy(pwdcpy, pw, sizeof(struct passwd));
          pw = pwdcpy;
          passwdstore->Add(client.name, pwdcpy, 60);
        }
      }

      if (pw)
      {
        uid = pw->pw_uid;
        gid = pw->pw_gid;
      }
    }
  }

  if (client.role)
  {
    char buffer[65536];
    struct group* gr = 0;
    struct group group;
    getgrnam_r(client.role, &group, buffer, 65536, &gr);

    if (gr) gid = gr->gr_gid;
  }
  else
  {
    if (pw) gid = pw->pw_gid;
  }

  if (uid == 0)
  {
    gid = 0;
  }

  XrdOucString tracestring = "getgid ";
  tracestring += (int) uid;
  tracestring += "/";
  tracestring += (int) gid;
  xcastor_debug("file=%s, tracestring=%s", path, tracestring.c_str());
  MapMutex.UnLock();
}


//------------------------------------------------------------------------------
// Set the ACL for a file
//------------------------------------------------------------------------------
void
XrdxCastor2Fs::SetAcl(const char* path, XrdSecEntity client, bool isLink)
{
  MapMutex.Lock();  // -->
  uid_t uid = 99;
  uid_t gid = 0;
  struct passwd* pw = NULL;
  XrdxCastor2FsGroupInfo* ginfo = NULL;

  if (client.name)
  {
    if ((ginfo = groupinfocache->Find(client.name)))
    {
      pw = &(ginfo->Passwd);

      if (pw) uid = pw->pw_uid;
    }
    else
    {
      if (!(pw = passwdstore->Find(client.name)))
      {
        pw = getpwnam(client.name);
        struct passwd* pwdcpy = (struct passwd*) malloc(sizeof(struct passwd));
        memcpy(pwdcpy, pw, sizeof(struct passwd));
        pw = pwdcpy;
        passwdstore->Add(client.name, pwdcpy, 60);
      }

      if (pw) uid = pw->pw_uid;
    }
  }

  if (client.role)
  {
    char buffer[65536];
    struct group* gr = 0;
    struct group group;
    getgrnam_r(client.role, &group, buffer, 65536, &gr);

    if (gr) gid = gr->gr_gid;
  }
  else
  {
    if (pw) gid = pw->pw_gid;
  }

  XrdOucString tracestring = "setacl ";
  tracestring += (int)uid;
  tracestring += "/";
  tracestring += (int)gid;
  xcastor_debug("file=%s, tracestring=%s", path, tracestring.c_str());

  if (isLink)
  {
    if (XrdxCastor2FsUFS::Lchown(path , uid, gid))
    {
      xcastor_debug("file=%s, error:chown failed");
    }
  }
  else
  {
    if (XrdxCastor2FsUFS::Chown(path, uid, gid))
    {
      xcastor_debug("file=%s, error:chown failed");
    }
  }

  MapMutex.UnLock(); // <--
}


//------------------------------------------------------------------------------
// Get all groups for a user name
//------------------------------------------------------------------------------
void
XrdxCastor2Fs::GetAllGroups(const char*   name,
                            XrdOucString& allGroups,
                            XrdOucString& defaultGroup)
{
  allGroups = ":";
  defaultGroup = "";
  XrdxCastor2FsGroupInfo* ginfo = NULL;

  if ((ginfo = groupinfocache->Find(name)))
  {
    allGroups = ginfo->AllGroups;
    defaultGroup = ginfo->DefaultGroup;
    return;
  }

  struct group* gr;

  struct passwd* passwdinfo = NULL;

  if (!(passwdinfo = XrdxCastor2Fs::passwdstore->Find(name)))
  {
    passwdinfo = getpwnam(name);

    if (passwdinfo)
    {
      struct passwd* pwdcpy = (struct passwd*) malloc(sizeof(struct passwd));
      memcpy(pwdcpy, passwdinfo, sizeof(struct passwd));
      passwdinfo = pwdcpy;
      XrdxCastor2Fs::passwdstore->Add(name, pwdcpy, 60);
    }
    else 
    {
      xcastor_debug("passwdinfo is NULL");
      return;
    }
  }

  setgrent();
  char buf[65536];
  struct group grp;

  while ((!getgrent_r(&grp, buf, 65536, &gr)))
  {
    int cnt;
    cnt = 0;

    if (gr->gr_gid == passwdinfo->pw_gid)
    {
      if (!defaultGroup.length()) defaultGroup += gr->gr_name;

      allGroups += gr->gr_name;
      allGroups += ":";
    }

    while (gr->gr_mem[cnt])
    {
      if (!strcmp(gr->gr_mem[cnt], name))
      {
        allGroups += gr->gr_name;
        allGroups += ":";
      }

      cnt++;
    }
  }

  endgrent();
  ginfo = new XrdxCastor2FsGroupInfo(defaultGroup.c_str(), allGroups.c_str(), passwdinfo);
  groupinfocache->Add(name, ginfo, ginfo->Lifetime);
}


//------------------------------------------------------------------------------
// Map client to role
//------------------------------------------------------------------------------
void
XrdxCastor2Fs::RoleMap(const XrdSecEntity* client,
                       const char*         env,
                       XrdSecEntity&       mappedClient,
                       const char*         tident)
{
  XrdSecEntity* entity = NULL;
  int len_env;
  XrdOucEnv lenv(env);
  const char* role = lenv.Get("role");
  SecEntityMutex.Lock();  // -->
  char clientid[1024];
  sprintf(clientid, "%s:%llu", tident, (unsigned long long) client);
  xcastor_debug("tident=%s env=%s", tident, lenv.Env(len_env));

  if ((!role) && (entity = secentitystore->Find(clientid)))
  {
    // Find existing client rolemaps ....
    mappedClient.name = entity->name;
    mappedClient.role = entity->role;
    mappedClient.host = entity->host;
    mappedClient.vorg = entity->vorg;
    mappedClient.grps = entity->grps;
    mappedClient.endorsements = entity->endorsements;
    mappedClient.tident = entity->endorsements;
    uid_t _client_uid;
    gid_t _client_gid;
    GetId("-", mappedClient, _client_uid, _client_gid);
    XrdxCastor2FsUFS::SetId(_client_uid, _client_gid);
    SecEntityMutex.UnLock();  // <--
    return;
  }

  do
  {
    // (re-)create client rolemaps
    MapMutex.Lock();  // -->
    mappedClient.name = "__noauth__";
    mappedClient.role = "__noauth__";
    mappedClient.host = "";
    mappedClient.vorg = "";
    mappedClient.role = "";
    mappedClient.grps = "__noauth__";
    mappedClient.endorsements = "";
    mappedClient.tident = "";

    if (client)
    {
      if (client->prot) strcpy(mappedClient.prot, client->prot);
      if (client->name) mappedClient.name = STRINGSTORE(client->name);
      if (client->host) mappedClient.host = STRINGSTORE(client->host);
      if (client->vorg) mappedClient.vorg = STRINGSTORE(client->vorg);
      if (client->role) mappedClient.role = STRINGSTORE(client->role);
      if (client->grps) mappedClient.grps = STRINGSTORE(client->grps);
      if (client->endorsements) mappedClient.endorsements = STRINGSTORE(client->endorsements);
      if (client->tident) mappedClient.tident = STRINGSTORE(client->tident);

      // Static user mapping
      XrdOucString* hisroles;

      if ((hisroles = roletable->Find("*")))
      {
        XrdOucString allgroups;
        XrdOucString defaultgroup;
        XrdOucString FixedName;

        if (hisroles->beginswith("static:"))
        {
          FixedName = hisroles->c_str() + 7;
        }
        else
        {
          FixedName = hisroles->c_str();
        }

        while ((FixedName.find(":")) != STR_NPOS)
        {
          FixedName.replace(":", "");
        }

        mappedClient.name = STRINGSTORE(FixedName.c_str());
        GetAllGroups(mappedClient.name, allgroups, defaultgroup);
        mappedClient.grps = STRINGSTORE(defaultgroup.c_str());
        mappedClient.role = STRINGSTORE(defaultgroup.c_str());
        break;
      }

      if ((client->prot) && ((!strcmp(client->prot, "gsi")) ||
                             (!strcmp(client->prot, "ssl"))))
      {
        XrdOucString certsubject = client->name;

        if ((MapCernCertificates) &&
            (certsubject.beginswith("/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=")))
        {
          certsubject.erasefromstart(strlen("/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN="));
          int pos = certsubject.find('/');

          if (pos != STR_NPOS)
            certsubject.erase(pos);

          mappedClient.name = STRINGSTORE(certsubject.c_str());
        }

        certsubject.replace("/CN=proxy", "");
        // Leave only the first CN=, cut the rest
        int pos = certsubject.find("CN=");
        int pos2 = certsubject.find("/", pos);

        if (pos2 > 0) certsubject.erase(pos2);

        XrdOucString* gridmaprole;

        if (GridMapFile.length())
        {
          ReloadGridMapFile();
        }

        GridMapMutex.Lock();    // -->

        if ((gridmaprole = gridmapstore->Find(certsubject.c_str())))
        {
          mappedClient.name = STRINGSTORE(gridmaprole->c_str());
          mappedClient.role = 0;
        }

        GridMapMutex.UnLock();  // <--
      }
    }

    if (client && mappedClient.name)
    {
      XrdOucString defaultgroup = "";
      XrdOucString allgroups = "";

      if (client->role)
      {
        defaultgroup = client->role;
      }

      if ((client->grps == 0) || (!strlen(client->grps)))
      {
        GetAllGroups(mappedClient.name, allgroups, defaultgroup);
        mappedClient.grps = STRINGSTORE(allgroups.c_str());
      }

      if (((! mappedClient.role) || (!strlen(mappedClient.role))) && (!role))
      {
        role = STRINGSTORE(defaultgroup.c_str());
      }

      XrdOucString* hisroles = roletable->Find(mappedClient.name);
      XrdOucString match = ":";
      match += role;
      match += ":";
      xcastor_debug("default %s all %s match %s role %sn", defaultgroup.c_str(),
                    allgroups.c_str(), match.c_str(), role);

      if (hisroles)
        if ((hisroles->find(match.c_str())) != STR_NPOS)
        {
          mappedClient.role = STRINGSTORE(role);
        }
        else
        {
          if (hisroles->beginswith("static:"))
          {
            mappedClient.role = STRINGSTORE(hisroles->c_str() + 7);
          }
          else
          {
            if ((allgroups.find(match.c_str())) != STR_NPOS)
              mappedClient.role = STRINGSTORE(role);
            else
              mappedClient.role = STRINGSTORE(defaultgroup.c_str());
          }
        }
      else
      {
        if ((allgroups.find(match.c_str())) != STR_NPOS)
        {
          mappedClient.role = STRINGSTORE(role);
        }
        else
        {
          mappedClient.role = STRINGSTORE(defaultgroup.c_str());
        }
      }

      if ((!strlen(mappedClient.role)) && (client->grps))
        mappedClient.role = STRINGSTORE(client->grps);
    }

    {
      // Try tident mapping
      XrdOucString reducedTident, user, stident;
      reducedTident = "";
      user = "";
      stident = tident;
      int dotpos = stident.find(".");
      reducedTident.assign(tident, 0, dotpos - 1);
      reducedTident += "@";
      int adpos  = stident.find("@");
      user.assign(tident, adpos + 1);
      reducedTident += user;
      XrdOucString* hisroles = roletable->Find(reducedTident.c_str());
      XrdOucString match = ":";
      match += role;
      match += ":";

      if (hisroles)
        if ((hisroles->find(match.c_str())) != STR_NPOS)
        {
          mappedClient.role = STRINGSTORE(role);
          mappedClient.name = STRINGSTORE(role);
        }
        else
        {
          if (hisroles->beginswith("static:"))
          {
            if (mappedClient.role)(mappedClient.role);

            mappedClient.role = STRINGSTORE(hisroles->c_str() + 7);
            mappedClient.name = STRINGSTORE(hisroles->c_str() + 7);
          }
        }
    }

    if (mappedClient.role && (!strcmp(mappedClient.role, "root")))
    {
      mappedClient.name = STRINGSTORE("root");
    }
    
    break;
  }
  while (0);

  MapMutex.UnLock();  // <--

  XrdSecEntity* newentity = new XrdSecEntity();

  if (newentity)
  {
    newentity->name = mappedClient.name;
    newentity->role = mappedClient.role;
    newentity->host = mappedClient.host;
    newentity->vorg = mappedClient.vorg;
    newentity->grps = mappedClient.grps;
    newentity->endorsements = mappedClient.endorsements;
    newentity->tident = mappedClient.tident;
    secentitystore->Add(clientid, newentity, 60);
  }

  SecEntityMutex.UnLock();  // <--
  uid_t _client_uid;
  gid_t _client_gid;
  GetId("-", mappedClient, _client_uid, _client_gid);
  XrdxCastor2FsUFS::SetId(_client_uid, _client_gid);
  return;
}


//------------------------------------------------------------------------------
// Reload grid map file
//------------------------------------------------------------------------------
void
XrdxCastor2Fs::ReloadGridMapFile()
{
  xcastor_debug("reload grid map file");
  static time_t GridMapMtime = 0;
  static time_t GridMapCheckTime = 0;
  time_t now = time(NULL);

  // Load it for the first time or again
  if ((!GridMapCheckTime) ||
      (now > GridMapCheckTime + XCASTOR2FS_GRIDMAPCHECKINTERVAL))
  {
    struct stat buf;

    if (!::stat(GridMapFile.c_str(), &buf))
    {
      if (buf.st_mtime != GridMapMtime)
      {
        GridMapMutex.Lock();
        // Store the last modification time
        GridMapMtime = buf.st_mtime;
        // Store the current time of the check
        GridMapCheckTime = now;
        // Dump the current table
        gridmapstore->Purge();
        // Open the gridmap file
        FILE* mapin = fopen(GridMapFile.c_str(), "r");

        if (!mapin)
        {
          // Error no grid map possible
          xcastor_debug("Unable to open gridmapfile:%s - no mapping!", GridMapFile.c_str());
        }
        else
        {
          char userdnin[4096];
          char usernameout[4096];
          int nitems;

          // Parse it
          while ((nitems = fscanf(mapin, "\"%[^\"]\" %s\n", userdnin, usernameout)) == 2)
          {
            XrdOucString dn = userdnin;
            dn.replace("\"", "");
            // Leave only the first CN=, cut the rest
            int pos = dn.find("CN=");
            int pos2 = dn.find("/", pos);

            if (pos2 > 0) dn.erase(pos2);

            if (!gridmapstore->Find(dn.c_str()))
            {
              gridmapstore->Add(dn.c_str(), new XrdOucString(usernameout));
              xcastor_debug("GridMapFile mapping added: %s => %s", dn.c_str(), usernameout);
            }
          }

          fclose(mapin);
        }

        GridMapMutex.UnLock();
      }
      else
      {
        // The file didn't change, we don't do anything
      }
    }
    else
    {
      xcastor_debug("Unbale to stat gridmapfile:%s - no mapping!", GridMapFile.c_str());
    }
  }
}



//------------------------------------------------------------------------------
// Get allowed service class for the requested path and desired svc
//------------------------------------------------------------------------------
std::string 
XrdxCastor2Fs::GetAllowedSvc(const char* path,
                             const std::string& desired_svc)
{
  xcastor_debug("path=%s", path);
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
    subpath.assign(spath, 0, pos);
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
std::set<std::string>&
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
    subpath.assign(spath, 0, pos);
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
  LogId()
{
  ConfigFN  = 0;
  mLogLevel = LOG_INFO; // default log level

  // Setup the circular in-memory logging buffer  
  XrdOucString unit = "rdr@";
  unit += XrdSysDNS::getHostName();
  unit += ":";
  unit += xCastor2FsTargetPort;
  
  Logging::Init();
  Logging::SetLogPriority( mLogLevel );
  Logging::SetUnit(unit.c_str());
  xcastor_info( "info=\"logging configured\" " );
}


//------------------------------------------------------------------------------
// Initialisation
//------------------------------------------------------------------------------
bool
XrdxCastor2Fs::Init()
{
  nsMap   = new XrdOucHash<XrdOucString> ();
  stagertable   = new XrdOucHash<XrdOucString> ();
  roletable = new XrdOucHash<XrdOucString> ();
  stringstore = new XrdOucHash<XrdOucString> ();
  secentitystore = new XrdOucHash<XrdSecEntity> ();
  gridmapstore = new XrdOucHash<XrdOucString> ();
  passwdstore = new XrdOucHash<struct passwd> ();
  groupinfocache   = new XrdOucHash<XrdxCastor2FsGroupInfo> ();
  XrdxCastor2Stager::msDelayStore = new XrdOucHash<XrdOucString> ();
  msCastorClient = xcastor::XrdxCastorClient::Create();
  if (!msCastorClient)
  {
    OfsEroute.Emsg( "Config", "error=failed to create castor client object" );
    return false;
  }

  return true;
}


//------------------------------------------------------------------------------
// Do namespace mappping
//------------------------------------------------------------------------------
XrdOucString
XrdxCastor2Fs::NsMapping(XrdOucString input)
{
  XrdOucString output = "";
  XrdOucString prefix;
  XrdOucString* value;
  int pos = input.find('/', 1);

  if (pos == STR_NPOS)
  {
    if (!(value = nsMap->Find("/")))
      return output;
   
    pos = 0;
    prefix = "/";
  }
  else
  {
    prefix.assign(input, 0, pos);

    // First check the namespace with deepness 1
    if (!(value = nsMap->Find(prefix.c_str())))
    {
      // If not found then check the namespace with deepness 0
      // e.g. look for a root mapping
      if (!(value = nsMap->Find("/")))
        return output;

      pos = 0;
      prefix = "/";
    }
  }

  output.assign(value->c_str(), 0, value->length() - 1);
  input.erase(prefix, 0, prefix.length());
  output += input;
  xcastor_static_debug("input=%s, output=%s", input.c_str(), output.c_str());
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
  const char* tident = error.getErrUser();
  mode_t acc_mode = Mode & S_IAMB;
  XrdOucEnv chmod_Env(info);
  XrdSecEntity mappedclient;
  XrdOucString map_path;
  xcastor_debug("path=%s", path);
  AUTHORIZE(client, &chmod_Env, AOP_Chmod, "chmod", path, error)
  map_path = NsMapping(path);

  if (map_path == "")
  {
    error.setErrInfo(ENOMEDIUM, "No mapping for file name");
    return SFS_ERROR;
  }

  RoleMap(client, info, mappedclient, tident);

  if (gMgr->Proc)
    gMgr->Stats.IncCmd();

  // Perform the actual chmod
  if (XrdxCastor2FsUFS::Chmod(map_path.c_str(), acc_mode))
    return XrdxCastor2Fs::Emsg(epname, error, serrno, "change mode on", map_path.c_str());

  return SFS_OK;
}


//------------------------------------------------------------------------------
// Determine if file exists
//------------------------------------------------------------------------------
int
XrdxCastor2Fs::exists(const char*          path,
                      XrdSfsFileExistence& file_exists,
                      XrdOucErrInfo&       error,
                      const XrdSecEntity*  client,
                      const char*          info)

{
  static const char* epname = "exists";
  const char* tident = error.getErrUser();
  XrdOucEnv exists_Env(info);
  XrdSecEntity mappedclient;
  XrdOucString map_path;
  xcastor_debug("path=%s", path);
  AUTHORIZE(client, &exists_Env, AOP_Stat, "execute exists", path, error)
  map_path = NsMapping(path);
  RoleMap(client, info, mappedclient, tident);

  if (map_path == "")
  {
    error.setErrInfo(ENOMEDIUM, "No mapping for file name");
    return SFS_ERROR;
  }

  if (gMgr->Proc)
    gMgr->Stats.IncCmd();

  return _exists(map_path.c_str(), file_exists, error, &mappedclient, info);
}


//------------------------------------------------------------------------------
// Test if file exists - low level
//------------------------------------------------------------------------------
int
XrdxCastor2Fs::_exists(const char*          path,
                       XrdSfsFileExistence& file_exists,
                       XrdOucErrInfo&       error,
                       const XrdSecEntity*  /*client*/,
                       const char*          /*info*/)

{
  static const char* epname = "exists";
  struct Cns_filestatcs fstat;

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
  const char* tident = error.getErrUser();
  XrdOucEnv mkdir_Env(info);
  XrdSecEntity mappedclient;
  XrdOucString map_path;
  xcastor_debug("path=%s", path);
  map_path = NsMapping(path);
  RoleMap(client, info, mappedclient, tident);

  if (map_path == "")
  {
    error.setErrInfo(ENOMEDIUM, "No mapping for file name");
    return SFS_ERROR;
  }

  if (gMgr->Proc)
    gMgr->Stats.IncCmd();

  int r1 = _mkdir(map_path.c_str(), Mode, error, &mappedclient, info);
  int r2 = SFS_OK;
  return (r1 | r2);
}


//------------------------------------------------------------------------------
// Mkdir - low level
//------------------------------------------------------------------------------
int
XrdxCastor2Fs::_mkdir(const char*         path,
                      XrdSfsMode          Mode,
                      XrdOucErrInfo&      error,
                      const XrdSecEntity* client,
                      const char*         /*info*/)

{
  static const char* epname = "mkdir";
  mode_t acc_mode = (Mode & S_IAMB) | S_IFDIR;

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
        ||  local_path == actual_path) return 0;

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
        if (!rc)
        {
          if (client) SetAcl(actual_path, (*client), 0);
        }

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
    SetAcl(path, (*client), 0);

  return SFS_OK;
}


//------------------------------------------------------------------------------
// Mkpath
// TODO: caution! this function does not set any ownership attributes if no client
// & error is given
//------------------------------------------------------------------------------
int
XrdxCastor2Fs::Mkpath(const char*    path,
                      mode_t         mode,
                      const char*    info,
                      XrdSecEntity*  client,
                      XrdOucErrInfo* error)
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
      ||  local_path == actual_path) return 0;

  *local_path = '\0';

  if (!XrdxCastor2FsUFS::Statfn(actual_path, &buf)) return 0;

  *local_path = '/';
  // Start creating directories starting with the root. Notice that we will not
  // do anything with the last component. The caller is responsible for that.
  local_path = actual_path + 1;

  while ((next_path = index(local_path, int('/'))))
  {
    *next_path = '\0';

    if (XrdxCastor2FsUFS::Statfn(actual_path, &buf))
    {
      if (client && error)
      {
        if ((gMgr->_mkdir(actual_path, mode, (*error), client, info)))
          return -serrno;
      }
      else
      {
        if (XrdxCastor2FsUFS::Mkdir(actual_path, mode) && (serrno != EEXIST))
          return -serrno;
      }
    }

    // Set acl on directory
    *next_path = '/';
    local_path = next_path + 1;
  }

  return 0;
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
  const char* tident = error.getErrUser();
  XrdSecEntity mappedclient;
  XrdOucString sinfo = (ininfo ? ininfo : "");
  XrdOucString map_path;
  const char* info = 0;
  int qpos = 0;

  if ((qpos = sinfo.find("?")) != STR_NPOS)
    sinfo.erase(qpos);
  
  info = (ininfo ? sinfo.c_str() : ininfo);
  xcastor_debug("path=%s, info=%s", path, (info ? info : ""));

  XrdOucEnv Open_Env(info);
  xcastor::Timing preparetiming("stageprepare");
  map_path = NsMapping(path);

  if (map_path == "")
  {
    error.setErrInfo(ENOMEDIUM, "No mapping for file name");
    return SFS_ERROR;
  }

  RoleMap(client, info, mappedclient, tident);
  struct Cns_filestatcs cstat;

  if (XrdxCastor2FsUFS::Statfn(map_path.c_str(), &cstat))
    return XrdxCastor2Fs::Emsg(epname, error, serrno, "stat", map_path.c_str());
 
  uid_t client_uid;
  gid_t client_gid;
  GetId(map_path.c_str(), mappedclient, client_uid, client_gid);
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
  XrdOucString reducedTident, hostname, stident;
  reducedTident = "";
  hostname = "";
  stident = tident;
  int dotpos = stident.find(".");
  reducedTident.assign(tident, 0, dotpos - 1);
  reducedTident += "@";
  int adpos  = stident.find("@");
  hostname.assign(tident, adpos + 1);
  reducedTident += hostname;

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
  XrdOucString map_path;
  map_path = NsMapping(path);
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
  const char* tident = error.getErrUser();
  XrdSecEntity mappedclient;
  xcastor::Timing rmtiming("fileremove");
  XrdOucString map_path;
  TIMING("START", &rmtiming);
  xcastor_debug("path=%s", path);
  XrdOucEnv env(info);
  AUTHORIZE(client, &env, AOP_Delete, "remove", path, error)
  map_path = NsMapping(path);
  xcastor_debug("map_path=%s", map_path.c_str());
  RoleMap(client, info, mappedclient, tident);

  if (map_path == "")
  {
    error.setErrInfo(ENOMEDIUM, "No mapping for file name");
    return SFS_ERROR;
  }

  uid_t client_uid;
  gid_t client_gid;
  GetId(map_path.c_str(), mappedclient, client_uid, client_gid);

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
      return XrdxCastor2Fs::Emsg(epname, error, EINVAL, "rem - cannot find a valid service "
                                 "class mapping for fn = ", map_path.c_str());

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
  retc = _rem(map_path.c_str(), error, &mappedclient, info);
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
                    const XrdSecEntity* /*client*/,
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
XrdxCastor2Fs::remdir(const char*         path,
                      XrdOucErrInfo&      error,
                      const XrdSecEntity* client,
                      const char*         info)
{
  static const char* epname = "remdir";
  const char* tident = error.getErrUser();
  XrdOucEnv remdir_Env(info);
  XrdSecEntity mappedclient;
  XrdOucString map_path;
  xcastor_debug("path=%s", path);
  AUTHORIZE(client, &remdir_Env, AOP_Delete, "remove", path, error)
  map_path = NsMapping(path);
  RoleMap(client, info, mappedclient, tident);

  if (map_path == "")
  {
    error.setErrInfo(ENOMEDIUM, "No mapping for file name");
    return SFS_ERROR;
  }

  if (gMgr->Proc)
    gMgr->Stats.IncCmd();

  return _remdir(map_path.c_str(), error, &mappedclient, info);
}


//------------------------------------------------------------------------------
// Delete a directory
//------------------------------------------------------------------------------
int
XrdxCastor2Fs::_remdir(const char*         path,
                       XrdOucErrInfo&      error,
                       const XrdSecEntity* /*client*/,
                       const char*         /*info*/)
{
  static const char* epname = "remdir";

  // Perform the actual deletion
  if (XrdxCastor2FsUFS::Remdir(path))
    return XrdxCastor2Fs::Emsg(epname, error, serrno, "remove", path);

  return SFS_OK;
}


//------------------------------------------------------------------------------
// Rename
//------------------------------------------------------------------------------
int
XrdxCastor2Fs::rename(const char*         old_name,
                      const char*         new_name,
                      XrdOucErrInfo&      error,
                      const XrdSecEntity* client,
                      const char*         infoO,
                      const char*         infoN)

{
  static const char* epname = "rename";
  const char* tident = error.getErrUser();
  XrdOucString source, destination;
  XrdOucString oldn, newn;
  XrdSecEntity mappedclient;
  XrdOucEnv renameo_Env(infoO);
  XrdOucEnv renamen_Env(infoN);
  AUTHORIZE(client, &renameo_Env, AOP_Update, "rename", old_name, error)
  AUTHORIZE(client, &renamen_Env, AOP_Update, "rename", new_name, error)
  oldn = NsMapping(old_name);
  
  if (oldn == "")
  {
    error.setErrInfo(ENOMEDIUM, "No mapping for file name");
    return SFS_ERROR;
  }
  
  newn = NsMapping(new_name);
  
  if (newn == "")
  {
    error.setErrInfo(ENOMEDIUM, "No mapping for file name");
    return SFS_ERROR;
  }
  
  RoleMap(client, infoO, mappedclient, tident);

  if (gMgr->Proc)
    gMgr->Stats.IncCmd();
  
  int r1, r2;
  r1 = r2 = SFS_OK;
  // Check if dest is existing
  XrdSfsFileExistence file_exists;

  if (!_exists(newn.c_str(), file_exists, error, &mappedclient, infoN))
  {
    if (file_exists == XrdSfsFileExistIsDirectory)
    {
      // We have to path the destination name since the target is a directory
      XrdOucString sourcebase = oldn.c_str();
      int npos = oldn.rfind("/");

      if (npos == STR_NPOS)
      {
        return XrdxCastor2Fs::Emsg(epname, error, EINVAL, "rename", oldn.c_str());
      }

      sourcebase.assign(oldn, npos);
      newn += "/";
      newn += sourcebase;

      while (newn.replace("//", "/")) {};
    }

    if (file_exists == XrdSfsFileExistIsFile)
    {
      // Remove the target file first!
      int remrc = _rem(newn.c_str(), error, &mappedclient, infoN);

      if (remrc)
      {
        return remrc;
      }
    }
  }

  r1 = XrdxCastor2FsUFS::Rename(oldn.c_str(), newn.c_str());

  if (r1)
    return XrdxCastor2Fs::Emsg(epname, error, serrno, "rename", oldn.c_str());

  xcastor_debug("namespace rename done: %s => %s", oldn.c_str(), newn.c_str());
  return SFS_OK;
}


//------------------------------------------------------------------------------
// Stat - get all information
//------------------------------------------------------------------------------
int
XrdxCastor2Fs::stat(const char*         path,
                    struct stat*        buf,
                    XrdOucErrInfo&      error,
                    const XrdSecEntity* client,
                    const char*         info)

{
  static const char* epname = "stat";
  const char* tident = error.getErrUser();
  XrdSecEntity mappedclient;
  XrdOucString map_path;
  xcastor::Timing stattiming("filestat");
  XrdOucEnv Open_Env(info);
  XrdOucString stage_status = "";
  TIMING("START", &stattiming);
  xcastor_debug("path=%s", path);
  AUTHORIZE(client, &Open_Env, AOP_Stat, "stat", path, error)
  map_path = XrdxCastor2Fs::NsMapping(path);

  if (map_path == "")
  {
    error.setErrInfo(ENOMEDIUM, "No mapping for file name");
    return SFS_ERROR;
  }

  RoleMap(client, info, mappedclient, tident);
  TIMING("CNSSTAT", &stattiming);
  struct Cns_filestatcs cstat;

  if (XrdxCastor2FsUFS::Statfn(map_path.c_str(), &cstat))
    return XrdxCastor2Fs::Emsg(epname, error, serrno, "stat", map_path.c_str());
 
  uid_t client_uid;
  uid_t client_gid;
  GetId(map_path.c_str(), mappedclient, client_uid, client_gid);

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
XrdxCastor2Fs::stat(const char*          Name,
                    mode_t&              mode,
                    XrdOucErrInfo&       out_error,
                    const XrdSecEntity*  client,
                    const char*          opaque)
{
  struct stat bfr;
  int rc = stat(Name, &bfr, out_error, client, opaque);

  if (!rc) mode = bfr.st_mode;

  return rc;
}



//------------------------------------------------------------------------------
// Lstat
//------------------------------------------------------------------------------
int
XrdxCastor2Fs::lstat(const char*         path,
                     struct stat*        buf,
                     XrdOucErrInfo&      error,
                     const XrdSecEntity* client,
                     const char*         info)
{
  static const char* epname = "lstat";
  const char* tident = error.getErrUser();
  XrdSecEntity mappedclient;
  XrdOucString map_path;
  XrdOucEnv lstat_Env(info);
  xcastor::Timing stattiming("filelstat");
  TIMING("START", &stattiming);
  xcastor_debug("path=%s", path);
  AUTHORIZE(client, &lstat_Env, AOP_Stat, "lstat", path, error)
  map_path = XrdxCastor2Fs::NsMapping(path);

  if (map_path == "")
  {
    error.setErrInfo(ENOMEDIUM, "No mapping for file name");
    return SFS_ERROR;
  }

  RoleMap(client, info, mappedclient, tident);

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
  return SFS_ERROR;
}


//------------------------------------------------------------------------------
// Read link
//------------------------------------------------------------------------------
int
XrdxCastor2Fs::readlink(const char*         path,
                        XrdOucString&       linkpath,
                        XrdOucErrInfo&      error,
                        const XrdSecEntity* client,
                        const char*         info)
{
  static const char* epname = "readlink";
  const char* tident = error.getErrUser();
  XrdSecEntity mappedclient;
  XrdOucString map_path;
  XrdOucEnv rl_Env(info);
  xcastor_debug("path=%s", path);
  AUTHORIZE(client, &rl_Env, AOP_Stat, "readlink", path, error)
  map_path = XrdxCastor2Fs::NsMapping(path);

  if (map_path == "")
  {
    error.setErrInfo(ENOMEDIUM, "No mapping for file name");
    return SFS_ERROR;
  }

  RoleMap(client, info, mappedclient, tident);
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
XrdxCastor2Fs::symlink(const char*         path,
                       const char*         linkpath,
                       XrdOucErrInfo&      error,
                       const XrdSecEntity* client,
                       const char*         info)
{
  static const char* epname = "symlink";
  const char* tident = error.getErrUser();
  XrdSecEntity mappedclient;
  XrdOucEnv sl_Env(info);
  xcastor_debug("path=%s", path);
  XrdOucString source, destination;
  AUTHORIZE(client, &sl_Env, AOP_Create, "symlink", linkpath, error)
  // We only need to map absolut links
  source = path;

  if (path[0] == '/')
  {
    source = XrdxCastor2Fs::NsMapping(path);

    if (source == "")
    {
      error.setErrInfo(ENOMEDIUM, "No mapping for file name");
      return SFS_ERROR;
    }
  }

  {
    destination = XrdxCastor2Fs::NsMapping(linkpath);

    if (destination == "")
    {
      error.setErrInfo(ENOMEDIUM, "No mapping for file name");
      return SFS_ERROR;
    }
  }

  RoleMap(client, info, mappedclient, tident);
  char lp[4096];

  if (XrdxCastor2FsUFS::Symlink(source.c_str(), destination.c_str()))
    return XrdxCastor2Fs::Emsg(epname, error, serrno, "symlink", source.c_str());

  SetAcl(destination.c_str(), mappedclient, 1);
  linkpath = lp;
  return SFS_OK;
}

//------------------------------------------------------------------------------
// Access
//------------------------------------------------------------------------------
int
XrdxCastor2Fs::access(const char*         path,
                      int                 /*mode*/,
                      XrdOucErrInfo&      error,
                      const XrdSecEntity* client,
                      const char*         info)
{
  static const char* epname = "access";
  const char* tident = error.getErrUser();
  XrdSecEntity mappedclient;
  XrdOucEnv access_Env(info);
  XrdOucString map_path;
  xcastor_debug("path=%s", path);
  AUTHORIZE(client, &access_Env, AOP_Stat, "access", path, error)
  map_path = XrdxCastor2Fs::NsMapping(path);

  if (map_path == "")
  {
    error.setErrInfo(ENOMEDIUM, "No mapping for file name");
    return SFS_ERROR;
  }

  RoleMap(client, info, mappedclient, tident);
  return SFS_OK;
}


//------------------------------------------------------------------------------
// Utimes
//------------------------------------------------------------------------------
int XrdxCastor2Fs::utimes(const char*         path,
                          struct timeval*     tvp,
                          XrdOucErrInfo&      error,
                          const XrdSecEntity* client,
                          const char*         info)
{
  static const char* epname = "utimes";
  const char* tident = error.getErrUser();
  XrdSecEntity mappedclient;
  XrdOucString map_path;
  XrdOucEnv utimes_Env(info);
  xcastor_debug("path=%s", path);
  AUTHORIZE(client, &utimes_Env, AOP_Update, "set utimes", path, error)
  map_path = XrdxCastor2Fs::NsMapping(path);

  if (map_path == "")
  {
    error.setErrInfo(ENOMEDIUM, "No mapping for file name");
    return SFS_ERROR;
  }

  RoleMap(client, info, mappedclient, tident);

  if ((XrdxCastor2FsUFS::Utimes(map_path.c_str(), tvp)))
    return XrdxCastor2Fs::Emsg(epname, error, serrno, "utimes", map_path.c_str());

  return SFS_OK;
}


//------------------------------------------------------------------------------
//! Emsg
//------------------------------------------------------------------------------
int
XrdxCastor2Fs::Emsg(const char*    pfx,
                    XrdOucErrInfo& einfo,
                    int            ecode,
                    const char*    op,
                    const char*    target)
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
                         int            stime,
                         const char*    msg)
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
XrdxCastor2Fs::fsctl(const int           cmd,
                     const char*         args,
                     XrdOucErrInfo&      error,
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

    if ((_exists(path.c_str(), file_exists, error, client, 0)) ||
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
      int retc = lstat(path.c_str(),
                       &buf,
                       error,
                       client,
                       0);

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
        int retc = chmod(path.c_str(),
                         newmode,
                         error,
                         client,
                         0);
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
        int retc = utimes(path.c_str(),
                          tvp,
                          error,
                          client,
                          0);
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
      if (delay > (4 * 3600))
      {
        // We don't delay for more than 4 hours
        delay = (4 * 3600);
      }
      
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
    {
      delay = 0;
    }
  }
  else 
  {
    // Send a delay response to the client
    delay = xCastor2FsDelayRead;

    if (delay > 0)
    {
      if (delay > (4 * 3600))
      {
        // We don't delay for more than 4 hours
        delay = (4 * 3600);
      }
      
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
    {
      delay = 0;
    }
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

