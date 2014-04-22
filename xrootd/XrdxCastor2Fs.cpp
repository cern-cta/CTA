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
#include "XrdSec/XrdSecInterface.hh"
#include "XrdSfs/XrdSfsAio.hh"
#include "XrdClient/XrdClientEnv.hh"
#include "XrdCl/XrdClFileSystem.hh"
/*-----------------------------------------------------------------------------*/
#include "XrdxCastor2Fs.hpp"
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

XrdSysError  xCastor2FsEroute(0);
XrdSysError* XrdxCastor2Fs::eDest;
XrdOucHash<XrdOucString>* XrdxCastor2Fs::filesystemhosttable;
XrdSysMutex               XrdxCastor2Fs::filesystemhosttablelock;
XrdOucHash<XrdOucString>* XrdxCastor2Fs::nsMap;
XrdOucHash<XrdOucString>* XrdxCastor2Fs::stagertable;
XrdOucHash<XrdOucString>* XrdxCastor2Fs::stagerpolicy;
XrdOucHash<XrdOucString>* XrdxCastor2Fs::roletable;
XrdOucHash<XrdOucString>* XrdxCastor2Fs::stringstore;
XrdOucHash<XrdSecEntity>* XrdxCastor2Fs::secentitystore;
XrdOucHash<XrdOucString>* XrdxCastor2Fs::gridmapstore;
XrdOucHash<struct passwd>* XrdxCastor2Fs::passwdstore;
XrdOucHash<XrdxCastor2FsGroupInfo>*  XrdxCastor2Fs::groupinfocache;
XrdOucHash<XrdOucString>* XrdxCastor2Stager::msDelayStore;
xcastor::XrdxCastorClient* XrdxCastor2Fs::msCastorClient;

int XrdxCastor2Fs::TokenLockTime = 5;
XrdxCastor2Fs* XrdxCastor2FS = 0;

#define XCASTOR2FS_EXACT_MATCH 1000

/******************************************************************************/
/*                        C o n v i n i e n c e                               */
/******************************************************************************/

//------------------------------------------------------------------------------
// This helps to avoid memory leaks by strdup, we maintain a string hash to
// keep all used user ids/group ids etc.
//------------------------------------------------------------------------------
char*
STRINGSTORE(const char* __charptr__)
{
  XrdOucString* yourstring;

  if (!__charptr__) return "";

  XrdxCastor2FS->StoreMutex.Lock();

  if ((yourstring = XrdxCastor2FS->stringstore->Find(__charptr__)))
  {
    XrdxCastor2FS->StoreMutex.UnLock();
    return (char*)yourstring->c_str();
  }
  else
  {
    XrdOucString* newstring = new XrdOucString(__charptr__);
    XrdxCastor2FS->stringstore->Add(__charptr__, newstring);
    XrdxCastor2FS->StoreMutex.UnLock();
    return (char*)newstring->c_str();
  }
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

      if ((hisroles = roletable->Find(mappedClient.name)))
      {
        XrdOucString allgroups;
        XrdOucString defaultgroup;
        XrdOucString FixedName;

        if (hisroles->beginswith("static:"))
          FixedName = hisroles->c_str() + 7;        
        else
          FixedName = hisroles->c_str();
        

        while ((FixedName.find(":")) != STR_NPOS)
          FixedName.replace(":", "");

        mappedClient.name = STRINGSTORE(FixedName.c_str());
        GetAllGroups(mappedClient.name, allgroups, defaultgroup);
        mappedClient.grps = STRINGSTORE(defaultgroup.c_str());
        mappedClient.role = STRINGSTORE(defaultgroup.c_str());
        break;
      }

      if ((hisroles = roletable->Find("*")))
      {
        XrdOucString allgroups;
        XrdOucString defaultgroup;
        XrdOucString FixedName;

        if (hisroles->beginswith("static:"))
          FixedName = hisroles->c_str() + 7;
        else
          FixedName = hisroles->c_str();

        while ((FixedName.find(":")) != STR_NPOS)
          FixedName.replace(":", "");
      
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

      if (hisroles) {
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
// Set stage variables
//------------------------------------------------------------------------------
int
XrdxCastor2Fs::SetStageVariables(const char*   Path,
                                 const char*   Opaque,
                                 XrdOucString  stagevariables,
                                 XrdOucString& stagehost,
                                 XrdOucString& serviceclass,
                                 int           n,
                                 const char*   /*tident*/)
{
  XrdOucEnv Open_Env(Opaque);

  if (Opaque)
  {
    xcastor_debug("path=%s opaque=%s stagevariables=%s", Path, Opaque,
                  stagevariables.c_str());
  }
  else
  {
    xcastor_debug("path=%s, stagevariables=%s", Path, stagevariables.c_str());
  }

  // The tokenizer likes line feeds
  stagevariables.replace(',', '\n');
  XrdOucTokenizer stagetokens((char*)stagevariables.c_str());
  XrdOucString desiredstagehost = "";
  XrdOucString desiredserviceclass = "";
  const char* sh = 0;
  const char* stoken;
  int ntoken = 0;

  while ((stoken = stagetokens.GetLine()))
  {
    if ((sh = Open_Env.Get("stagerHost")))
      desiredstagehost = sh;
    else
      desiredstagehost = "";

    if ((sh = Open_Env.Get("svcClass")))
      desiredserviceclass = sh;
    else
      desiredserviceclass = "";

    XrdOucString mhost = stoken;
    XrdOucString shost;
    int offset;

    if ((offset = mhost.find("::")) != STR_NPOS)
    {
      shost.assign(mhost.c_str(), 0, offset - 1);
      stagehost = shost.c_str();
      shost.assign(mhost.c_str(), offset + 2);
      serviceclass = shost;
    }
    else
    {
      stagehost = mhost;
      serviceclass = "default";
    }

    xcastor_debug("0 path=%s, dstagehost=%s, dserviceclass=%s, stagehost=%s, "
                  "serviceclass=%s, n=%i, ntoken=%i", Path,
                  desiredstagehost.c_str(), desiredserviceclass.c_str(),
                  stagehost.c_str(), serviceclass.c_str(), n, ntoken);

    if (!desiredstagehost.length())
    {
      if (stagehost == "*")
      {
        XrdOucString* dh = 0;

        // Use the default one
        if ((dh = stagertable->Find("default")))
        {
          XrdOucString defhost = dh->c_str();
          int spos = 0;

          // Remove the service class if defined
          if ((spos = defhost.find("::")) != STR_NPOS)
            defhost.assign(defhost.c_str(), 0, spos - 1);

          // Set the default one now!
          desiredstagehost = defhost;
        }
      }
      else
      {
        desiredstagehost = stagehost;
      }
    }

    if ((stagehost == "*"))
    {
      if (desiredstagehost.length())
      {
        stagehost = desiredstagehost;

        if (!desiredserviceclass.length())
        {
          if (serviceclass != "*")
            desiredserviceclass = serviceclass;
        }
      }
    }

    if ((serviceclass == "*") && desiredserviceclass.length())
    {
      serviceclass = desiredserviceclass;
    }

    xcastor_debug("1 path=%s, dstagehost=%s, dserviceclass=%s, stagehost=%s, "
                  "serviceclass=%s, n=%i, ntoken=%i",
                  Path, desiredstagehost.c_str(), desiredserviceclass.c_str(),
                  stagehost.c_str(), serviceclass.c_str(), n, ntoken);

    // This handles the case, where  only the stager is set
    if ((desiredstagehost == stagehost) && (!desiredserviceclass.length()))
    {
      if (serviceclass != "*")
      {
        desiredserviceclass = serviceclass;
      }
    }

    xcastor_debug("2 path=%s, dstagehost=%s, dserviceclass=%s, stagehost=%s, "
                  "serviceclass=%s, n=%i, ntoken=%i",
                  Path, desiredstagehost.c_str(), desiredserviceclass.c_str(),
                  stagehost.c_str(), serviceclass.c_str(), n, ntoken);

    // We are successfull if we either return the appropriate token or if
    // the token matches the user specified stager/svc class pair as a result:
    // if the host/svc class pair is not defined, a user cannot request it!
    // this now also supports to allow any service class or host via *::default
    // or stager::* to be user selected
    if (((desiredstagehost == stagehost) &&
         (desiredserviceclass == serviceclass)) &&
        ((n == ntoken) || (n == XCASTOR2FS_EXACT_MATCH)))
    {
      xcastor_debug("path=%s, stagehost=%s, serviceclass=%s",
                    Path, stagehost.c_str(), serviceclass.c_str());
      return 1;
    }
    else
    {
      if ((n != XCASTOR2FS_EXACT_MATCH) && (ntoken == n))
        return 0;
    }

    ntoken++;
  }

  return -1;
}


//------------------------------------------------------------------------------
// Get stage variables
//------------------------------------------------------------------------------
bool
XrdxCastor2Fs::GetStageVariables(const char*   Path,
                                 const char*   Opaque,
                                 XrdOucString& stagevariables,
                                 uid_t         client_uid,
                                 gid_t         client_gid,
                                 const char*   /*tident*/)
{
  if (Opaque)
  {
    xcastor_debug("path=%s, opaque=%s", Path, Opaque);
  }
  else
  {
    xcastor_debug("path=%s", Path);
  }

  XrdOucEnv Open_Env(Opaque);
  XrdOucString spath = Path;
  XrdOucString* mhost;
  // Try the stagertable
  int pos = 0;
  bool found = false;

  // Mapping by path is done first
  while ((pos = spath.find("/", pos)) != STR_NPOS)
  {
    XrdOucString subpath;
    subpath.assign(Path, 0, pos);

    if ((mhost = stagertable->Find(subpath.c_str())))
    {
      stagevariables = mhost->c_str();
      found = true;
    }

    pos++;
  }

  // Mapping by uid/gid overwrites path mapping
  XrdOucString uidmap = "uid:";
  XrdOucString gidmap = "gid:";
  uidmap += (int)client_uid;
  gidmap += (int)client_gid;

  if ((mhost = stagertable->Find(uidmap.c_str())))
  {
    stagevariables = mhost->c_str();
    found = true;
  }

  if ((mhost = stagertable->Find(gidmap.c_str())))
  {
    stagevariables = mhost->c_str();
    found = true;
  }

  // uid/gid + path mapping wins!
  // user mapping superseeds group mapping
  pos = 0;

  while ((pos = spath.find("/", pos)) != STR_NPOS)
  {
    XrdOucString subpath;
    subpath.assign(Path, 0, pos);
    subpath += "::";
    subpath += "gid:";
    subpath += (int) client_gid;

    if ((mhost = stagertable->Find(subpath.c_str())))
    {
      stagevariables = mhost->c_str();
      found = true;
    }

    pos++;
  }

  pos = 0;

  while ((pos = spath.find("/", pos)) != STR_NPOS)
  {
    XrdOucString subpath;
    subpath.assign(Path, 0, pos);
    subpath += "::";
    subpath += "uid:";
    subpath += (int) client_uid;

    if ((mhost = stagertable->Find(subpath.c_str())))
    {
      stagevariables = mhost->c_str();
      found = true;
    }

    pos++;
  }

  if (!found)
  {
    if ((mhost = stagertable->Find("default")))
    {
      stagevariables = mhost->c_str();
    }
    else
    {
      return false;
    }
  }

  return true;
}


/******************************************************************************/
/*           D i r e c t o r y   O b j e c t   I n t e r f a c e s            */
/******************************************************************************/


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
XrdxCastor2FsDirectory::XrdxCastor2FsDirectory(char* user, int MonID) :
  XrdSfsDirectory(user, MonID)
{
  ateof = 0;
  fname = 0;
  dh    = (DIR*)0;
  d_pnt = &dirent_full.d_entry;
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
XrdxCastor2FsDirectory::~XrdxCastor2FsDirectory()
{
  if (dh) close();
}


//------------------------------------------------------------------------------
// Open directory
//------------------------------------------------------------------------------
int
XrdxCastor2FsDirectory::open(const char*         dir_path,
                             const XrdSecEntity* client,
                             const char*         info)
{
  static const char* epname = "open";
  const char* tident = error.getErrUser();
  XrdSecEntity mappedclient;
  XrdOucString map_dir;
  XrdOucEnv Open_Env(info);
  AUTHORIZE(client, &Open_Env, AOP_Readdir, "open directory", dir_path, error);
  map_dir = XrdxCastor2Fs::NsMapping(dir_path) ;
  xcastor_debug("open directory: %s", map_dir.c_str());

  if (map_dir == "")
    return XrdxCastor2Fs::Emsg(epname, error, ENOMEDIUM, "map filename", dir_path);

  XrdxCastor2FS->RoleMap(client, info, mappedclient, tident);

  if (XrdxCastor2FS->Proc)
    XrdxCastor2FS->Stats.IncCmd();

  // Verify that this object is not already associated with an open directory
  if (dh) return XrdxCastor2Fs::Emsg(epname, error, EADDRINUSE,
                                       "open directory", map_dir.c_str());

  // Set up values for this directory object
  ateof = 0;
  fname = strdup(map_dir.c_str());

  // Open the directory and get it's id
  if (!(dh = XrdxCastor2FsUFS::OpenDir(map_dir.c_str())))
    return  XrdxCastor2Fs::Emsg(epname, error, serrno, "open directory", map_dir.c_str());

  return SFS_OK;
}


//------------------------------------------------------------------------------
// Read next directory entry
//------------------------------------------------------------------------------
const char*
XrdxCastor2FsDirectory::nextEntry()
{
  static const char* epname = "nextEntry";

  // Lock the directory and do any required tracing
  if (!dh)
  {
    XrdxCastor2Fs::Emsg(epname, error, EBADF, "read directory", fname);
    return (const char*)0;
  }

  // Check if we are at EOF (once there we stay there)
  if (ateof) return (const char*)0;

  if (XrdxCastor2FS->Proc)
    XrdxCastor2FS->Stats.IncReadd();

  // Read the next directory entry
  if (!(d_pnt = XrdxCastor2FsUFS::ReadDir(dh)))
  {
    if (serrno != 0)
      XrdxCastor2Fs::Emsg(epname, error, serrno, "read directory", fname);
    
    return (const char*)0;
  }

  entry = d_pnt->d_name;
  xcastor_debug("dir next entry: %s", entry.c_str());
  // Return the actual entry
  return (const char*)(entry.c_str());
}


//------------------------------------------------------------------------------
// Close directory
//------------------------------------------------------------------------------
int
XrdxCastor2FsDirectory::close()
{
  static const char* epname = "closedir";

  if (XrdxCastor2FS->Proc)
    XrdxCastor2FS->Stats.IncCmd();
 
  // Release the handle
  if (dh && XrdxCastor2FsUFS::CloseDir(dh))
  {
    XrdxCastor2Fs::Emsg(epname, error, serrno ? serrno : EIO, "close directory", fname);
    return SFS_ERROR;
  }

  if (fname) free(fname);

  dh = (DIR*)0;
  return SFS_OK;
}


/******************************************************************************/
/*                F i l e   O b j e c t   I n t e r f a c e s                 */
/******************************************************************************/


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
XrdxCastor2FsFile::XrdxCastor2FsFile(const char* user, int MonID) :
  XrdSfsFile(user, MonID),
  LogId()
{
  oh = -1;
  fname = 0;
  ohp = NULL;
  ohperror = NULL;
  ohperror_cnt = 0;
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
XrdxCastor2FsFile::~XrdxCastor2FsFile()
{
  if (oh) close();
  if (ohp) pclose(ohp);
  if (ohperror) pclose(ohperror);
}


//------------------------------------------------------------------------------
// Open file
//------------------------------------------------------------------------------
int
XrdxCastor2FsFile::open(const char*         path,
                        XrdSfsFileOpenMode  open_mode,
                        mode_t              Mode,
                        const XrdSecEntity* client,
                        const char*         ininfo)
{
  static const char* epname = "open";
  const char* tident = error.getErrUser();
  const char* origpath = path;
  XrdSecEntity mappedclient;
  xcastor::Timing opentiming("fileopen");
  XrdOucString sinfo = (ininfo ? ininfo : "");
  XrdOucString map_path;
  const char* info;
  int qpos = 0;

  // => case where open fails and client bounces back
  if ((qpos = sinfo.find("?") != STR_NPOS))
    sinfo.erase(qpos);

  // If the clients sends tried info, we have to erase it
  if ((qpos = sinfo.find("tried") != STR_NPOS))
    sinfo.erase(qpos);

  // If the clients send old redirection info, we have to erase it
  if ((qpos = sinfo.find("castor2fs.sfn=")))
    sinfo.erase(qpos);

  if (ininfo)
    info = sinfo.c_str();
  else
    info = 0;
  
  if (info)
    xcastor_debug("path=%s, info=%s", path, info);

  TIMING("START", &opentiming);
  map_path = XrdxCastor2Fs::NsMapping(path);
  XrdxCastor2FS->RoleMap(client, info, mappedclient, tident);
  TIMING("MAPPING", &opentiming);

  if (map_path == "")
  {
    error.setErrInfo(ENOMEDIUM, "No mapping for file name");
    return SFS_ERROR;
  }

  // Check if user is coming back for a response after a stall
  bool stall_comeback = XrdxCastor2FS->msCastorClient->HasSubmittedReq(map_path.c_str(), error);

  int aop = AOP_Read;
  int retc = 0;
  int open_flag = 0;
  struct Cns_filestatcs buf;
  int isRW = 0;
  int isRewrite = 0;
  int crOpts = (Mode & SFS_O_MKPTH ? XRDOSS_mkpath : 0);
  XrdOucEnv Open_Env(info);
  int ecode = 0;
  //TODO: print the open mode in hex
  xcastor_debug("open_mode=%i, fn=%s, tident=%s", Mode, map_path.c_str(), tident);

  // Set the actual open mode and find mode
  if (open_mode & SFS_O_CREAT) open_mode = SFS_O_CREAT;
  else if (open_mode & SFS_O_TRUNC) open_mode = SFS_O_TRUNC;

  switch (open_mode & (SFS_O_RDONLY | SFS_O_WRONLY | SFS_O_RDWR |
                       SFS_O_CREAT  | SFS_O_TRUNC))
  {
  case SFS_O_CREAT:
    open_flag  = O_RDWR | O_CREAT | O_EXCL;
    crOpts |= XRDOSS_new;
    isRW = 1;
    break;

  case SFS_O_TRUNC:
    open_flag |= O_RDWR | O_CREAT | O_TRUNC;
    isRW = 1;
    break;

  case SFS_O_RDONLY:
    open_flag = O_RDONLY;
    isRW = 0;
    break;

  case SFS_O_WRONLY:
    open_flag = O_WRONLY;
    isRW = 1;
    break;

  case SFS_O_RDWR:
    open_flag = O_RDWR;
    isRW = 1;
    break;

  default:
    open_flag = O_RDONLY;
    isRW = 0;
    break;
  }

  if (open_flag & O_CREAT)
  {
    AUTHORIZE(client, &Open_Env, AOP_Create, "create", map_path.c_str(), error);
  }
  else
  {
    AUTHORIZE(client, &Open_Env, (isRW ? AOP_Update : AOP_Read), "open", map_path.c_str(), error);
  }
  
  // See if the cluster is in maintenance mode and has delays configured for write/read
  XrdOucString msg_delay;
  int64_t delay = XrdxCastor2FS->GetAdminDelay(msg_delay, isRW);

  if (delay) 
  {
    TIMING("RETURN", &opentiming);
    opentiming.Print();
    return XrdxCastor2FS->Stall(error, delay, msg_delay.c_str());
  }

  TIMING("DELAYCHECK", &opentiming);

  // Prepare to create or open the file, only if this is the first time the user
  // comes with this request. If the user comes after a stall we skip this part.
  if (!stall_comeback)
  {
    if (isRW)
    {
      if (!(retc = XrdxCastor2FsUFS::Statfn(map_path.c_str(), &buf)))
      {
        if (buf.filemode & S_IFDIR)
          return XrdxCastor2Fs::Emsg(epname, error, EISDIR, "open directory as file", map_path.c_str());

        if (crOpts & XRDOSS_new)
        {
          return XrdxCastor2Fs::Emsg(epname, error, EEXIST, "open file in create mode - file exists");
        }
        else
        {
          if (open_mode == SFS_O_TRUNC)
          {
            // We delete the file because the open specified the truncate flag
            if ((retc = XrdxCastor2FsUFS::Unlink(map_path.c_str())))
            {
              return XrdxCastor2Fs::Emsg(epname, error, serrno, "remove file as requested by truncate flag",
                                         map_path.c_str());
            }
          }
          else
          {
            isRewrite = 1;
          }
        }
      }
    }
    else
    {
      // This is the read/stage case
      if ((retc = XrdxCastor2FsUFS::Statfn(map_path.c_str(), &buf)))
        return XrdxCastor2Fs::Emsg(epname, error, serrno, "stat file", map_path.c_str());
      
      if (!retc && !(buf.filemode & S_IFREG))
      {
        if (buf.filemode & S_IFDIR)
          return XrdxCastor2Fs::Emsg(epname, error, EISDIR, "open directory as file", map_path.c_str());
        else
          return XrdxCastor2Fs::Emsg(epname, error, ENOTBLK, "open not regular file", map_path.c_str());
      }
      
      if (buf.filesize == 0)
      {
        oh = XrdxCastor2FsUFS::Open(XrdxCastor2FS->zeroProc.c_str(), 0, 0);
        fname = strdup(XrdxCastor2FS->zeroProc.c_str());
        return SFS_OK;
      }
    }
  }
  else 
  {
    xcastor_debug("skip some checks as request was previously submitted");
  }
  
  TIMING("PREOPEN", &opentiming);
  uid_t client_uid;
  gid_t client_gid;
  XrdOucString sclient_uid = "";
  XrdOucString sclient_gid = "";
  XrdxCastor2FS->GetId(map_path.c_str(), mappedclient, client_uid, client_gid);
  sclient_uid += (int) client_uid;
  sclient_gid += (int) client_gid;
 
  XrdOucString stagehost = "";
  XrdOucString serviceclass = "default";
  XrdOucString stagevariables = "";
  const char* val;

  if (!XrdxCastor2FS->GetStageVariables(map_path.c_str(), info, stagevariables, 
                                        client_uid, client_gid, tident))
  {
    return XrdxCastor2Fs::Emsg(epname, error, EINVAL, "set the stager host - you didn't "
                               "specify a valid one and I have no stager map for fn = ", 
                               map_path.c_str());
  }

  XrdOucString* policy = NULL;

  if (isRW)
  {
    const char* val;

    // We try the user specified or the first possible setting
    if ((val = Open_Env.Get("stagerHost"))) stagehost = val;
    if ((val = Open_Env.Get("svcClass"))) serviceclass = val;

    if (stagehost.length() && serviceclass.length())
    {
      // Try the user specified pair
      if ((XrdxCastor2FS->SetStageVariables(map_path.c_str(), info, stagevariables, stagehost,
                                            serviceclass, XCASTOR2FS_EXACT_MATCH, tident)) == 1)
      {
        // Select the policy
        XrdOucString* wildcardpolicy = NULL;
        XrdOucString policytag = stagehost;
        policytag += "::";
        policytag += serviceclass;
        XrdOucString grouppolicytag = policytag;
        grouppolicytag += "::gid:";
        grouppolicytag += (int)client_gid;
        XrdOucString userpolicytag  = policytag;
        userpolicytag += "::uid:";
        userpolicytag += (int)client_uid;
        XrdOucString wildcardpolicytag = stagehost;
        wildcardpolicytag += "::";
        wildcardpolicytag += "*";
        wildcardpolicy = XrdxCastor2FS->stagerpolicy->Find(wildcardpolicytag.c_str());

        if (!(policy = XrdxCastor2FS->stagerpolicy->Find(userpolicytag.c_str())))
          if (!(policy = XrdxCastor2FS->stagerpolicy->Find(grouppolicytag.c_str())))
            policy = XrdxCastor2FS->stagerpolicy->Find(policytag.c_str());

        if ((!policy) && (wildcardpolicy))
          policy = wildcardpolicy;

        if (policy && (strstr(policy->c_str(), "ronly")) && isRW)
        {
          return XrdxCastor2Fs::Emsg(epname, error, EPERM, "write - path is forced readonly for you : fn = ",
                                     map_path.c_str());
        }
      }
      else
      {
        return XrdxCastor2Fs::Emsg(epname, error, EINVAL, "write - cannot find any valid "
                                   "stager/service class mapping for you for fn = ", map_path.c_str());
      }
    }
    else
    {
      int n = 0;
      bool allowed = false;
      int hasmore = 0;

      // Try the list and stop when we find a matching policy tag
      do
      {
        if ((hasmore = XrdxCastor2FS->SetStageVariables(map_path.c_str(), info, stagevariables,
                       stagehost, serviceclass, n++, tident)) == 1)
        {
          // Select the policy
          policy = NULL;
          XrdOucString* wildcardpolicy = NULL;
          XrdOucString policytag = stagehost;
          policytag += "::";
          policytag += serviceclass;
          XrdOucString grouppolicytag = policytag;
          grouppolicytag += "::gid:";
          grouppolicytag += (int)client_gid;
          XrdOucString userpolicytag  = policytag;
          userpolicytag += "::uid:";
          userpolicytag += (int)client_uid;
          XrdOucString wildcardpolicytag = stagehost;
          wildcardpolicytag += "::";
          wildcardpolicytag += "*";
          wildcardpolicy = XrdxCastor2FS->stagerpolicy->Find(wildcardpolicytag.c_str());

          if (!(policy = XrdxCastor2FS->stagerpolicy->Find(userpolicytag.c_str())))
            if (!(policy = XrdxCastor2FS->stagerpolicy->Find(grouppolicytag.c_str())))
              policy = XrdxCastor2FS->stagerpolicy->Find(policytag.c_str());

          if ((!policy) && (wildcardpolicy))
          {
            policy = wildcardpolicy;
          }

          if (policy && (!strstr(policy->c_str(), "ronly")))
          {
            allowed = true;
            break;
          }
        }
      }
      while ((n < 20) && (hasmore >= 0));

      if (!allowed)
      {
        return XrdxCastor2Fs::Emsg(epname, error, EPERM, "write - you are not "
                                   "allowed to do write requests for fn = ", map_path.c_str());
      }
    }
  }

  // Open response structure in which we initialize rpfn2 for not scheduled access
  struct XrdxCastor2Stager::RespInfo resp_info;
  resp_info.mRedirectionPfn2 = 0;
  resp_info.mRedirectionPfn2 += ":";
  resp_info.mRedirectionPfn2 += stagehost;
  resp_info.mRedirectionPfn2 += ":";
  resp_info.mRedirectionPfn2 += serviceclass;
  resp_info.mRedirectionPfn2 += ":0";
  resp_info.mRedirectionPfn2 += ":0";

  // Delay tag used to stall the current client incrementally
  XrdOucString delaytag = tident;
  delaytag += ":";
  delaytag += map_path.c_str();

  if (isRW)
  {
    if (!stall_comeback && !isRewrite)
    {
      // File doesn't exist, we have to create the path - check if we need to mkpath
      if ((Mode & SFS_O_MKPTH) || (policy && (strstr(policy->c_str(), "mkpath"))))
      {
        XrdOucString newpath = map_path;

        while (newpath.replace("//", "/")) {};

        int rpos = STR_NPOS;

        while ((rpos = newpath.rfind("/", rpos)) != STR_NPOS)
        {
          rpos--;
          struct Cns_filestatcs cstat;
          XrdOucString spath;
          spath.assign(newpath, 0, rpos);

          if (XrdxCastor2FsUFS::Statfn(spath.c_str(), &cstat))
          {
            // Protect against misconfiguration ( all.export / ) and missing stat result on Cns_stat('/');
            if (rpos < 0)
              return XrdxCastor2Fs::Emsg(epname, error, serrno , "create path in root directory");
            
            continue;
          }
          else
          {
            // Start to create from here and finish
            int fpos = rpos + 2;

            while ((fpos = newpath.find("/", fpos)) != STR_NPOS)
            {
              XrdOucString createpath;
              createpath.assign(newpath, 0, fpos);
              xcastor_debug("creating path as uid=%i, gid=%i", (int)client_uid, (int)client_gid);
              mode_t cmask = 0;
              Cns_umask(cmask);
              fpos++;

              if (XrdxCastor2FsUFS::Mkdir(createpath.c_str(), S_IRWXU | S_IRGRP | S_IXGRP | S_IROTH | S_IXOTH) && (serrno != EEXIST))
                return XrdxCastor2Fs::Emsg(epname, error, serrno , "create path need dir = ", createpath.c_str());
            }

            const char* file_class = 0;

            if ((file_class = Open_Env.Get("fileClass")) &&
                (Cns_chclass(newpath.c_str(), 0, (char*)file_class)))
            {
              return XrdxCastor2Fs::Emsg(epname, error, serrno , "set fileclass to ", file_class);
            }

            break;
          }
        }
      }
    }

    // For all write operations
    TIMING("PUT", &opentiming);
    int status = 0;

    // Create structures for request and response and call the get method
    struct XrdxCastor2Stager::ReqInfo req_info(client_uid, client_gid, map_path.c_str(), 
                                               stagehost.c_str(), serviceclass.c_str());
   
    if (!isRewrite)
    {
      xcastor_debug("Put stagehost=%s, serviceclass=%s", stagehost.c_str(), serviceclass.c_str());
      status = XrdxCastor2Stager::DoAsyncReq(error, "put", &req_info, resp_info);
      delaytag += ":put";
      aop = AOP_Create;
    }
    else
    {
      xcastor_debug("Update stagehost=%s, serviceclass=%s", stagehost.c_str(), serviceclass.c_str());
      status = XrdxCastor2Stager::DoAsyncReq(error, "update", &req_info, resp_info);
      delaytag += ":update";
      aop = AOP_Update;
    }

    if (status == SFS_ERROR)
    {
      // If the file is not yet closed from the last write delay the client
      if (error.getErrInfo() == EBUSY)
      {
        TIMING("RETURN", &opentiming);

        if (XrdxCastor2FS->mLogLevel == LOG_DEBUG)
          opentiming.Print();
      
        return XrdxCastor2FS->Stall(error, XrdxCastor2Stager::GetDelayValue(delaytag.c_str()) , 
                                    "file is still busy, please wait");
      }

      TIMING("RETURN", &opentiming);

      if (XrdxCastor2FS->mLogLevel == LOG_DEBUG)
        opentiming.Print();

      return XrdxCastor2Fs::Emsg(epname, error, EINVAL, "async request failed");

    }
    else if (status >= SFS_STALL)
    {
      TIMING("RETURN", &opentiming);

      if (XrdxCastor2FS->mLogLevel == LOG_DEBUG)
        opentiming.Print();

      return XrdxCastor2FS->Stall(error, XrdxCastor2Stager::GetDelayValue(delaytag.c_str()) , 
                                  "request queue full or response not ready yet");
    }

    // Clean-up any possible delay tag remainings
    XrdxCastor2Stager::DropDelayTag(delaytag.c_str());
    
    if (resp_info.mStageStatus != "READY")
    {
      TIMING("RETURN", &opentiming);
      
      if (XrdxCastor2FS->mLogLevel == LOG_DEBUG)
        opentiming.Print();

      return XrdxCastor2Fs::Emsg(epname, error, EINVAL, "access file in stager (Put request failed)  fn = ",
                                 map_path.c_str());
    }
  }
  else
  {
    // Read operation
    XrdOucString usedpolicytag = "";
    int n = 0;
    bool possible = false;
    int hasmore = 0;

    // Loop through the possible stager settings to find the file somewhere staged
    do
    {
      if ((hasmore = XrdxCastor2FS->SetStageVariables(map_path.c_str(), info,
                     stagevariables, stagehost, serviceclass, n++, tident)) == 1)
      {
        // Select the policy
        policy = NULL;
        XrdOucString* wildcardpolicy = NULL;
        XrdOucString policytag = stagehost;
        policytag += "::";
        policytag += serviceclass;
        XrdOucString grouppolicytag = policytag;
        grouppolicytag += "::gid:";
        grouppolicytag += (int)client_gid;
        XrdOucString userpolicytag  = policytag;
        userpolicytag += "::uid:";
        userpolicytag += (int)client_uid;
        XrdOucString wildcardpolicytag = stagehost;
        wildcardpolicytag += "::";
        wildcardpolicytag += "*";
        wildcardpolicy = XrdxCastor2FS->stagerpolicy->Find(wildcardpolicytag.c_str());
        usedpolicytag = userpolicytag.c_str();

        if (!(policy = XrdxCastor2FS->stagerpolicy->Find(userpolicytag.c_str())))
        {
          usedpolicytag = grouppolicytag.c_str();

          if (!(policy = XrdxCastor2FS->stagerpolicy->Find(grouppolicytag.c_str())))
          {
            usedpolicytag = policytag;
            policy = XrdxCastor2FS->stagerpolicy->Find(policytag.c_str());
          }
        }

        if ((!policy) && (wildcardpolicy))
        {
          policy = wildcardpolicy;
          usedpolicytag = wildcardpolicytag;
        }

        // Query the staging status
        TIMING("STAGERQUERY", &opentiming);
        XrdOucString stage_status;
        
        if (!XrdxCastor2Stager::StagerQuery(error, (uid_t) client_uid, (gid_t) client_gid, 
                                            map_path.c_str(), stagehost.c_str(), 
                                            serviceclass.c_str(), stage_status))
        {
          stage_status = "NA";
        }

        if ((stage_status != "STAGED") && 
            (stage_status != "CANBEMIGR") && 
            (stage_status != "STAGEOUT"))
        {
          // Check if we want transparent staging
          if (policy && ((strstr(policy->c_str(), "nohsm"))))
          {
            
            if (XrdxCastor2FS->mLogLevel == LOG_DEBUG)
              opentiming.Print();

            continue;
          }
        }

        TIMING("PREP2GET", &opentiming);
        
        if (!XrdxCastor2Stager::Prepare2Get(error, (uid_t) client_uid, (gid_t) client_gid, 
                                            map_path.c_str(), stagehost.c_str(), serviceclass.c_str(), 
                                            resp_info))
        {
          TIMING("RETURN", &opentiming);
          
          if (XrdxCastor2FS->mLogLevel == LOG_DEBUG)
            opentiming.Print();

          continue;
        }

        xcastor_debug("Got redirection to:%s and stagestatus is:%s", 
                      resp_info.mRedirectionPfn1.c_str(), 
                      resp_info.mStageStatus.c_str());

        if (resp_info.mRedirectionPfn1.length() && (resp_info.mStageStatus == "READY"))
        {
          // That is perfect, we can use the setting to read
          possible = true;
          XrdxCastor2Stager::DropDelayTag(delaytag.c_str());
          break;
        }

        // We select this setting and tell the client to wait for the staging in this pool/svc class
        if (resp_info.mStageStatus == "READY")
        {
          TIMING("RETURN", &opentiming);
          
          if (XrdxCastor2FS->mLogLevel == LOG_DEBUG)
            opentiming.Print();

          return XrdxCastor2FS->Stall(error, XrdxCastor2Stager::GetDelayValue(delaytag.c_str()) , 
                                      "file is being staged in");
        }
        else
        {
          TIMING("RETURN", &opentiming);
          
          if (XrdxCastor2FS->mLogLevel == LOG_DEBUG)
            opentiming.Print();

          continue;
        }
      }
    }
    while ((n < 20) && (hasmore >= 0));

    if (!possible)
    {
      TIMING("RETURN", &opentiming);
      opentiming.Print();
      XrdxCastor2Stager::DropDelayTag(delaytag.c_str());
      return XrdxCastor2Fs::Emsg(epname, error, EINVAL, "access file in ANY stager (all prepare2get failed) fn = ",
                                 map_path.c_str());
    }

    // If there was no stager_get we fill request id 0
    if (!resp_info.mRedirectionPfn2.length())
    {
      resp_info.mRedirectionPfn2 += ":";
      resp_info.mRedirectionPfn2 += stagehost;
      resp_info.mRedirectionPfn2 += ":";
      resp_info.mRedirectionPfn2 += serviceclass;
    }

    // CANBEMIGR or STAGED, STAGEOUT files can be read ;-)
    if (policy)
      xcastor_debug("policy for:%s is %s", usedpolicytag.c_str(), policy->c_str());
    else
      xcastor_debug("policy for:%s is default [read not scheduled]", usedpolicytag.c_str());

    if (policy && ((strstr(policy->c_str(), "schedread")) || (strstr(policy->c_str(), "schedall"))))
    {
      TIMING("GET", &opentiming);

      // Create structures for request and response and call the get method
      struct XrdxCastor2Stager::ReqInfo req_info(client_uid, client_gid, map_path.c_str(), 
                                                 stagehost.c_str(), serviceclass.c_str());

      int status = XrdxCastor2Stager::DoAsyncReq(error, "get", &req_info, resp_info);
      delaytag += ":get";

      if (status == SFS_ERROR)
      {
        TIMING("RETURN", &opentiming);

        if (XrdxCastor2FS->mLogLevel == LOG_DEBUG)
          opentiming.Print();

        return XrdxCastor2Fs::Emsg(epname, error, EINVAL, "async req failed fn=", map_path.c_str());
      }
      else if (status >= SFS_STALL)
      {
        TIMING("RETURN", &opentiming);

        if (XrdxCastor2FS->mLogLevel == LOG_DEBUG)
          opentiming.Print();

        return XrdxCastor2FS->Stall(error, XrdxCastor2Stager::GetDelayValue(delaytag.c_str()) , 
                                    "request queue full or response not ready");
      }

      if (resp_info.mStageStatus != "READY")
      {
        TIMING("RETURN", &opentiming);
        
        if (XrdxCastor2FS->mLogLevel == LOG_DEBUG)
          opentiming.Print();

        return XrdxCastor2Fs::Emsg(epname, error, EINVAL, "access stager (Get request failed) fn=", 
                                   map_path.c_str());
      }
    }
  }

  // Save the redirection host with and without the domain in the host table 
  int pos;
  XrdOucString nodomainhost = "";

  if ((pos = resp_info.mRedirectionHost.find(".")) != STR_NPOS)
  {
    nodomainhost.assign(resp_info.mRedirectionHost, 0, pos - 1);
    XrdxCastor2FS->filesystemhosttablelock.Lock();    // -->

    if (!XrdxCastor2FS->filesystemhosttable->Find(nodomainhost.c_str()))
      XrdxCastor2FS->filesystemhosttable->Add(nodomainhost.c_str(), new XrdOucString(nodomainhost.c_str()));
    
    XrdxCastor2FS->filesystemhosttablelock.UnLock();  // <--
  }

  XrdxCastor2FS->filesystemhosttablelock.Lock();   // -->

  if (!XrdxCastor2FS->filesystemhosttable->Find(resp_info.mRedirectionHost.c_str()))
    XrdxCastor2FS->filesystemhosttable->Add(resp_info.mRedirectionHost.c_str(), 
                                            new XrdOucString(resp_info.mRedirectionHost.c_str()));
  
  XrdxCastor2FS->filesystemhosttablelock.UnLock(); // -->


  TIMING("REDIRECTION", &opentiming);
  xcastor_debug("redirection to:%s", resp_info.mRedirectionHost.c_str());

  // Save statistics about server read/write operations 
  if (XrdxCastor2FS->Proc)
  {
    std::ostringstream ostreamclient;
    std::ostringstream ostreamserver;

    ostreamclient << stagehost << "::" << serviceclass << "::" << mappedclient.name;
    ostreamserver << stagehost << "::" << serviceclass << "::" 
                  << resp_info.mRedirectionHost;
    
    XrdxCastor2FS->Stats.IncServerRdWr(ostreamserver.str().c_str(), isRW);
    XrdxCastor2FS->Stats.IncUserRdWr(ostreamclient.str().c_str(), isRW);
  }

  // Add the opaque authorization information for the server for read & write
  XrdOucString accopaque = "";
  XrdxCastor2ServerAcc::AuthzInfo* authz = new XrdxCastor2ServerAcc::AuthzInfo(false);
  authz->sfn = (char*) origpath;
  authz->pfn1 = (char*) resp_info.mRedirectionPfn1.c_str();
  authz->pfn2 = (char*) resp_info.mRedirectionPfn2.c_str();
  authz->id  = (char*)tident;
  authz->client_sec_uid = STRINGSTORE(sclient_uid.c_str());
  authz->client_sec_gid = STRINGSTORE(sclient_gid.c_str());
  authz->accessop = aop;
  time_t now = time(NULL);
  authz->exptime  = (now + XrdxCastor2FS->TokenLockTime);
  
  if (XrdxCastor2FS->IssueCapability)
  {
    authz->token = NULL;
    authz->signature = NULL;
  }
  else
  {
    authz->token = "";
    authz->signature = "";
  }
  
  authz->manager = (char*)XrdxCastor2FS->ManagerId.c_str();
  XrdOucString authztoken;
  XrdOucString sb64;
  
  if (!XrdxCastor2ServerAcc::BuildToken(authz, authztoken))
  {
    XrdxCastor2Fs::Emsg(epname, error, retc, "build authorization token for sfn = ", map_path.c_str());
    delete authz;
    return SFS_ERROR;
  }
  
  authz->token = (char*) authztoken.c_str();
  TIMING("BUILDTOKEN", &opentiming);
  
  if (XrdxCastor2FS->IssueCapability)
  {
    int sig64len = 0;
    
    if (!XrdxCastor2FS->ServerAcc->SignBase64((unsigned char*)authz->token,
                                              strlen(authz->token), sb64,
                                              sig64len, stagehost.c_str()))
    {
      XrdxCastor2Fs::Emsg(epname, error, retc, "sign authorization token for sfn = ", map_path.c_str());
      delete authz;
      return SFS_ERROR;
    }
    
    authz->signature = (char*)sb64.c_str();
  }
  
  TIMING("SIGNBASE64", &opentiming);
  
  if (!XrdxCastor2ServerAcc::BuildOpaque(authz, accopaque))
  {
    XrdxCastor2Fs::Emsg(epname, error, retc, "build authorization opaque string for sfn = ", map_path.c_str());
    delete authz;
    return SFS_ERROR;
  }

  TIMING("BUILDOPAQUE", &opentiming);
  // Add the internal token opaque tag
  resp_info.mRedirectionHost += "?";
  
  // We add the user defined stager selection also to the redirection in case
  // the client comes back from a failure
  if ((val = Open_Env.Get("svcClass")))
  {
    resp_info.mRedirectionHost += "svcClass=";
    resp_info.mRedirectionHost += val;
    resp_info.mRedirectionHost += "&";
  }

  if ((val = Open_Env.Get("stagerHost")))
  {
    resp_info.mRedirectionHost += "stagerHost=";
    resp_info.mRedirectionHost += val;
    resp_info.mRedirectionHost += "&";
  }
  
  // Add the external token opaque tag
  resp_info.mRedirectionHost += accopaque;
  
  if (!isRW)
  {
    // We always assume adler, but we should check the type here ...
    if (!strcmp(buf.csumtype, "AD"))
    {
      resp_info.mRedirectionHost += "adler32=";
      resp_info.mRedirectionHost += buf.csumvalue;
      resp_info.mRedirectionHost += "&";
    }

    // Add the 0-size create flag
    if (buf.filesize == 0)
      resp_info.mRedirectionHost += "createifnotexist=1&";
  }
  
  ecode = atoi(XrdxCastor2FS->xCastor2FsTargetPort.c_str());
  error.setErrInfo(ecode, resp_info.mRedirectionHost.c_str());
  delete authz;
  
  TIMING("AUTHZ", &opentiming);

  // Do the proc statistics if a proc file system is specified
  if (XrdxCastor2FS->Proc)
    XrdxCastor2FS->Stats.IncRdWr(isRW);

  // Check if a mode was given
  if (Open_Env.Get("mode"))
  {
    mode_t mode = strtol(Open_Env.Get("mode"), NULL, 8);

    if (XrdxCastor2FsUFS::Chmod(map_path.c_str(), mode))
    {
      return XrdxCastor2Fs::Emsg(epname, error, serrno, "set mode on", map_path.c_str());
    }
  }

  TIMING("PROC/DONE", &opentiming);
  
  if (XrdxCastor2FS->mLogLevel == LOG_DEBUG)
    opentiming.Print();

  xcastor_debug("redirection to:%s", resp_info.mRedirectionHost.c_str());
  return SFS_REDIRECT;
}


//------------------------------------------------------------------------------
// Close file
//------------------------------------------------------------------------------
int
XrdxCastor2FsFile::close()
{
  static const char* epname = "close";

  if (XrdxCastor2FS->Proc)
  {
    XrdxCastor2FS->Stats.IncCmd();
  }

  // Release the handle and return
  if (oh >= 0  && XrdxCastor2FsUFS::Close(oh))
    return XrdxCastor2Fs::Emsg(epname, error, serrno, "close", fname);

  oh = -1;

  if (fname)
  {
    free(fname);
    fname = 0;
  }

  if (ohp != NULL)
  {
    pclose(ohp);
    ohp = NULL;
  }

  if (ohperror_cnt != 0)
  {
    XrdOucString ohperrorfile = "/tmp/xrdfscmd.";
    ohperrorfile += (int)ohperror_cnt;
    ::unlink(ohperrorfile.c_str());
  }

  if (ohperror != NULL)
  {
    fclose(ohperror);
    ohperror = NULL;
  }

  return SFS_OK;
}


//------------------------------------------------------------------------------
// Read from file
//------------------------------------------------------------------------------
XrdSfsXferSize
XrdxCastor2FsFile::read(XrdSfsFileOffset offset,
                        char*            buff,
                        XrdSfsXferSize   blen)
{
  static const char* epname = "read";
  XrdSfsXferSize nbytes;

  if (XrdxCastor2FS->Proc)
  {
    XrdxCastor2FS->Stats.IncCmd();
  }

  // Make sure the offset is not too large
#if _FILE_OFFSET_BITS!=64

  if (offset >  0x000000007fffffff)
    return XrdxCastor2Fs::Emsg(epname, error, EFBIG, "read", fname);

#endif

  // Pipe reads
  if (ohp)
  {
    char linestorage[4096];
    char* lineptr = linestorage;
    char* bufptr = buff;
    size_t n = 4096;
    int pn;
    nbytes = 0;

    while ((pn = getline(&lineptr, &n, ohp)) > 0)
    {
      XrdOucString line = lineptr;
      XrdOucString rep1 = "/";
      rep1 += XrdxCastor2FS->xCastor2FsName;
      rep1 += "/";

      if (line.beginswith(rep1))
        line.replace(rep1, "/");

      memcpy(bufptr, line.c_str(), line.length());
      bufptr[line.length()] = 0;
      nbytes += line.length();
      bufptr += line.length();

      if (nbytes > (blen - 1024))
        break;

      // Read something from stderr if, existing
      if (ohperror)
      {
        while ((pn = getline(&lineptr, &n, ohperror)) > 0)
        {
          XrdOucString line = "__STDERR__>";
          line += lineptr;
          XrdOucString rep1 = "/";
          rep1 += XrdxCastor2FS->xCastor2FsName;
          rep1 += "/";
          line.replace(rep1, "/");
          memcpy(bufptr, line.c_str(), line.length());
          bufptr[line.length()] = 0;
          nbytes += line.length();
          bufptr += line.length();

          if (nbytes > (blen - 1024))
            break;
        }
      }
    }

    // Read something from stderr if, existing
    if (ohperror)
      while ((pn = getline(&lineptr, &n, ohperror)) > 0)
      {
        XrdOucString line = "__STDERR__>";
        line += lineptr;
        XrdOucString rep1 = "/";
        rep1 += XrdxCastor2FS->xCastor2FsName;
        rep1 += "/";
        line.replace(rep1, "/");
        memcpy(bufptr, line.c_str(), line.length());
        bufptr[line.length()] = 0;
        nbytes += line.length();
        bufptr += line.length();

        if (nbytes > (blen - 1024))
          break;
      }

    return nbytes;
  }

  // Read the actual number of bytes
  do
  {
    nbytes = pread(oh, (void*)buff, (size_t)blen, (off_t)offset);
  }
  while (nbytes < 0 && errno == EINTR);

  if (nbytes  < 0)
  {
    return XrdxCastor2Fs::Emsg(epname, error, errno, "read", fname);
  }

  // Return number of bytes read
  return nbytes;
}


//------------------------------------------------------------------------------
// Read from file - asynchronous
//------------------------------------------------------------------------------
int
XrdxCastor2FsFile::read(XrdSfsAio* aiop)
{
  if (XrdxCastor2FS->Proc)
  {
    XrdxCastor2FS->Stats.IncCmd();
  }

  // Execute this request in a synchronous fashion
  aiop->Result = this->read((XrdSfsFileOffset)aiop->sfsAio.aio_offset,
                            (char*)aiop->sfsAio.aio_buf,
                            (XrdSfsXferSize)aiop->sfsAio.aio_nbytes);
  aiop->doneRead();
  return 0;
}


//------------------------------------------------------------------------------
// Write to file
//------------------------------------------------------------------------------
XrdSfsXferSize
XrdxCastor2FsFile::write(XrdSfsFileOffset offset,
                         const char*      buff,
                         XrdSfsXferSize   blen)
{
  static const char* epname = "write";
  XrdSfsXferSize nbytes;

  if (XrdxCastor2FS->Proc)
  {
    XrdxCastor2FS->Stats.IncCmd();
  }

  // Make sure the offset is not too large
#if _FILE_OFFSET_BITS!=64

  if (offset >  0x000000007fffffff)
    return XrdxCastor2Fs::Emsg(epname, error, EFBIG, "write", fname);

#endif

  // Write the requested bytes
  do
  {
    nbytes = pwrite(oh, (void*)buff, (size_t)blen, (off_t)offset);
  }
  while (nbytes < 0 && errno == EINTR);

  if (nbytes  < 0)
    return XrdxCastor2Fs::Emsg(epname, error, errno, "write", fname);

  // Return number of bytes written
  return nbytes;
}


//------------------------------------------------------------------------------
// Write to file - asynchrnous
//------------------------------------------------------------------------------
int
XrdxCastor2FsFile::write(XrdSfsAio* aiop)
{
  // Execute this request in a synchronous fashion
  aiop->Result = this->write((XrdSfsFileOffset)aiop->sfsAio.aio_offset,
                             (char*)aiop->sfsAio.aio_buf,
                             (XrdSfsXferSize)aiop->sfsAio.aio_nbytes);
  aiop->doneWrite();
  return 0;
}


//------------------------------------------------------------------------------
// Stat the file
//------------------------------------------------------------------------------
int
XrdxCastor2FsFile::stat(struct stat* buf)
{
  static const char* epname = "stat";

  // Fake for pipe reads
  if (ohp)
  {
    memset(buf, sizeof(struct stat), 0);
    buf->st_size = 1024 * 1024 * 1024;
    return SFS_OK;
  }

  if (oh)
  {
    int rc = ::stat(fname, buf);

    if (rc)
    {
      return XrdxCastor2Fs::Emsg(epname, error, errno, "stat", fname);
    }

    return SFS_OK;
  }

  // Execute the function
  struct Cns_filestatcs cstat;

  if (XrdxCastor2FsUFS::Statfn(fname, &cstat))
  {
    return XrdxCastor2Fs::Emsg(epname, error, serrno, "stat", fname);
  }

  // All went well
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
  return SFS_OK;
}


//------------------------------------------------------------------------------
// Sync the file
//------------------------------------------------------------------------------
int
XrdxCastor2FsFile::sync()
{
  static const char* epname = "sync";

  if (XrdxCastor2FS->Proc)
  {
    XrdxCastor2FS->Stats.IncCmd();
  }

  // Perform the function
  if (fsync(oh))
    return XrdxCastor2Fs::Emsg(epname, error, errno, "synchronize", fname);

  return SFS_OK;
}


//------------------------------------------------------------------------------
// Sync asynchronous
//------------------------------------------------------------------------------
int
XrdxCastor2FsFile::sync(XrdSfsAio* aiop)
{
  // Execute this request in a synchronous fashion
  aiop->Result = this->sync();
  aiop->doneWrite();
  return 0;
}


//------------------------------------------------------------------------------
// Truncate file
//------------------------------------------------------------------------------
int
XrdxCastor2FsFile::truncate(XrdSfsFileOffset flen)
{
  static const char* epname = "trunc";

  if (XrdxCastor2FS->Proc)
  {
    XrdxCastor2FS->Stats.IncCmd();
  }

  // Make sure the offset is not too larg
#if _FILE_OFFSET_BITS!=64

  if (flen >  0x000000007fffffff)
    return XrdxCastor2Fs::Emsg(epname, error, EFBIG, "truncate", fname);

#endif

  // Perform the function
  if (ftruncate(oh, flen))
    return XrdxCastor2Fs::Emsg(epname, error, errno, "truncate", fname);

  return SFS_OK;
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
// Get file system
//------------------------------------------------------------------------------
extern "C"
XrdSfsFileSystem* XrdSfsGetFileSystem(XrdSfsFileSystem* /*native_fs*/,
                                      XrdSysLogger*     lp,
                                      const char*       configfn)
{
  xCastor2FsEroute.SetPrefix("xcastor2fs_");
  xCastor2FsEroute.logger(lp);
  static XrdxCastor2Fs myFS(&xCastor2FsEroute);
  XrdOucString vs = "xCastor2Fs (xCastor2 File System) v ";
  xCastor2FsEroute.Say("++++++ (c) 2008 CERN/IT-DM-SMD ", vs.c_str());

  // Initialize the subsystems
  if (!myFS.Init(xCastor2FsEroute)) return 0;

  myFS.ConfigFN = (configfn && *configfn ? strdup(configfn) : 0);

  if (myFS.Configure(xCastor2FsEroute)) return 0;

  XrdxCastor2FS = &myFS;
  // Initialize authorization module ServerAcc
  XrdxCastor2FS->ServerAcc = (XrdxCastor2ServerAcc*) XrdAccAuthorizeObject(lp, configfn, NULL);

  if (!XrdxCastor2FS->ServerAcc)
  {
    return 0;
  }

  // Initialize the CASTOR Cthread library
  Cthread_init();
  return &myFS;
}


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
XrdxCastor2Fs::XrdxCastor2Fs(XrdSysError* ep):
  LogId()
{
  eDest = ep;
  ConfigFN  = 0;
  mLogLevel = LOG_INFO; // log info
}


//------------------------------------------------------------------------------
// Initialisation
//------------------------------------------------------------------------------
bool
XrdxCastor2Fs::Init(XrdSysError& fsEroute)
{
  filesystemhosttable = new XrdOucHash<XrdOucString> ();
  nsMap   = new XrdOucHash<XrdOucString> ();
  stagertable   = new XrdOucHash<XrdOucString> ();
  stagerpolicy  = new XrdOucHash<XrdOucString> ();
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
    fsEroute.Emsg( "Config", "error=failed to create castor client object" );
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
    {
      return false;
    }

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
      {
        return output;
      }

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
  AUTHORIZE(client, &chmod_Env, AOP_Chmod, "chmod", path, error);
  map_path = XrdxCastor2Fs::NsMapping(path);

  if (map_path == "")
  {
    error.setErrInfo(ENOMEDIUM, "No mapping for file name");
    return SFS_ERROR;
  }

  RoleMap(client, info, mappedclient, tident);

  if (XrdxCastor2FS->Proc)
  {
    XrdxCastor2FS->Stats.IncCmd();
  }

  // Perform the actual chmod
  if (XrdxCastor2FsUFS::Chmod(map_path.c_str(), acc_mode))
  {
    return XrdxCastor2Fs::Emsg(epname, error, serrno, "change mode on", map_path.c_str());
  }

  return SFS_OK;
}


//------------------------------------------------------------------------------
// Obtain checksum information for a file.
//------------------------------------------------------------------------------
int
XrdxCastor2Fs::chksum(csFunc Func,
                      const char* csName,
                      const char* path,
                      XrdOucErrInfo& error,
                      const XrdSecEntity* client,
                      const char* opaque)
{
  xcastor_debug("path=%s", path);
  static const char* epname = "exists";
  XrdOucEnv chksum_env(opaque);
  AUTHORIZE(client, &chksum_env, AOP_Stat, "chksum", path, error);
  char buff[MAXPATHLEN + 8];

  // Verify csName and get its size
  if (Func == XrdSfsFileSystem::csSize)
  {
    if ((strlen(csName) == strlen("adler32")) &&
        (strncmp(csName, "adler32", strlen(csName)) == 0))
    {
      error.setErrCode(4);
      return SFS_OK;
    }
    else
    {
      strcpy(buff, csName);
      strcat(buff, " checksum not supported");
      error.setErrInfo(ENOTSUP, buff);
      return SFS_ERROR;
    }
  }
  else if ((Func == XrdSfsFileSystem::csGet) || (Func == XrdSfsFileSystem::csCalc))
  {
    struct Cns_filestatcs fstat;
    XrdOucString map_path = XrdxCastor2Fs::NsMapping(path);

    if (map_path == "")
    {
      error.setErrInfo(ENOMEDIUM, "no mapping for file");
      return SFS_ERROR;
    }

    if (!XrdxCastor2FsUFS::Statfn(map_path.c_str(), &fstat))
    {
      // Return precomputed checksum value
      strncpy(buff, fstat.csumvalue, strlen(fstat.csumvalue) + 1);
      error.setErrInfo(0, buff);
      return SFS_OK;
    }
    else
    {
      error.setErrInfo(EINVAL, "no such file");
      return SFS_ERROR;
    }
  }

  error.setErrInfo(ENOTSUP, "operation not supported");
  return SFS_ERROR;
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
  AUTHORIZE(client, &exists_Env, AOP_Stat, "execute exists", path, error);
  map_path = XrdxCastor2Fs::NsMapping(path);
  RoleMap(client, info, mappedclient, tident);

  if (map_path == "")
  {
    error.setErrInfo(ENOMEDIUM, "No mapping for file name");
    return SFS_ERROR;
  }

  if (XrdxCastor2FS->Proc)
  {
    XrdxCastor2FS->Stats.IncCmd();
  }

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
    if (S_ISDIR(fstat.filemode)) file_exists = XrdSfsFileExistIsDirectory;
    else if (S_ISREG(fstat.filemode)) file_exists = XrdSfsFileExistIsFile;
    else file_exists = XrdSfsFileExistNo;

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
  map_path = XrdxCastor2Fs::NsMapping(path);
  RoleMap(client, info, mappedclient, tident);

  if (map_path == "")
  {
    error.setErrInfo(ENOMEDIUM, "No mapping for file name");
    return SFS_ERROR;
  }

  if (XrdxCastor2FS->Proc)
  {
    XrdxCastor2FS->Stats.IncCmd();
  }

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
  {
    SetAcl(path, (*client), 0);
  }

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
        if ((XrdxCastor2FS->_mkdir(actual_path, mode, (*error), client, info)))
        {
          return -serrno;
        }
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
  {
    sinfo.erase(qpos);
  }

  info = (ininfo ? sinfo.c_str() : ininfo);

  if (info)
  {
    xcastor_debug("path=%s, info=%s", path, info);
  }
  else
  {
    xcastor_debug("path=%s", path);
  }

  XrdOucEnv Open_Env(info);
  xcastor::Timing preparetiming("stageprepare");
  map_path = XrdxCastor2Fs::NsMapping(path);

  if (map_path == "")
  {
    error.setErrInfo(ENOMEDIUM, "No mapping for file name");
    return SFS_ERROR;
  }

  RoleMap(client, info, mappedclient, tident);
  struct Cns_filestatcs cstat;

  if (XrdxCastor2FsUFS::Statfn(map_path.c_str(), &cstat))
  {
    return XrdxCastor2Fs::Emsg(epname, error, serrno, "stat", map_path.c_str());
  }

  uid_t client_uid;
  gid_t client_gid;
  GetId(map_path.c_str(), mappedclient, client_uid, client_gid);
  XrdOucString stagehost = "";
  XrdOucString serviceclass = "default";
  XrdOucString stagestatus = "";
  XrdOucString stagevariables = "";

  if (!XrdxCastor2FS->GetStageVariables(map_path.c_str(), info, stagevariables, client_uid, client_gid, tident))
  {
    return XrdxCastor2Fs::Emsg(epname, error, EINVAL, "set the stager host - "
                               "you didn't specify it and I have no stager map for fn = ", map_path.c_str());
  }

  int n = 0;
  const char* val;

  // Try with the stagehost from the environment
  if ((val = Open_Env.Get("stagerHost")))
  {
    stagehost = val;
  }

  if ((val = Open_Env.Get("svcClass")))
  {
    serviceclass = val;
  }

  if (stagehost.length() && serviceclass.length())
  {
    // Try the user specified pair
    if ((XrdxCastor2FS->SetStageVariables(map_path.c_str(), info, stagevariables,
                                          stagehost, serviceclass,
                                          XCASTOR2FS_EXACT_MATCH, tident)) == 1)
    {
      // Select the policy
      XrdOucString* policy = NULL;
      XrdOucString* wildcardpolicy = NULL;
      XrdOucString policytag = stagehost;
      policytag += "::";
      policytag += serviceclass;
      XrdOucString grouppolicytag = policytag;
      grouppolicytag += "::gid:";
      grouppolicytag += (int)client_gid;
      XrdOucString userpolicytag  = policytag;
      userpolicytag += "::uid:";
      userpolicytag += (int)client_uid;
      XrdOucString wildcardpolicytag = stagehost;
      wildcardpolicytag += "::";
      wildcardpolicytag += "*";
      wildcardpolicy = XrdxCastor2FS->stagerpolicy->Find(wildcardpolicytag.c_str());

      if (!(policy = XrdxCastor2FS->stagerpolicy->Find(userpolicytag.c_str())))
        if (!(policy = XrdxCastor2FS->stagerpolicy->Find(grouppolicytag.c_str())))
          policy = XrdxCastor2FS->stagerpolicy->Find(policytag.c_str());

      if ((!policy) && (wildcardpolicy))
      {
        policy = wildcardpolicy;
      }

      if (policy && (strstr(policy->c_str(), "nostage")))
      {
        return XrdxCastor2Fs::Emsg(epname, error, EPERM, "do prepare request - "
                                   "you are not allowed to do stage requests fn = ", map_path.c_str());
      }
    }
    else
    {
      return XrdxCastor2Fs::Emsg(epname, error, EINVAL, "write - cannot find any"
                                 " valid stager/service class mapping for you for fn = ", map_path.c_str());
    }
  }
  else
  {
    // Try the list and stop when we find a matching policy tag
    bool allowed = false;
    int hasmore = 0;

    do
    {
      if ((hasmore = XrdxCastor2FS->SetStageVariables(map_path.c_str(), info, stagevariables,
                     stagehost, serviceclass, n++, tident)) == 1)
      {
        // Select the policy
        XrdOucString* policy = NULL;
        XrdOucString* wildcardpolicy = NULL;
        XrdOucString policytag = stagehost;
        policytag += "::";
        policytag += serviceclass;
        XrdOucString grouppolicytag = policytag;
        grouppolicytag += "::gid:";
        grouppolicytag += (int)client_gid;
        XrdOucString userpolicytag  = policytag;
        userpolicytag += "::uid:";
        userpolicytag += (int)client_uid;
        XrdOucString wildcardpolicytag = stagehost;
        wildcardpolicytag += "::";
        wildcardpolicytag += "*";
        wildcardpolicy = XrdxCastor2FS->stagerpolicy->Find(wildcardpolicytag.c_str());

        if (!(policy = XrdxCastor2FS->stagerpolicy->Find(userpolicytag.c_str())))
          if (!(policy = XrdxCastor2FS->stagerpolicy->Find(grouppolicytag.c_str())))
            policy = XrdxCastor2FS->stagerpolicy->Find(policytag.c_str());

        if ((!policy) && (wildcardpolicy))
        {
          policy = wildcardpolicy;
        }

        if (policy && (!strstr(policy->c_str(), "nostage")))
        {
          allowed = true;
          break;
        }
      }
    }
    while ((n < 20) && (hasmore >= 0));

    if (!allowed)
    {
      return XrdxCastor2Fs::Emsg(epname, error, EPERM, "do prepare request - "
                                 "you are not allowed to do stage requests fn = ", map_path.c_str());
    }
  }

  // Here we have the allowed stager/service class setting to issue the prepare request
  TIMING("STAGERQUERY", &preparetiming);
  //XrdOucString dummy1;
  //XrdOucString dummy2;
  struct XrdxCastor2Stager::RespInfo resp_info;

  if (!XrdxCastor2Stager::Prepare2Get(error, (uid_t) client_uid, (gid_t) client_gid, 
                                      map_path.c_str(), stagehost.c_str(), serviceclass.c_str(), 
                                      resp_info))
  {
    TIMING("END", &preparetiming);
    
    if (XrdxCastor2FS->mLogLevel == LOG_DEBUG)
      preparetiming.Print();

    return SFS_ERROR;
  }

  TIMING("END", &preparetiming);
  
  if (XrdxCastor2FS->mLogLevel == LOG_DEBUG)
    preparetiming.Print();

  return SFS_OK;
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

  if (XrdxCastor2FS->Proc)
  {
    XrdxCastor2FS->Stats.IncCmd();
  }

  xcastor_debug("checking tident=%s, hostname=%s", tident, hostname.c_str());
  XrdxCastor2FS->filesystemhosttablelock.Lock();

  if (!filesystemhosttable->Find(hostname.c_str()))
  {
    XrdxCastor2FS->filesystemhosttablelock.UnLock();
    // This is not a trusted disk server, assume the 'traditional' xrootd prepare
    XrdOucTList* tp = pargs.paths;
    XrdOucTList* op = pargs.oinfo;

    // Run through the paths to make sure client can read each one
    while (tp)
    {
      int retc;

      if ((retc = stageprepare(tp->text, error, client, op ? op->text : NULL)) != SFS_OK)
      {
        return retc;
      }

      if (tp) tp = tp->next;
      else break;

      if (op) op = op->next;
    }

    return SFS_OK;
  }

  XrdxCastor2FS->filesystemhosttablelock.UnLock();

  if (! pargs.paths->text || !pargs.oinfo->text)
  {
    xcastor_debug("ignoring prepare request");
    return SFS_OK;
  }

  xcastor_debug("updating opaque=%s", pargs.oinfo->text);
  XrdOucEnv oenv(pargs.oinfo->text);
  const char* path = pargs.paths->text;
  XrdOucString map_path;
  map_path = XrdxCastor2Fs::NsMapping(path);
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
    {
      return XrdxCastor2Fs::Emsg(epname, error, serrno, "unlink", map_path.c_str());
    }
  }

  TIMING("END", &preparetiming);
  
  if (XrdxCastor2FS->mLogLevel == LOG_DEBUG)
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
  AUTHORIZE(client, &env, AOP_Delete, "remove", path, error);
  map_path = XrdxCastor2Fs::NsMapping(path);
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

  if (XrdxCastor2FS->Proc)
  {
    XrdxCastor2FS->Stats.IncRm();
  }

  TIMING("Authorize", &rmtiming);

  if (env.Get("stagerm"))
  {
    XrdOucString stagevariables = "";
    XrdOucString stagehost = "";
    XrdOucString serviceclass = "";

    if (!XrdxCastor2FS->GetStageVariables(map_path.c_str(), info, stagevariables, client_uid, client_gid, tident))
    {
      return XrdxCastor2Fs::Emsg(epname, error, EINVAL, "set the stager host - "
                                 "add remove opaque tag stagerm=<..> or specify a "
                                 "valid one because I have no stager map for fn = ", map_path.c_str());
    }

    int n = 0;
    char* val = 0;

    // Try with the stagehost from the environment
    if ((val = env.Get("stagerHost")))
    {
      stagehost = val;
    }

    if ((val = env.Get("svcClass")))
    {
      serviceclass = val;
    }

    if (stagehost.length() && serviceclass.length())
    {
      // Try the user specified pair
      if ((XrdxCastor2FS->SetStageVariables(map_path.c_str(), info, stagevariables,
                                            stagehost, serviceclass,
                                            XCASTOR2FS_EXACT_MATCH, tident)) == 1)
      {
        // Select the policy
        XrdOucString* policy = NULL;
        XrdOucString* wildcardpolicy = NULL;
        XrdOucString policytag = stagehost;
        policytag += "::";
        policytag += serviceclass;
        XrdOucString grouppolicytag = policytag;
        grouppolicytag += "::gid:";
        grouppolicytag += (int)client_gid;
        XrdOucString userpolicytag  = policytag;
        userpolicytag += "::uid:";
        userpolicytag += (int)client_uid;
        XrdOucString wildcardpolicytag = stagehost;
        wildcardpolicytag += "::";
        wildcardpolicytag += "*";
        wildcardpolicy = XrdxCastor2FS->stagerpolicy->Find(wildcardpolicytag.c_str());

        if (!(policy = XrdxCastor2FS->stagerpolicy->Find(userpolicytag.c_str())))
          if (!(policy = XrdxCastor2FS->stagerpolicy->Find(grouppolicytag.c_str())))
            policy = XrdxCastor2FS->stagerpolicy->Find(policytag.c_str());

        if ((!policy) && (wildcardpolicy))
        {
          policy = wildcardpolicy;
        }

        if (policy && (strstr(policy->c_str(), "nodelete")))
        {
          return XrdxCastor2Fs::Emsg(epname, error, EPERM, "do stage_rm request - "
                                     "you are not allowed to do stage_rm requests fn = ", map_path.c_str());
        }
      }
      else
      {
        return XrdxCastor2Fs::Emsg(epname, error, EINVAL, "write - cannot find any "
                                   "valid stager/service class mapping for you for fn = ", map_path.c_str());
      }
    }
    else
    {
      // Try the list and stop when we find a matching policy tag
      bool allowed = false;
      int hasmore = 0;

      do
      {
        if ((hasmore = XrdxCastor2FS->SetStageVariables(map_path.c_str(), info, stagevariables,
                       stagehost, serviceclass, n++, tident)) == 1)
        {
          // Select the policy
          XrdOucString* policy = NULL;
          XrdOucString* wildcardpolicy = NULL;
          XrdOucString policytag = stagehost;
          policytag += "::";
          policytag += serviceclass;
          XrdOucString grouppolicytag = policytag;
          grouppolicytag += "::gid:";
          grouppolicytag += (int)client_gid;
          XrdOucString userpolicytag  = policytag;
          userpolicytag += "::uid:";
          userpolicytag += (int)client_uid;
          XrdOucString wildcardpolicytag = stagehost;
          wildcardpolicytag += "::";
          wildcardpolicytag += "*";
          wildcardpolicy = XrdxCastor2FS->stagerpolicy->Find(wildcardpolicytag.c_str());

          if (!(policy = XrdxCastor2FS->stagerpolicy->Find(userpolicytag.c_str())))
            if (!(policy = XrdxCastor2FS->stagerpolicy->Find(grouppolicytag.c_str())))
              policy = XrdxCastor2FS->stagerpolicy->Find(policytag.c_str());

          if ((!policy) && (wildcardpolicy))
          {
            policy = wildcardpolicy;
          }

          if (policy && (!strstr(policy->c_str(), "nodelete")))
          {
            allowed = true;
            break;
          }
        }
      }
      while ((n < 20) && (hasmore >= 0));

      if (!allowed)
      {
        return XrdxCastor2Fs::Emsg(epname, error, EPERM, "do stage_rm request - "
                                   "you are not allowed to do stage_rm requests for fn = ", map_path.c_str());
      }
    }

    // Here we have the allowed stager/service class setting to issue the stage_rm request
    TIMING("STAGERRM", &rmtiming);

    if (!XrdxCastor2Stager::Rm(error, (uid_t) client_uid,
                               (gid_t) client_gid, map_path.c_str(),
                               stagehost.c_str(), serviceclass.c_str()))
    {
      TIMING("END", &rmtiming);
      
      if (XrdxCastor2FS->mLogLevel == LOG_DEBUG)
        rmtiming.Print();
      
      return SFS_ERROR;
    }

    // The stage_rm worked, let's proceed with the namespace
  }

  int retc = 0;
  retc = _rem(map_path.c_str(), error, &mappedclient, info);
  TIMING("RemoveNamespace", &rmtiming);
  
  if (XrdxCastor2FS->mLogLevel == LOG_DEBUG)
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
  {
    return XrdxCastor2Fs::Emsg(epname, error, serrno, "remove", path);
  }

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
  AUTHORIZE(client, &remdir_Env, AOP_Delete, "remove", path, error);
  map_path = XrdxCastor2Fs::NsMapping(path);
  RoleMap(client, info, mappedclient, tident);

  if (map_path == "")
  {
    error.setErrInfo(ENOMEDIUM, "No mapping for file name");
    return SFS_ERROR;
  }

  if (XrdxCastor2FS->Proc)
  {
    XrdxCastor2FS->Stats.IncCmd();
  }

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
  AUTHORIZE(client, &renameo_Env, AOP_Update, "rename", old_name, error);
  AUTHORIZE(client, &renamen_Env, AOP_Update, "rename", new_name, error);
  {
    oldn = XrdxCastor2Fs::NsMapping(old_name);

    if (oldn == "")
    {
      error.setErrInfo(ENOMEDIUM, "No mapping for file name");
      return SFS_ERROR;
    }
  }
  {
    newn = XrdxCastor2Fs::NsMapping(new_name);

    if (newn == "")
    {
      error.setErrInfo(ENOMEDIUM, "No mapping for file name");
      return SFS_ERROR;
    }
  }
  RoleMap(client, infoO, mappedclient, tident);

  if (XrdxCastor2FS->Proc)
  {
    XrdxCastor2FS->Stats.IncCmd();
  }

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
  {
    return XrdxCastor2Fs::Emsg(epname, error, serrno, "rename", oldn.c_str());
  }

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
  TIMING("START", &stattiming);
  xcastor_debug("path=%s", path);
  AUTHORIZE(client, &Open_Env, AOP_Stat, "stat", path, error);
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
  {
    return XrdxCastor2Fs::Emsg(epname, error, serrno, "stat", map_path.c_str());
  }

  uid_t client_uid;
  uid_t client_gid;
  GetId(map_path.c_str(), mappedclient, client_uid, client_gid);
  XrdOucString stagehost = "";
  XrdOucString serviceclass = "default";
  XrdOucString stagestatus = "";
  XrdOucString stagevariables = "";

  if (!(Open_Env.Get("nostagerquery")))
  {
    if (!S_ISDIR(cstat.filemode))
    {
      if (!XrdxCastor2FS->GetStageVariables(map_path.c_str(), info, stagevariables, client_uid, client_gid, tident))
      {
        return XrdxCastor2Fs::Emsg(epname, error, EINVAL, "set the stager host - "
                                   "you didn't specify it and I have no stager map for fn = ", map_path.c_str());
      }

      // Now loop over all possible stager/svc combinations and to find an online one
      int n = 0;
      // Try the list and stop when we find a matching policy tag
      int hasmore = 0;

      do
      {
        stagestatus = "NA";

        if ((hasmore = XrdxCastor2FS->SetStageVariables(map_path.c_str(), info, stagevariables,
                       stagehost, serviceclass, n++, tident)) == 1)
        {
          // We don't care about policies for stat's
          XrdOucString qrytag = "STAGERQUERY-";
          qrytag += n;
          TIMING(qrytag.c_str(), &stattiming);

          if (!XrdxCastor2Stager::StagerQuery(error, (uid_t) client_uid,
                                              (gid_t) client_gid, map_path.c_str(), stagehost.c_str(),
                                              serviceclass.c_str(), stagestatus))
          {
            stagestatus = "NA";
          }

          if ((stagestatus == "STAGED") ||
              (stagestatus == "CANBEMIGR") ||
              (stagestatus == "STAGEOUT"))
          {
            break;
          }
        }
      }
      while ((n < 20) && (hasmore >= 0));
    }
  }

  if (XrdxCastor2FS->Proc)
  {
    XrdxCastor2FS->Stats.IncStat();
  }

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
    if ((stagestatus != "STAGED") &&
        (stagestatus != "CANBEMIGR") &&
        (stagestatus != "STAGEOUT"))
    {
      // This file is offline
      buf->st_mode = static_cast<mode_t>(0);
      buf->st_dev  = 0;
      buf->st_ino  = 0; 
    }

    xcastor_debug("map_path=%s, stagestatus=%s", map_path.c_str(), stagestatus.c_str());
  }

  TIMING("END", &stattiming);

  if (XrdxCastor2FS->mLogLevel == LOG_DEBUG)
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
  AUTHORIZE(client, &lstat_Env, AOP_Stat, "lstat", path, error);
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
  {
    return XrdxCastor2Fs::Emsg(epname, error, serrno, "lstat", map_path.c_str());
  }

  if (XrdxCastor2FS->Proc)
  {
    XrdxCastor2FS->Stats.IncStat();
  }

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

  if (XrdxCastor2FS->mLogLevel == LOG_DEBUG)
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
  AUTHORIZE(client, &rl_Env, AOP_Stat, "readlink", path, error);
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
  {
    return XrdxCastor2Fs::Emsg(epname, error, serrno, "readlink", map_path.c_str());
  }

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
  AUTHORIZE(client, &sl_Env, AOP_Create, "symlink", linkpath, error);
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
  {
    return XrdxCastor2Fs::Emsg(epname, error, serrno, "symlink", source.c_str());
  }

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
  AUTHORIZE(client, &access_Env, AOP_Stat, "access", path, error);
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
  AUTHORIZE(client, &utimes_Env, AOP_Update, "set utimes", path, error);
  map_path = XrdxCastor2Fs::NsMapping(path);

  if (map_path == "")
  {
    error.setErrInfo(ENOMEDIUM, "No mapping for file name");
    return SFS_ERROR;
  }

  RoleMap(client, info, mappedclient, tident);

  if ((XrdxCastor2FsUFS::Utimes(map_path.c_str(), tvp)))
  {
    return XrdxCastor2Fs::Emsg(epname, error, serrno, "utimes", map_path.c_str());
  }

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
  eDest->Emsg(pfx, buffer);
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
    {
      path.erase(0, 1);
    }

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
    sprintf(locResp, "%s", (char*)XrdxCastor2FS->ManagerLocation.c_str());
    error.setErrInfo(strlen(locResp) + 3, (const char**)Resp, 2);
    xcastor_debug("located at headnode: %s", locResp);
    return SFS_DATA;
  }

  if (cmd != SFS_FSCTL_PLUGIN)
  {
    return SFS_ERROR;
  }

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


//--------------------------------------------------------------------------
//! Set the log level for the manager xrootd server
//--------------------------------------------------------------------------
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
