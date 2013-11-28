/*******************************************************************************
 *                      XrdxCastor2FsConfig.cpp
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
#include "XrdxCastor2Fs.hpp"
/*-----------------------------------------------------------------------------*/
#include <sstream>
/*-----------------------------------------------------------------------------*/
#include "XrdOuc/XrdOucStream.hh"
#include "XrdSys/XrdSysPlugin.hh"
#include "XrdSys/XrdSysDNS.hh"
/*-----------------------------------------------------------------------------*/

//------------------------------------------------------------------------------
// Configure OFS plugin
//------------------------------------------------------------------------------
int XrdxCastor2Fs::Configure(XrdSysError& Eroute)
{
  char* var;
  const char* val;
  int  cfgFD, retc, NoGo = 0;
  XrdOucStream config_stream(&Eroute, getenv("XRDINSTANCE"));
  XrdOucString role = "server";
  bool xcastor2fsdefined = false;
  bool SubscribeCms = false;
  bool authorize = false;
  XrdOucString auth_lib = ""; ///< path to a possible authorizationn library
  Authorization = 0;
  Procfilesystem = "";
  ProcfilesystemSync = false;
  xCastor2FsTargetPort = "1094";
  xCastor2FsDelayRead = 0;
  xCastor2FsDelayWrite = 0;
  long myPort = 0;
  {
    // Borrowed from XrdOfs
    unsigned int myIPaddr = 0;
    char buff[256], *bp;
    int i;
    // Obtain port number we will be using
    myPort = (bp = getenv("XRDPORT")) ? strtol(bp, (char**)NULL, 10) : 0;
    // Establish our hostname and IPV4 address
    HostName = XrdSysDNS::getHostName();

    if (!XrdSysDNS::Host2IP(HostName, &myIPaddr)) myIPaddr = 0x7f000001;

    strcpy(buff, "[::");
    bp = buff + 3;
    bp += XrdSysDNS::IP2String(myIPaddr, 0, bp, 128);
    *bp++ = ']';
    *bp++ = ':';
    sprintf(bp, "%li", myPort);

    for (i = 0; HostName[i] && HostName[i] != '.'; i++);

    HostName[i] = '\0';
    HostPref = strdup(HostName);
    HostName[i] = '.';
    Eroute.Say("=====> xcastor2.hostname: ", HostName, "");
    Eroute.Say("=====> xcastor2.hostpref: ", HostPref, "");
    ManagerLocation = buff;
    ManagerId = HostName;
    ManagerId += ":";
    ManagerId += (int)myPort;
    Eroute.Say("=====> xcastor2.managerid: ", ManagerId.c_str(), "");
  }

  if (!ConfigFN || !*ConfigFN)
    Eroute.Emsg("Config", "Configuration file not specified.");
  else
  {
    // Try to open the configuration file.
    if ((cfgFD = open(ConfigFN, O_RDONLY, 0)) < 0)
      return Eroute.Emsg("Config", errno, "open config file", ConfigFN);
  
    config_stream.Attach(cfgFD);
    
    // Now start reading records until eof.
    while ((var = config_stream.GetMyFirstWord()))
    {
      // Get the all.* configuration values
      if (!strncmp(var, "all.", 4))
      {
        var += 4;

        if (!strcmp("role", var))
        {
          if (!(val = config_stream.GetWord()))
          {
            Eroute.Emsg("Config", "argument for all.role missing.");
            NoGo = 1;
          }
          else
          {
            XrdOucString lrole = val;

            if ((val = config_stream.GetWord()))
            {
              if (!strcmp(val, "if"))
              {
                if ((val = config_stream.GetWord()))
                {
                  if (!strcmp(val, HostName))
                    role = lrole;
      
                  if (!strcmp(val, HostPref))
                    role = lrole;
                }
              }
              else
                role = lrole;
            }
            else
              role = lrole;
          }
        }
      }

      // Get the xcasto2.* configuration values
      if (!strncmp(var, "xcastor2.", 9))
      {
        var += 9;

        // Get the fs name
        if (!strncmp("fs", var, 2))
        {
          if (!(val = config_stream.GetWord()))
          {
            Eroute.Emsg("Config", "argument for fs invalid.");
            NoGo = 1;
            break;
          }
          else
          {
            Eroute.Say("=====> xcastor2.fs: ", val, "");
            xcastor2fsdefined = true;
            xCastor2FsName = val;
          }
        }

        // Get the debug level
        if (!strcmp("loglevel", var))
        {
          if (!(val = config_stream.GetWord()))
          {
            Eroute.Emsg("Config", "argument for debug level invalid set to ERR.");
            mLogLevel = LOG_INFO;
          }
          else
          {
            std::string str_val(val);

            // Log level can be a string or a number
            if (isdigit(str_val[0]))
              mLogLevel = atoi(val);
            else
              mLogLevel = Logging::GetPriorityByString(val);

            Eroute.Say("=====> xcastor2.loglevel: ",
                       Logging::GetPriorityString(mLogLevel), "");
          }
        }

        // Get any debug filter name
        if (!strcmp("debugfilter", var))
        {
          if (!(val = config_stream.GetWord()))
            Eroute.Emsg("Config", "argument for debug filter invalid set to none.");
          else
          {
            Logging::SetFilter(val);
            Eroute.Say("=====> xcastor2.debugfileter: ", val, "");
          }
        }

        // Get the target port
        if (!strcmp("targetport", var))
        {
          if (!(val = config_stream.GetWord()))
          {
            Eroute.Emsg("Config", "argument for fs invalid.");
            NoGo = 1;
          }
          else
          {
            Eroute.Say("=====> xcastor2.targetport: ", val, "");
            xCastor2FsTargetPort = val;
          }
        }

        // Get the namespace mapping
        if (!strncmp("nsmap", var, 5))
        {
          std::string ns_key = "";
          std::string ns_value = "";
          
          if (!(val = config_stream.GetWord()))
          {
            Eroute.Emsg("Config", "argument 1 for nsmap missing");
            NoGo = 1;
          }
          else
            ns_key = val;

          if (!(val = config_stream.GetWord()))
          {
            Eroute.Emsg("Config", "argument 2 for nsmap missing");
            NoGo = 1;
          }
          else
            ns_value = val;

          // Add slash at the end if not present
          if (*ns_key.rbegin() != '/')
            ns_key += '/';

          if (*ns_value.rbegin() != '/')
            ns_value += '/';

          Eroute.Say("=====> xcastor2.nsmap: ", ns_key.c_str(), " -> ", ns_value.c_str());
          mNsMap[ns_key] = ns_value;
        }

        // Get proc location and options
        if (!strcmp("proc", var))
        {
          if (!(val = config_stream.GetWord()))
          {
            Eroute.Emsg("Config", "argument for proc invalid.");
            NoGo = 1;
          }
          else
          {
            Procfilesystem = val;
            Procfilesystem += "/proc";

            while (Procfilesystem.replace("//", "/")) {};

            if ((val = config_stream.GetWord()))
            {
              if (!strncmp(val, "sync", 4))
                ProcfilesystemSync = true;
              else if (!strncmp(val, "async", 5))
                ProcfilesystemSync = false;
              else
              {
                Eroute.Emsg("Config", "argument for proc invalid. Specify xcastor2.proc [sync|async]");
                NoGo = 1;
              }
            }

            if (ProcfilesystemSync)
              Eroute.Say("=====> xcastor2.proc: ", Procfilesystem.c_str(), " sync");
            else
              Eroute.Say("=====> xcastor2.proc: ", Procfilesystem.c_str(), " async");
          }
        }
        
        // Get role map
        if (!strncmp("role", var, 4))
        {
          std::string role_key;
          std::string role_value;
          
          if (!(val = config_stream.GetWord()))
          {
            Eroute.Emsg("Config", "argument for role invalid.");
            NoGo = 1;
          }
          else
            role_key = val;
          
          if (!(val = config_stream.GetWord()))
          {
            Eroute.Emsg("Config", "argument 2 for role missing.");
            NoGo = 1;
          }
          else
            role_value = val;
         
          Eroute.Say("=====> xcastor2.role : ", role_key.c_str(), " -> ", role_value.c_str());
          mRoleMap[role_key] = role_value;
        }
      
        // Capability
        if (!strcmp("capability", var))
        {
          if (!(val = config_stream.GetWord()))
          {
            Eroute.Emsg("Config", "argument 2 for capbility missing. Can be true/lazy/1 or false/0");
            NoGo = 1;
          }
          else
          {
            if ((!(strcmp(val, "true"))) || (!(strcmp(val, "1"))) || (!(strcmp(val, "lazy"))))
              mIssueCapability = true;
            else
            {
              if ((!(strcmp(val, "false"))) || (!(strcmp(val, "0"))))
                mIssueCapability = false;
              else
              {
                Eroute.Emsg("Config", "argument 2 for capbility invalid. Can be <true>/1 or <false>/0");
                NoGo = 1;
              }
            }
          }
        }

        // Get grace period for clients
        if (!strcmp("tokenlocktime", var))
        {
          if (!(val = config_stream.GetWord()))
          {
            Eroute.Emsg("Config", "argument 2 for tokenlocktime missing. "
                        "Specifies the grace period for a client to show up "
                        "with a write on a disk server in seconds.");
            NoGo = 1;
          }
          else
          {
            msTokenLockTime = atoi(val);
            
            if (msTokenLockTime == 0)
            {
              Eroute.Emsg("Config", "argument 2 for tokenlocktime is 0/illegal. " 
                          "Specify the grace period for a client to show up with "
                          "a write on a disk server in seconds.");
              NoGo = 1;
            }
          }
        }
        
        // Get stager map configuration
        if (!strcmp("stagermap", var))
        {
          if ((!(val = config_stream.GetWord())))
          {
            xcastor_err("stagermap: provide a path and the service class(es) to assign");
            NoGo = 1;
          }
          else
          {
            std::string stage_path = val;
            
            if ((!(val = config_stream.GetWord())))
            {
              xcastor_err("stagermap: provide a path and the service class(es) to assign");
              NoGo = 1;
            }
            else
            {
              if (mStageMap.find(stage_path) == mStageMap.end())
              {
                std::string list_svc = val;
                std::istringstream sstr(list_svc);
                std::set<std::string> set_svc;
                std::string svc;
                list_svc = "";
                
                while (std::getline(sstr, svc, ','))
                {
                  std::pair<std::set<std::string>::iterator, bool> res = set_svc.insert(svc);
                  
                  if (res.second)
                    list_svc += svc + ',';
                }

                mStageMap[stage_path] = set_svc;
                xcastor_info("stagermap: %s -> %s", stage_path.c_str(), list_svc.c_str());
              }
              else
                xcastor_err("stagermap: already contains stage path: %s", stage_path.c_str());
            }
          }
        }

        //
        if (!strcmp("subscribe", var))
        {
          if ((!(val = config_stream.GetWord())) || (strcmp("true", val) && strcmp("false", val) && strcmp("1", val) && strcmp("0", val)))
          {
            Eroute.Emsg("Config", "argument 2 for subscribe illegal or missing. Must be <true>,<false>,<1> or <0>!");
            NoGo = 1;
          }
          else
          {
            if ((!strcmp("true", val) || (!strcmp("1", val))))
              SubscribeCms = true;
          }
          
          if (SubscribeCms)
            Eroute.Say("=====> xcastor2.subscribe : true");
          else
            Eroute.Say("=====> xcastor2.subscribe : false");
        }
        
        //
        if (!strcmp("authlib", var))
        {
          if ((!(val = config_stream.GetWord())) || (::access(val, R_OK)))
          {
            Eroute.Emsg("Config", "I cannot acccess you authorization library!");
            NoGo = 1;
          }
          else
            auth_lib = val;
          
          Eroute.Say("=====> xcastor2.authlib : ", auth_lib.c_str());
        }
        
        //
        if (!strcmp("authorize", var))
        {
          if ((!(val = config_stream.GetWord())) || (strcmp("true", val) && strcmp("false", val) && strcmp("1", val) && strcmp("0", val)))
          {
            Eroute.Emsg("Config", "argument 2 for authorize ilegal or missing. Must be <true>,<false>,<1> or <0>!");
            NoGo = 1;
          }
          else
          {
            if ((!strcmp("true", val) || (!strcmp("1", val))))
              authorize = true;
          }

          if (authorize)
            Eroute.Say("=====> xcastor2.authorize : true");
          else
            Eroute.Say("=====> xcastor2.authorize : false");
        }
      }
    }
  }

  // Check if xcastor2fs has been set
  if (!xcastor2fsdefined)
  {
    Eroute.Say("Config error: no xcastor2 fs has been defined (xcastor2.fs /...)", "", "");
    NoGo = 1;
  }
  else
    Eroute.Say("=====> xcastor2.fs: ", xCastor2FsName.c_str(), "");

  // We need to specify this if the server was not started with the explicit
  // manager option ... e.g. see XrdOfs. The variable is needed in the
  // XrdxCastor2ServerAcc to find out which keys are required to be loaded
  Eroute.Say("=====> all.role: ", role.c_str(), "");
  XrdOucString sTokenLockTime = "";
  sTokenLockTime += msTokenLockTime;
  Eroute.Say("=====> xcastor2.tokenlocktime: ", sTokenLockTime.c_str(), "");

  if (role == "manager")
    putenv((char*)"XRDREDIRECT=R");

  if ((auth_lib != "") && (authorize))
  {
    // Load the authorization plugin
    XrdSysPlugin* myLib;
    XrdAccAuthorize *(*ep)(XrdSysLogger*, const char*, const char*);
    // Authorization comes from the library or we use the default
    Authorization = XrdAccAuthorizeObject(Eroute.logger(), ConfigFN, NULL);

    if (!(myLib = new XrdSysPlugin(&Eroute, auth_lib.c_str())))
    {
      Eroute.Emsg("Config", "Failed to load authorization library!");
      NoGo = 1;
    }
    else
    {
      ep = (XrdAccAuthorize * (*)(XrdSysLogger*, const char*, const char*))
           (myLib->getPlugin("XrdAccAuthorizeObject"));

      if (!ep)
      {
        Eroute.Emsg("Config", "Failed to get authorization library plugin!");
        NoGo = 1;
      }
      else
      {
        Authorization = ep(Eroute.logger(), ConfigFN, NULL);
      }
    }
  }

  if (Procfilesystem != "")
  {
    // Create the proc directories
    XrdOucString makeProc;
    makeProc = "mkdir -p ";
    makeProc += Procfilesystem;
#ifdef XFS_SUPPORT
    makeProc += "/xfs";
#endif
    system(makeProc.c_str());
    // Create empty file in proc directory
    zeroProc = "";
    zeroProc = "rm -rf ";
    zeroProc += Procfilesystem;
    zeroProc += "/zero";
    zeroProc += "; touch ";
    zeroProc += Procfilesystem;
    zeroProc += "/zero";
    system(zeroProc.c_str());
    // Store the filename in zeroProc
    zeroProc = Procfilesystem;
    zeroProc += "/zero";
  }

  // Create the proc object, if a proc fs was specified
  if (Procfilesystem != "")
  {
    Proc = new XrdxCastor2Proc(Procfilesystem.c_str(), ProcfilesystemSync);

    if (Proc)
    {
      if (Proc->Open())
      {
        time_t now = time(NULL);
        pthread_t tid;
        int rc;
        // Store version
        XrdxCastor2ProcFile* pf;
        XrdOucString ps = "1.1";
        // TODO: PACKAGE_STRING;
        ps += "\n";
        pf = Proc->Handle("version");
        pf && pf->Write(ps.c_str());
        pf = Proc->Handle("start");
        pf && pf->Write(ctime(&now));
        // We have to give the proc pointer to the Stats class
        Stats.SetProc(Proc);

        if ((rc = XrdSysThread::Run(&tid,
                                    XrdxCastor2FsStatsStart,
                                    static_cast<void*>(&(this->Stats)),
                                    0, "Stats Updater")))
        {
          Eroute.Emsg("Config", "cannot run stats update thread");
          NoGo = 1;
        }
      }
      else
      {
        Eroute.Emsg("Config", "cannot create proc fs");
      }
    }
    else
    {
      Eroute.Emsg("Config", "cannot get proc fs");
    }
  }
  else
  {
    Eroute.Say("=====> xcastor2.proc: ", "unspecified", "");
  }

  if ((retc = config_stream.LastError()))
    NoGo = Eroute.Emsg("Config", -retc, "read config file", ConfigFN);

  config_stream.Close();
  return NoGo;
}
