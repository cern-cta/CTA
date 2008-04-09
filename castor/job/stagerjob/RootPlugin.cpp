/******************************************************************************
 *                      RootPlugin.cpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
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
 * @(#)$RCSfile$ $Revision$ $Release$ $Date$ $Author$
 *
 * Plugin of the stager job concerning Root
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include <errno.h>
#include <string>
#include <sstream>
#include "getconfent.h"
#include "castor/dlf/Dlf.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/job/stagerjob/RootPlugin.hpp"

// static instance of the RootPlugin
castor::job::stagerjob::RootPlugin rootPlugin;

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::job::stagerjob::RootPlugin::RootPlugin() throw():
  RawMoverPlugin("root") {
}

//------------------------------------------------------------------------------
// postForkHook
//------------------------------------------------------------------------------
void castor::job::stagerjob::RootPlugin::postForkHook
(InputArguments &args, PluginContext &context)
  throw(castor::exception::Exception) {
  // get ROOTSYS
  const char *rootsys_default = "/usr/local/bin";
  const char *rootsys = getenv("ROOTSYS");
  if (rootsys == NULL) {
    rootsys = getconfent("ROOT","ROOTSYS",0);
    if (rootsys == NULL) {
      rootsys = rootsys_default;
    }
  }
  // Build the command line
  std::string progfullpath = rootsys;
  progfullpath += "/rootd";
  std::ostringstream cmdLine;
  cmdLine << "Executed " << progfullpath << " -i -d 0 "
          << " -F " << context.fullDestPath
          << " -H " << args.rawRequestUuid
          << " (pid=" << context.childPid << ")";
  // "Mover fork uses the following command line"
  castor::dlf::Param params[] =
    {castor::dlf::Param("JobId", getenv("LSB_JOBID")),
     castor::dlf::Param("command line", cmdLine.str()),
     castor::dlf::Param("TotalWaitTime", context.totalWaitTime),
     castor::dlf::Param(args.subRequestUuid)};
  castor::dlf::dlf_writep(args.requestUuid, DLF_LVL_DEBUG,
                          MOVERFORK, 4, params, &args.fileId);
  // check that the mover can be executed
  if (access(progfullpath.c_str(), X_OK) != 0) {
    // "Mover program can not be executed. Check permissions"
    castor::dlf::Param params[] =
      {castor::dlf::Param("JobId", getenv("LSB_JOBID")),
       castor::dlf::Param("mover path", progfullpath),
       castor::dlf::Param(args.subRequestUuid)};
    castor::dlf::dlf_writep(args.requestUuid, DLF_LVL_ERROR,
                            MOVERNOTEXEC, 3, params, &args.fileId);
  }
  // Call upper level
  RawMoverPlugin::postForkHook(args, context);
}

//------------------------------------------------------------------------------
// execMover
//------------------------------------------------------------------------------
void castor::job::stagerjob::RootPlugin::execMover
(InputArguments &args, PluginContext &context)
  throw(castor::exception::Exception) {
  // get ROOTSYS and set the environment
  const char *rootsys_default = "/usr/local/bin";
  const char *rootsys = getenv("ROOTSYS");
  if (rootsys == NULL) {
    rootsys = getconfent("ROOT","ROOTSYS",0);
    if (rootsys == NULL) {
      rootsys = rootsys_default;
    }
  }
  setenv("ROOTSYS", rootsys, 1);
  std::string progfullpath = rootsys;
  progfullpath += "/rootd";
  std::string progname = "rootd";
  if (access(progfullpath.c_str(), X_OK) != 0) {
    dlf_shutdown(5);
    exit(EXIT_FAILURE);
  }
  // Duplicate socket on stdin/stdout/stdout and close the others
  close(0);
  close(1);
  close(2);
  int s;
  if (dup2(s, 0) < 0 || dup2(s, 1) < 0 || dup2(s, 2) < 0) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("errorCode", errno),
       castor::dlf::Param("errorMessage", strerror(errno)),
       castor::dlf::Param(args.subRequestUuid)};
    castor::dlf::dlf_writep
      (args.requestUuid, DLF_LVL_ERROR,
       castor::job::stagerjob::DUP2FAILED, 3, params, &args.fileId);
    dlf_shutdown(5);
    exit(EXIT_FAILURE);
  }
  // execute mover
  execl (progfullpath.c_str(), progname.c_str(),
         "-i", "-d", "0", "-F", context.fullDestPath.c_str(),
         "-H", args.requestUuid, NULL);
  // Should never be reached
  dlf_shutdown(5);
  exit(EXIT_FAILURE);
}
