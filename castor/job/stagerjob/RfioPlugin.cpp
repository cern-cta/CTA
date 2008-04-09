/******************************************************************************
 *                      RfioPlugin.cpp
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
 * Plugin of the stager job concerning Rfio
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include <string>
#include <sstream>
#include "getconfent.h"
#include "castor/dlf/Dlf.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/job/stagerjob/RfioPlugin.hpp"

// static instance of the RfioPlugin
castor::job::stagerjob::RfioPlugin rfioPlugin;

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::job::stagerjob::RfioPlugin::RfioPlugin() throw():
  InstrumentedMoverPlugin("rfio") {
}

//------------------------------------------------------------------------------
// getLogLevel
//------------------------------------------------------------------------------
void castor::job::stagerjob::RfioPlugin::getLogLevel
(std::string &logFile, bool &debug)
  throw() {
  // Get loggin file
  logFile = "/dev/null";
  char* value = getconfent("RFIOD", "LOGFILE", 0);
  if (value != NULL) {
    logFile = strdup(value);
  }
  // get logging level
  debug = false;
  value = getconfent("RFIOD", "DEBUG", 0);
  if (value != NULL) {
    if (!strcasecmp(value, "yes") ||
        !strcasecmp(value, "1")   ||
        !strcasecmp(value, "y")) {
      debug = true;
    }
  }
}

//------------------------------------------------------------------------------
// postForkHook
//------------------------------------------------------------------------------
void castor::job::stagerjob::RfioPlugin::postForkHook
(InputArguments &args, PluginContext &context)
  throw(castor::exception::Exception) {
  // log the mover command line on behalf of the mover process
  // that cannot log
  std::string logFile;
  bool isDebugMode;
  getLogLevel(logFile, isDebugMode);
  // "Mover fork uses the following command line"
  std::string progfullpath = "/usr/bin/rfiod";
  std::ostringstream cmdLine;
  cmdLine << "Executed " << progfullpath << " -1 -";
  if (isDebugMode) cmdLine << "d";
  cmdLine << "slnf " << logFile
          << " -T " << getSelectTimeOut()
          << " -S " << context.socket
          << " -P " << context.port
          << " -M " << context.mask
          << " -U -Z " << args.subRequestId
          << " " << context.fullDestPath
          << " (pid=" << context.childPid << ")";
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
  // and call upper level for the actual work
  InstrumentedMoverPlugin::postForkHook(args, context);
}


//------------------------------------------------------------------------------
// execMover
//------------------------------------------------------------------------------
void castor::job::stagerjob::RfioPlugin::execMover
(InputArguments &args, PluginContext &context)
  throw(castor::exception::Exception) {
  // Build argument list
  std::string progfullpath = "/usr/bin/rfiod";
  std::string progname = "rfiod";
  std::ostringstream arg_S;
  arg_S << context.socket;
  std::ostringstream arg_P;
  arg_P << context.port;
  std::ostringstream arg_M;
  arg_M << context.mask;
  std::ostringstream arg_T;
  arg_T << getSelectTimeOut();
  std::ostringstream arg_Z;
  arg_Z << args.subRequestId;
  std::string logFile;
  bool isDebugMode;
  getLogLevel(logFile, isDebugMode);
  // Call exec
  if (isDebugMode) {
    execl (progfullpath.c_str(), progname.c_str(),
           "-1", "-d", "-s", "-l", "-n", "-f", logFile.c_str(),
           "-T", arg_T.str().c_str(), "-S", arg_S.str().c_str(),
           "-P", arg_P.str().c_str(), "-M", arg_M.str().c_str(),
           "-U", "-Z", arg_Z.str().c_str(),
           context.fullDestPath.c_str(), NULL);
  } else{
    // the flags are the same as in unsecure mode so the stagerJob
    // can work with rfiod unsecure (provisional)
    // if (args.isSecure != 0) {
    execl (progfullpath.c_str(), progname.c_str(),
           "-1", "-s", "-l", "-n", "-f", logFile.c_str(),
           "-T", arg_T.str().c_str(), "-S", arg_S.str().c_str(),
           "-P", arg_P.str().c_str(), "-M", arg_M.str().c_str(),
           "-U", "-Z", arg_Z.str().c_str(),
           context.fullDestPath.c_str(), NULL);
  }
  // Should never be reached
  dlf_shutdown(5);
  exit(EXIT_FAILURE);
}
