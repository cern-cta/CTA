/******************************************************************************
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
 *
 * Plugin of the stager job concerning Root
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

// Include Files
#include <errno.h>
#include <string>
#include <sstream>
#include <unistd.h>
#include "getconfent.h"
#include "castor/dlf/Dlf.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/job/stagerjob/InputArguments.hpp"
#include "castor/job/stagerjob/RootPlugin.hpp"

// Timeout on select. This is the time we'll wait on a root
// client to connect (see also castor.conf)
#define SELECT_TIMEOUT_ROOT 60

// Default port range
#define ROOTDMINPORT 45000
#define ROOTDMAXPORT 46000

// Static instance of the RootPlugin
castor::job::stagerjob::RootPlugin rootPlugin;

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::job::stagerjob::RootPlugin::RootPlugin() throw():
  RawMoverPlugin("root") {
  m_prevFileMtime = 0;
}

//------------------------------------------------------------------------------
// getPortRange
//------------------------------------------------------------------------------
std::pair<int, int> castor::job::stagerjob::RootPlugin::getPortRange
(InputArguments &args, std::string name)
  throw() {
  // Look at the config file
  char* entry = getconfent("ROOT", name.c_str(), 0);
  if (NULL == entry) {
    return std::pair<int, int>(ROOTDMINPORT, ROOTDMAXPORT);
  }
  // Parse min port
  std::istringstream iss(entry);
  std::string value;
  std::getline(iss, value, ',');
  if (iss.fail() || value.empty()) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("RequiredFormat", "min,max"),
       castor::dlf::Param("Found", entry),
       castor::dlf::Param(args.subRequestUuid)};
    castor::dlf::dlf_writep(args.requestUuid, DLF_LVL_ERROR,
                            ROOTDBADPORT, 3, params, &args.fileId);
    return std::pair<int, int>(ROOTDMINPORT, ROOTDMAXPORT);
  }
  int min = ROOTDMINPORT;
  if (value.find_first_not_of("0123456789") != value.npos) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("Found", value),
       castor::dlf::Param(args.subRequestUuid)};
    castor::dlf::dlf_writep(args.requestUuid, DLF_LVL_ERROR,
                            ROOTDBADMINPORT, 2, params, &args.fileId);
  } else {
    // Check min port value
    min = atoi(value.c_str());
    if (min < 1024 || min > 65535) {
      castor::dlf::Param params[] =
        {castor::dlf::Param("Value", min),
         castor::dlf::Param(args.subRequestUuid)};
      castor::dlf::dlf_writep(args.requestUuid, DLF_LVL_ERROR,
                              ROOTDBADMINVAL, 2, params, &args.fileId);
      min = ROOTDMINPORT;
    }
  }
  // Parse max port
  int max = ROOTDMAXPORT;
  std::getline(iss, value);
  if (value.find_first_not_of("0123456789") != value.npos) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("Found", value),
       castor::dlf::Param(args.subRequestUuid)};
    castor::dlf::dlf_writep(args.requestUuid, DLF_LVL_ERROR,
                            ROOTDBADMAXPORT, 2, params, &args.fileId);
  } else {
    max = atoi(value.c_str());
    if (max < 1024 || max > 65535 || max < min) {
      castor::dlf::Param params[] =
        {castor::dlf::Param("Value", max),
         castor::dlf::Param("Min value", min),
         castor::dlf::Param(args.subRequestUuid)};
      castor::dlf::dlf_writep(args.requestUuid, DLF_LVL_ERROR,
                              ROOTDBADMAXVAL, 3, params, &args.fileId);
      max = ROOTDMAXPORT;
    }
  }
  return std::pair<int, int>(min, max);
}

//------------------------------------------------------------------------------
// getPortRange
//------------------------------------------------------------------------------
std::pair<int, int>
castor::job::stagerjob::RootPlugin::getPortRange
(InputArguments &args) throw() {
  return getPortRange(args, "PORT_RANGE");
}

//------------------------------------------------------------------------------
// preForkHook
//------------------------------------------------------------------------------
void castor::job::stagerjob::RootPlugin::preForkHook
(InputArguments &args, PluginContext &context)
   {
  // Set the default time out for select
  char *value = getconfent("ROOT", "TIMEOUT", 0);
  int t = SELECT_TIMEOUT_ROOT;
  if (value) {
    t = strtol(value, 0, 10);
    if (t < 1) {
      // "Invalid value for ROOT/TIMEOUT option, using default"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Value", t),
         castor::dlf::Param("Default", SELECT_TIMEOUT_ROOT),
         castor::dlf::Param(args.subRequestUuid)};
      castor::dlf::dlf_writep(args.requestUuid, DLF_LVL_WARNING,
                              ROOTDBADTIMEOUT, 3, params, &args.fileId);
      t = SELECT_TIMEOUT_ROOT;
    }
  }
  setSelectTimeOut(t);

  // If this is an update make sure that the modification time of the file on
  // disk is older than the current system clock time. This allows us in
  // postForkHook to detect if the client actually made any changes to the
  // file. This is workaround and does not cover all use-cases!!! (See: #29491)
  if (args.accessMode == castor::job::stagerjob::ReadWrite) {
    unsigned int attempts = 0;
    for (attempts = 0; attempts < 60; attempts++) {
      struct stat64 statbuf;
      if (stat64((char *)context.fullDestPath.c_str(), &statbuf) == 0) {
        if (statbuf.st_mtime < time(NULL)) {
          m_prevFileMtime = statbuf.st_mtime;
          break;
        }

        // Sleep 1 second, if after 60 seconds we cannot ensure that the 
        // modification time of the file is in the past we exit the loop. This
        // stops us looping indefinitely.
        sleep(1);
      } else {
        // An error occurred in the stat call. Other than logging an error and
        // exiting the loop there isn't much else we can do.
        if (errno != ENOENT) {
          // "Failed to determine file modification time, file may not be closed
          // properly"
          castor::dlf::Param params[] =
            {castor::dlf::Param("Path", context.fullDestPath),
             castor::dlf::Param("Message", "Failed to stat64()"), 
             castor::dlf::Param("Error", strerror(errno)),
             castor::dlf::Param(args.subRequestUuid)};
          castor::dlf::dlf_writep(args.requestUuid, DLF_LVL_WARNING,
                                  ROOTRDWRSTATERR, 4, params, &args.fileId);
        }
        break;
      }
    }

    // If we exhausted all attempts to get the modification time log a warning
    // message
    if ((m_prevFileMtime == 0) && (attempts == 60)) {
      // "Failed to determine file modification time, file may not be closed
      // properly"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Path", context.fullDestPath),
         castor::dlf::Param("Message", "File is currently being modified"),
         castor::dlf::Param(args.subRequestUuid)};
      castor::dlf::dlf_writep(args.requestUuid, DLF_LVL_WARNING,
                              ROOTRDWRSTATERR, 3, params, &args.fileId);   
    }
  }

  // Call upper level
  RawMoverPlugin::preForkHook(args, context);
}

//------------------------------------------------------------------------------
// postForkHook
//------------------------------------------------------------------------------
void castor::job::stagerjob::RootPlugin::postForkHook
(InputArguments &args, PluginContext &context,
 bool /*useChksSum*/, int /*moverStatus*/)
   {
  // Get ROOTSYS
  const char *rootsys_default = "/usr/local/bin";
  const char *rootsys = getenv("ROOTSYS");
  if (rootsys == NULL) {
    rootsys = getconfent("ROOT", "ROOTSYS", 0);
    if (rootsys == NULL) {
      rootsys = rootsys_default;
    }
  }
  // Build the command line
  std::string progfullpath = rootsys;
  progfullpath += "/rootd";
  std::ostringstream cmdLine;
  cmdLine << progfullpath << " -i -d 0"
          << " -F " << context.fullDestPath
          << " -H " << args.rawRequestUuid
          << " (pid=" << context.childPid << ")";
  // "Mover fork uses the following command line"
  castor::dlf::Param params[] =
    {castor::dlf::Param("Command Line", cmdLine.str()),
     castor::dlf::Param(args.subRequestUuid)};
  castor::dlf::dlf_writep(args.requestUuid, DLF_LVL_DEBUG,
                          MOVERFORK, 2, params, &args.fileId);
  // Check that the mover can be executed
  if (access(progfullpath.c_str(), X_OK) != 0) {
    // "Mover program cannot be executed. Check permissions"
    castor::dlf::Param params[] =
      {castor::dlf::Param("Mover Path", progfullpath),
       castor::dlf::Param(args.subRequestUuid)};
    castor::dlf::dlf_writep(args.requestUuid, DLF_LVL_ERROR,
                            MOVERNOTEXEC, 2, params, &args.fileId);
  }

  // Wait for children
  bool childFailed = waitForChild(args);

  // If the file was opened for update determine if a write was actually
  // performed by comparing the previously known file modification time
  // determined in the preForkHook with the current file modification time.
  if ((args.accessMode == castor::job::stagerjob::ReadWrite) &&
      (m_prevFileMtime > 0)) {
    struct stat64 statbuf;
    if (stat64((char *)context.fullDestPath.c_str(), &statbuf) == 0) {
      if (statbuf.st_mtime > m_prevFileMtime) {
        // "File update detected, changing access mode to write"
        castor::dlf::Param params[] =
          {castor::dlf::Param(args.subRequestUuid)};
        castor::dlf::dlf_writep(args.requestUuid, DLF_LVL_SYSTEM,
                                XROOTFILEUPDATE, 1, params, &args.fileId);
        args.accessMode = castor::job::stagerjob::WriteOnly;
      }
    }
  }

  // Call upper level
  RawMoverPlugin::postForkHook(args, context, childFailed);
}

//------------------------------------------------------------------------------
// execMover
//------------------------------------------------------------------------------
void castor::job::stagerjob::RootPlugin::execMover
(InputArguments &args, PluginContext &context)
   {
  // Get ROOTSYS
  const char *rootsys_default = "/usr/local/bin";
  const char *rootsys = getenv("ROOTSYS");
  if (rootsys == NULL) {
    rootsys = getconfent("ROOT", "ROOTSYS", 0);
    if (rootsys == NULL) {
      rootsys = rootsys_default;
    }
  }
  // Check mover executable
  setenv("ROOTSYS", rootsys, 1);
  std::string progfullpath = rootsys;
  progfullpath += "/rootd";
  std::string progname = "rootd";
  if (access(progfullpath.c_str(), X_OK) != 0) {
    dlf_shutdown();
    exit(EXIT_FAILURE);
  }
  // Duplicate socket on stdin/stdout/stderr and close the others
  if ((dup2(context.socket, 0) < 0) ||
      (dup2(context.socket, 1) < 0) ||
      (dup2(context.socket, 2) < 0)) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("Error Code", errno),
       castor::dlf::Param("Error Message", strerror(errno)),
       castor::dlf::Param(args.subRequestUuid)};
    castor::dlf::dlf_writep
      (args.requestUuid, DLF_LVL_ERROR,
       castor::job::stagerjob::DUP2FAILED, 3, params, &args.fileId);
    dlf_shutdown();
    exit(EXIT_FAILURE);
  }
  // Execute mover
  execl (progfullpath.c_str(), progname.c_str(),
         "-i", "-d", "0", "-F", context.fullDestPath.c_str(),
         "-H", args.rawRequestUuid.c_str(), NULL);
  // Should never be reached
  castor::dlf::Param params[] =
    {castor::dlf::Param("Error Code", errno),
     castor::dlf::Param("Error Message", strerror(errno)),
     castor::dlf::Param(args.subRequestUuid)};
  castor::dlf::dlf_writep(args.requestUuid, DLF_LVL_ERROR,
                          EXECFAILED, 3, params, &args.fileId);
  dlf_shutdown();
  exit(EXIT_FAILURE);
}
