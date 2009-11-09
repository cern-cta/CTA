/******************************************************************************
 *                      GridFTPPlugin.cpp
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
 * Plugin of the stager job concerning GridFTP
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
#include "castor/exception/InvalidArgument.hpp"
#include "castor/job/stagerjob/InputArguments.hpp"
#include "castor/job/stagerjob/GridFTPPlugin.hpp"

// Timeout on select. This is the time we'll wait on a GridFTP
// client to connect (see also castor.conf)
#define SELECT_TIMEOUT_GSIFTP 180

// Default port range
#define GRIDFTPMINPORT 20000
#define GRIDFTPMAXPORT 21000

// Static instance of the GridFTPPlugin
castor::job::stagerjob::GridFTPPlugin gridFTPPlugin;

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::job::stagerjob::GridFTPPlugin::GridFTPPlugin() throw() :
  RawMoverPlugin("gsiftp") {
}

//------------------------------------------------------------------------------
// getPortRange
//------------------------------------------------------------------------------
std::pair<int, int> castor::job::stagerjob::GridFTPPlugin::getPortRange
(InputArguments &args, std::string name)
  throw() {
  // Look at the config file
  char* entry = getconfent("GSIFTP", name.c_str(), 0);
  if (NULL == entry) {
    return std::pair<int, int>(GRIDFTPMINPORT, GRIDFTPMAXPORT);
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
                            GSIBADPORT, 3, params, &args.fileId);
    return std::pair<int, int>(GRIDFTPMINPORT, GRIDFTPMAXPORT);
  }
  int min = GRIDFTPMINPORT;
  if (value.find_first_not_of("0123456789") != value.npos) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("Found", value),
       castor::dlf::Param(args.subRequestUuid)};
    castor::dlf::dlf_writep(args.requestUuid, DLF_LVL_ERROR,
                            GSIBADMINPORT, 2, params, &args.fileId);
  } else {
    // Check min port value
    min = atoi(value.c_str());
    if (min < 1024 || min > 65535) {
      castor::dlf::Param params[] =
        {castor::dlf::Param("Value", min),
         castor::dlf::Param(args.subRequestUuid)};
      castor::dlf::dlf_writep(args.requestUuid, DLF_LVL_ERROR,
                              GSIBADMINVAL, 2, params, &args.fileId);
      min = GRIDFTPMINPORT;
    }
  }
  // Parse max port
  int max = GRIDFTPMAXPORT;
  std::getline(iss, value);
  if (value.find_first_not_of("0123456789") != value.npos) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("Found", value),
       castor::dlf::Param(args.subRequestUuid)};
    castor::dlf::dlf_writep(args.requestUuid, DLF_LVL_ERROR,
                            GSIBADMAXPORT, 2, params, &args.fileId);
  } else {
    max = atoi(value.c_str());
    if (max < 1024 || max > 65535 || max < min) {
      castor::dlf::Param params[] =
        {castor::dlf::Param("Value", max),
         castor::dlf::Param("Min value", min),
         castor::dlf::Param(args.subRequestUuid)};
      castor::dlf::dlf_writep(args.requestUuid, DLF_LVL_ERROR,
                              GSIBADMAXVAL, 3, params, &args.fileId);
      max = GRIDFTPMAXPORT;
    }
  }
  return std::pair<int, int>(min, max);
}

//------------------------------------------------------------------------------
// getPortRange
//------------------------------------------------------------------------------
std::pair<int, int>
castor::job::stagerjob::GridFTPPlugin::getPortRange
(InputArguments &args) throw() {
  return getPortRange(args, "CONTROL_TCP_PORT_RANGE");
}

//------------------------------------------------------------------------------
// getEnvironment
//------------------------------------------------------------------------------
void castor::job::stagerjob::GridFTPPlugin::getEnvironment
(InputArguments &args, Environment &env)
  throw(castor::exception::Exception) {
  // Get the location of globus
  char* globus_location = getenv("GLOBUS_LOCATION");
  if (globus_location == NULL) {
    globus_location = getconfent("GSIFTP", "GLOBUS_LOCATION", 0);
    if (globus_location == NULL) {
      env.globus_location = "/opt/globus";
    } else {
      env.globus_location = globus_location;
    }
  }
  // Get data port range
  env.tcp_port_range = getPortRange(args, "DATA_TCP_PORT_RANGE");
  env.tcp_source_range = getPortRange(args, "DATA_TCP_SOURCE_RANGE");

  // Get log file names and log level
  char *globus_logfile = getconfent("GSIFTP", "LOGFILE", 0);
  if (globus_logfile == NULL) {
    env.globus_logfile = "/var/log/gridftp.log";
  } else {
    env.globus_logfile = globus_logfile;
  }

  const char *globus_logfile_netlogger = getconfent("GSIFTP", "NETLOGFILE", 0);
  if (globus_logfile_netlogger == NULL) {
    env.globus_logfile_netlogger = "/var/log/globus-gridftp.log";
  } else {
    env.globus_logfile_netlogger = globus_logfile_netlogger;
  }

  const char *globus_loglevel = getconfent("GSIFTP", "LOGLEVEL", 0);
  if (globus_loglevel == NULL) {
    env.globus_loglevel = "ALL";
  } else {
    env.globus_loglevel = globus_loglevel;
  }

  // Check whether XROOT should be used to open files (instead of RFIO)
  bool useXroot = false;
  const char *useXrootStr = getconfent("GSIFTP", "USEXROOT", 0);
  if (0 != useXrootStr) {
    useXroot = (strcasecmp(useXrootStr, "YES") == 0);
    if (!useXroot && (strcasecmp(useXrootStr, "NO") != 0)) {
      castor::exception::InvalidArgument e;
      e.getMessage() << "Invalid option for GSIFTP/USEXROOT: '" << useXrootStr
                     << "' - must be 'yes' or 'no'" << std::endl;
      throw e;
    }
  }
  env.dsi_module_extension = (useXroot ? "xroot" : "int");

  // Get the location of xroot
  char* xroot_location = getenv("XROOT_LOCATION");
  if (xroot_location == NULL) {
    xroot_location = getconfent("GSIFTP", "XROOT_LOCATION", 0);
    if (xroot_location == NULL) {
      env.xroot_location = "/opt/xrootd";
    } else {
      env.xroot_location = xroot_location;
    }
  }

  // Get certificate and key file names
  const char *globus_x509_user_cert = getconfent("GSIFTP", "X509_USER_CERT", 0);
  if (globus_x509_user_cert == NULL) {
    env.globus_x509_user_cert = "/etc/grid-security/castor-gridftp-dsi-" +
      env.dsi_module_extension + "/castor-gridftp-dsi-" +
      env.dsi_module_extension + "-cert.pem";
  } else {
    env.globus_x509_user_cert = globus_x509_user_cert;
  }

  const char *globus_x509_user_key = getconfent("GSIFTP", "X509_USER_KEY", 0);
  if (globus_x509_user_key == NULL) {
    env.globus_x509_user_key = "/etc/grid-security/castor-gridftp-dsi-" +
      env.dsi_module_extension + "/castor-gridftp-dsi-" +
      env.dsi_module_extension + "-key.pem";
  } else {
    env.globus_x509_user_key = globus_x509_user_key;
  }

  // Check whether gridftp internal should generate checksum information
  bool useCksum = true;
  const char *useCksumStr = getconfent("GSIFTP", "USE_CKSUM", 0);
  if (0 != useCksumStr) {
    useCksum = (strcasecmp(useCksumStr, "YES") == 0);
    if (!useCksum && (strcasecmp(useCksumStr, "NO") != 0)) {
      castor::exception::InvalidArgument e;
      e.getMessage() << "Invalid option for GSIFTP/USE_CKSUM: '" << useCksumStr
                     << "' - must be 'yes' or 'no'" << std::endl;
      throw e;
    }
  }
  env.use_cksum = (useCksum ? "yes" : "no");
}

//------------------------------------------------------------------------------
// preForkHook
//------------------------------------------------------------------------------
void castor::job::stagerjob::GridFTPPlugin::preForkHook
(InputArguments &args, PluginContext &context)
  throw(castor::exception::Exception) {
  // Set the default time out for select
  char *value = getconfent("GSIFTP", "TIMEOUT", 0);
  int t = SELECT_TIMEOUT_GSIFTP;
  if (value) {
    t = strtol(value, 0, 10);
    if (t < 1) {
      // "Invalid value for GSIFTP/TIMEOUT option, using default"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Value", t),
         castor::dlf::Param("Default", SELECT_TIMEOUT_GSIFTP),
         castor::dlf::Param(args.subRequestUuid)};
      castor::dlf::dlf_writep(args.requestUuid, DLF_LVL_WARNING,
                              GSIBADTIMEOUT, 3, params, &args.fileId);
      t = SELECT_TIMEOUT_GSIFTP;
    }
  }
  setSelectTimeOut(t);

  // Call upper level
  RawMoverPlugin::preForkHook(args, context);
}

//------------------------------------------------------------------------------
// postForkHook
//------------------------------------------------------------------------------
void castor::job::stagerjob::GridFTPPlugin::postForkHook
(InputArguments &args, PluginContext &context)
  throw(castor::exception::Exception) {
  // Get environment
  Environment env;
  getEnvironment(args, env);
  // Build the command line
  std::string progfullpath = env.globus_location + "/sbin/globus-gridftp-server";
  std::ostringstream cmdLine;
  cmdLine << "Executed " << progfullpath
          << " -i -d " << env.globus_loglevel
          << " -control-idle-timeout 3600 -Z "
          << env.globus_logfile_netlogger
          << " -l " << env.globus_logfile
          << " -dsi CASTOR2" << env.dsi_module_extension
          << " -allowed-modules CASTOR2"
          << env.dsi_module_extension << " (pid="
          << context.childPid << ")";
  // "Mover fork uses the following command line"
  std::ostringstream tcprange;
  tcprange << env.tcp_port_range.first << ","
           << env.tcp_port_range.second;
  std::ostringstream sourcerange;
  sourcerange << env.tcp_source_range.first << ","
              << env.tcp_source_range.second;
  castor::dlf::Param params[] =
    {castor::dlf::Param("JobId", getenv("LSB_JOBID")),
     castor::dlf::Param("Command Line", cmdLine.str()),
     castor::dlf::Param("TCP Port Range", tcprange.str()),
     castor::dlf::Param("TCP Source Range", sourcerange.str()),
     castor::dlf::Param("Certificate", env.globus_x509_user_cert),
     castor::dlf::Param("User Key", env.globus_x509_user_key),
     castor::dlf::Param(args.subRequestUuid)};
  castor::dlf::dlf_writep(args.requestUuid, DLF_LVL_DEBUG,
                          MOVERFORK, 7, params, &args.fileId);
  // Check that the mover can be executed
  if (access(progfullpath.c_str(), X_OK) != 0) {
    // "Mover program cannot be executed. Check permissions"
    castor::dlf::Param params[] =
      {castor::dlf::Param("JobId", getenv("LSB_JOBID")),
       castor::dlf::Param("Mover Path", progfullpath),
       castor::dlf::Param(args.subRequestUuid)};
    castor::dlf::dlf_writep(args.requestUuid, DLF_LVL_ERROR,
                            MOVERNOTEXEC, 3, params, &args.fileId);
  }
  // Call upper level
  RawMoverPlugin::postForkHook(args, context, true);
}

//------------------------------------------------------------------------------
// execMover
//------------------------------------------------------------------------------
void castor::job::stagerjob::GridFTPPlugin::execMover
(InputArguments &args, PluginContext &context)
  throw(castor::exception::Exception) {
  // Get environment
  Environment env;
  getEnvironment(args, env);
  // Set some enviroment variables
  setenv("GLOBUS_LOCATION", env.globus_location.c_str(), 1);
  std::ostringstream libloc;
  libloc << env.globus_location << "/lib";
  if (env.dsi_module_extension == "xroot") {
    libloc << ":" << env.xroot_location << "/lib";
  }
  setenv("LD_LIBRARY_PATH", libloc.str().c_str(), 1);
  std::ostringstream tcprange;
  tcprange << env.tcp_port_range.first << ","
           << env.tcp_port_range.second;
  setenv("GLOBUS_TCP_PORT_RANGE", tcprange.str().c_str(), 1);
  std::ostringstream sourcerange;
  sourcerange << env.tcp_source_range.first << ","
              << env.tcp_source_range.second;
  setenv("GLOBUS_TCP_SOURCE_RANGE", sourcerange.str().c_str(), 1);
  setenv("X509_USER_CERT", env.globus_x509_user_cert.c_str(), 1);
  setenv("X509_USER_KEY", env.globus_x509_user_key.c_str(), 1);
  setenv("USE_CKSUM", env.use_cksum.c_str(), 1);
  // This variables we will use inside CASTOR2 DSI
  setenv("UUID", args.rawRequestUuid.c_str(), 1);
  std::string path = "";
  if (env.dsi_module_extension == "xroot") {
    path += "root://localhost:1095//" + args.rawRequestUuid +
      "?castor2fs.pfn1=";
  }
  path += context.fullDestPath;
  if (env.dsi_module_extension == "xroot") {
    path += "&castor2fs.pfn2=unused";
  }
  setenv("FULLDESTPATH", path.c_str(), 1);
  switch(args.accessMode) {
  case ReadOnly:
    setenv("ACCESS_MODE","r",1);
    break;
  case WriteOnly:
    setenv("ACCESS_MODE","w",1);
    break;
  case ReadWrite:
    setenv("ACCESS_MODE","o",1);
    break;
  }

  // Build the command line
  std::string progfullpath = env.globus_location + "/sbin/globus-gridftp-server";
  std::string progname = "globus-gridftp-server";
  if (access(progfullpath.c_str(), X_OK) != 0) {
    dlf_shutdown();
    exit(EXIT_FAILURE);
  }
  // Duplicate socket on stdin/stdout/stderr and close the others
  if (dup2(context.socket, 0) < 0 ||
      dup2(context.socket, 1) < 0 ||
      dup2(context.socket, 2) < 0) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("Error Code", errno),
       castor::dlf::Param("Error Message", strerror(errno)),
       castor::dlf::Param(args.subRequestUuid)};
    castor::dlf::dlf_writep(args.requestUuid, DLF_LVL_ERROR,
                            DUP2FAILED, 3, params, &args.fileId);
    dlf_shutdown();
    exit(EXIT_FAILURE);
  }
  std::string moduleName = "CASTOR2" + env.dsi_module_extension;
  execl (progfullpath.c_str(), progname.c_str(),
         "-i",
         "-d", env.globus_loglevel.c_str(),
         // Prevents globus-gridftp-server from performing the
         // mapping and changing identity
         "-auth-level", "0",
         "-control-idle-timeout", "3600",
         "-Z", env.globus_logfile_netlogger.c_str(),
         "-l", env.globus_logfile.c_str(),
         "-dsi", moduleName.c_str(),
         "-allowed-modules", moduleName.c_str(),
         NULL);
  // Should never be reached
  dlf_shutdown();
  exit(EXIT_FAILURE);
}
