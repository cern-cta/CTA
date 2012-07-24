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
#include <unistd.h>
#include "getconfent.h"
#include "rfio_constants.h"
#include "castor/dlf/Dlf.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/job/stagerjob/InputArguments.hpp"
#include "castor/job/stagerjob/RfioPlugin.hpp"
#include <string.h>

// Default port range
#define RFIODMINPORT RFIO_LOW_PORT_RANGE
#define RFIODMAXPORT RFIO_HIGH_PORT_RANGE

// Static instance of the RfioPlugin
castor::job::stagerjob::RfioPlugin rfioPlugin;

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::job::stagerjob::RfioPlugin::RfioPlugin() throw():
  InstrumentedMoverPlugin("rfio") {
  // Also register for rfio3 (rfio registration is done by the parent)
  castor::job::stagerjob::registerPlugin("rfio3", this);
}

//------------------------------------------------------------------------------
// getPortRange
//------------------------------------------------------------------------------
std::pair<int, int> castor::job::stagerjob::RfioPlugin::getPortRange
(InputArguments &args, std::string name)
  throw() {
  // Look at the config file
  char* entry = getconfent("RFIOD", name.c_str(), 0);
  if (NULL == entry) {
    return std::pair<int, int>(RFIODMINPORT, RFIODMAXPORT);
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
                            RFIODBADPORT, 3, params, &args.fileId);
    return std::pair<int, int>(RFIODMINPORT, RFIODMAXPORT);
  }
  int min = RFIODMINPORT;
  if (value.find_first_not_of("0123456789") != value.npos) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("Found", value),
       castor::dlf::Param(args.subRequestUuid)};
    castor::dlf::dlf_writep(args.requestUuid, DLF_LVL_ERROR,
                            RFIODBADMINPORT, 2, params, &args.fileId);
  } else {
    // Check min port value
    min = atoi(value.c_str());
    if (min < 1024 || min > 65535) {
      castor::dlf::Param params[] =
        {castor::dlf::Param("Value", min),
         castor::dlf::Param(args.subRequestUuid)};
      castor::dlf::dlf_writep(args.requestUuid, DLF_LVL_ERROR,
                              RFIODBADMINVAL, 2, params, &args.fileId);
      min = RFIODMINPORT;
    }
  }
  // Parse max port
  int max = RFIODMAXPORT;
  std::getline(iss, value);
  if (value.find_first_not_of("0123456789") != value.npos) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("Found", value),
       castor::dlf::Param(args.subRequestUuid)};
    castor::dlf::dlf_writep(args.requestUuid, DLF_LVL_ERROR,
                            RFIODBADMAXPORT, 2, params, &args.fileId);
  } else {
    max = atoi(value.c_str());
    if (max < 1024 || max > 65535 || max < min) {
      castor::dlf::Param params[] =
        {castor::dlf::Param("Value", max),
         castor::dlf::Param("Min value", min),
         castor::dlf::Param(args.subRequestUuid)};
      castor::dlf::dlf_writep(args.requestUuid, DLF_LVL_ERROR,
                              RFIODBADMAXVAL, 3, params, &args.fileId);
      max = RFIODMAXPORT;
    }
  }
  return std::pair<int, int>(min, max);
}

//------------------------------------------------------------------------------
// getPortRange
//------------------------------------------------------------------------------
std::pair<int, int>
castor::job::stagerjob::RfioPlugin::getPortRange
(InputArguments &args) throw() {
  return getPortRange(args, "PORT_RANGE");
}

//------------------------------------------------------------------------------
// getLogLevel
//------------------------------------------------------------------------------
void castor::job::stagerjob::RfioPlugin::getLogLevel
(std::string &logFile, bool &debug)
  throw() {
  // Get log file
  logFile = "/dev/null";
  char* value = getconfent("RFIOD", "LOGFILE", 0);
  if (value != NULL) {
    logFile = strdup(value);
  }
  // Get logging level
  debug = false;
  value = getconfent("RFIOD", "DEBUG", 0);
  if (value != NULL) {
    if (!strcasecmp(value, "yes")) {
      debug = true;
    }
  }
}

//------------------------------------------------------------------------------
// setEnvironment
//------------------------------------------------------------------------------
void castor::job::stagerjob::RfioPlugin::setEnvironment
(EnvironmentRfio env) throw () {
  setenv("CSEC_TRACE", env.csec_trace.c_str(), 1);
  setenv("CSEC_TRACEFILE", env.csec_tracefile.c_str(), 1);
  setenv("CSEC_MECH", env.csec_mech.c_str(), 1);
  setenv("GRIDMAP", env.gridmapfile.c_str(), 1);
  setenv("X509_USER_KEY", env.globus_x509_user_key.c_str(), 1);
  setenv("X509_USER_CERT", env.globus_x509_user_cert.c_str(), 1);
  setenv("KRB5_KTNAME", env.keytab_location.c_str(), 1);
  std::ostringstream libloc;
  libloc << env.globus_location << "/lib";
  setenv("LD_LIBRARY_PATH", libloc.str().c_str(), 1);
}

//------------------------------------------------------------------------------
// getEnvironment
//------------------------------------------------------------------------------
void castor::job::stagerjob::RfioPlugin::getEnvironment
(InputArguments&, EnvironmentRfio &env) throw () {
  // Globus location, required to resolved some dependencies
  const char* globus_location = getconfent("CSEC", "GLOBUS_LOCATION", 0);
  if (globus_location == NULL) {
    env.globus_location = "/opt/globus";
  } else {
    env.globus_location = globus_location;
  }

  // X509 Environment variables
  // Get certificate, key and location of the gridmapfile
  const char *globus_x509_user_cert = getconfent("CSEC", "X509_USER_CERT", 0);
  if (globus_x509_user_cert == NULL) {
    env.globus_x509_user_cert = "/etc/grid-security/castor-csec/castor-csec-cert.pem";
  } else {
    env.globus_x509_user_cert = globus_x509_user_cert;
  }

  const char *globus_x509_user_key = getconfent("CSEC", "X509_USER_KEY", 0);
  if (globus_x509_user_key == NULL) {
    env.globus_x509_user_key = "/etc/grid-security/castor-csec/castor-csec-key.pem";
  } else {
    env.globus_x509_user_key = globus_x509_user_key;
  }

  const char *gridmapfile = getconfent("CSEC", "GRIDMAP", 0);
  if (gridmapfile == NULL) {
    env.gridmapfile = "/etc/grid-security/grid-mapfile";
  } else {
    env.gridmapfile = gridmapfile;
  }

  // KRB5 Environment variables
  const char *keytab_location = getconfent("CSEC", "KRB5_KTNAME", 0);
  if (keytab_location == NULL) {
    env.keytab_location = "FILE:/etc/castor-csec-krb5.keytab";
  } else {
    env.keytab_location = keytab_location;
  }

  const char* debug = getconfent("CSEC", "DEBUG", 0);
  if (debug != NULL) {
    if (!strcasecmp(debug, "yes")) {

      // Get the CSEC mechanism, trace and tracefile
      const char *csec_trace = getconfent("CSEC", "TRACE", 0);
      if (csec_trace == NULL) {
        env.csec_trace = "3";
      } else {
        env.csec_trace = csec_trace;
      }

      const char *csec_tracefile = getconfent("CSEC", "TRACEFILE", 0);
      if (csec_tracefile == NULL) {
        env.csec_tracefile = "/var/log/castor/rfiod.csec.log";
      } else {
        env.csec_tracefile = csec_tracefile;
      }
    }
  }

  const char *csec_mech = getconfent("CSEC", "MECH", 0);
  if (csec_mech == NULL) {
    env.csec_mech = "GSI KRB5";
  } else {
    env.csec_mech = csec_mech;
  }
}

//------------------------------------------------------------------------------
// postForkHook
//------------------------------------------------------------------------------
void castor::job::stagerjob::RfioPlugin::postForkHook
(InputArguments &args, PluginContext &context,
 bool /*useChksSum*/, int /*moverStatus*/)
  throw(castor::exception::Exception) {

  // Log the mover command line on behalf of the mover process that cannot log
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
          << " -U -Z " << args.subRequestId;
  if (args.isSecure){
    cmdLine << "-u" << args.euid
            << "-g" << args.egid;
  }
  cmdLine << " " << context.fullDestPath
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
    // Throw an exception
    castor::exception::Exception e(SEINTERNAL);
    e.getMessage() << "Mover program cannot be executed";
    throw e;
  }
  // And call upper level for the actual work
  InstrumentedMoverPlugin::postForkHook(args, context);
}

//------------------------------------------------------------------------------
// execMover
//------------------------------------------------------------------------------
void castor::job::stagerjob::RfioPlugin::execMover
(InputArguments &args, PluginContext &context)
  throw(castor::exception::Exception) {

  // Get environment
  EnvironmentRfio env;
  getEnvironment(args, env);
  setEnvironment(env);
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
  std::ostringstream arg_U;
  arg_U << args.euid;
  std::ostringstream arg_G;
  arg_G << args.egid;
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
  } else {
    if (args.isSecure) {
      execl (progfullpath.c_str(), progname.c_str(),
             "-1", "-d", "-s", "-l", "-n", "-f", logFile.c_str(),
             "-T", arg_T.str().c_str(), "-S", arg_S.str().c_str(),
             "-P", arg_P.str().c_str(), "-M", arg_M.str().c_str(),
             "-U", "-Z", arg_Z.str().c_str(), "-u", arg_U.str().c_str(),
             "-g", arg_G.str().c_str(),
             context.fullDestPath.c_str(),NULL);
    } else {
      // The flags are the same as in unsecure mode so the stagerjob
      // can work with rfiod unsecure (provisional)
      execl (progfullpath.c_str(), progname.c_str(),
             "-1", "-s", "-l", "-n", "-f", logFile.c_str(),
             "-T", arg_T.str().c_str(), "-S", arg_S.str().c_str(),
             "-P", arg_P.str().c_str(), "-M", arg_M.str().c_str(),
             "-U", "-Z", arg_Z.str().c_str(),
             context.fullDestPath.c_str(), NULL);
    }
  }
  // Should never be reached
  dlf_shutdown();
  exit(EXIT_FAILURE);
}
