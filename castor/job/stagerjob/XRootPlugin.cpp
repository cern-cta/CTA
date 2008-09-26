/******************************************************************************
 *                      XRootPlugin.cpp
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
 * Plugin of the stager job concerning XRoot
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include <string>
#include <errno.h>
#include <sstream>
#include "getconfent.h"
#include "castor/dlf/Dlf.hpp"
#include "castor/rh/IOResponse.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/job/stagerjob/InputArguments.hpp"
#include "castor/job/stagerjob/XRootPlugin.hpp"

// static instance of the XRootPlugin
castor::job::stagerjob::XRootPlugin xrootPlugin;

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::job::stagerjob::XRootPlugin::XRootPlugin() throw():
  RawMoverPlugin("xroot") {
}

//------------------------------------------------------------------------------
// postForkHook
//------------------------------------------------------------------------------
void castor::job::stagerjob::XRootPlugin::postForkHook
(InputArguments &args, PluginContext &context)
  throw(castor::exception::Exception) {

  // Log the mover command line on behalf of the mover process that cannot log
  std::vector<std::string> paths;
  paths.push_back("/usr/local/xroot/bin/XrdCS2e");
  paths.push_back("/opt/xrootd/bin/x2castorjob");

  // Determine which command line exists
  std::string progfullpath = "";
  for (unsigned int i = 0; i < paths.size(); i++) {
     if (access(paths[i].c_str(), F_OK) == 0) {
       progfullpath = paths[i];
       break;
     }
  }

  // Path found?
  if (progfullpath == "") {
    castor::exception::Exception e(SEINTERNAL);
    e.getMessage() << "Xrootd is not installed";
    throw e;
  } else {

    // Build command line
    std::ostringstream cmdLine;
    cmdLine << "Executed " << progfullpath;
    std::string::size_type loc = progfullpath.find("x2castorjob", 0);
    if (loc != std::string::npos) {
      cmdLine << " " << args.subRequestId;
    } else {
      cmdLine << " " << args.rawSubRequestUuid << " "
	      << args.fileId.fileid << "@" << args.fileId.server
	      << " " << context.fullDestPath.c_str();
    }
    cmdLine << " (pid=" << context.childPid << ")";

    // "Mover fork uses the following command line"
    castor::dlf::Param params[] =
      {castor::dlf::Param("JobId", getenv("LSB_JOBID")),
       castor::dlf::Param("Command Line" , cmdLine.str()),
       castor::dlf::Param(args.subRequestUuid)};
    castor::dlf::dlf_writep(args.requestUuid, DLF_LVL_DEBUG,
			    MOVERFORK, 3, params, &args.fileId);

    // Check that the mover can be executed
    if (access(progfullpath.c_str(), X_OK) != 0) {
      // "Mover program cannot be executed. Check permissions"
      castor::dlf::Param params[] =
	{castor::dlf::Param("JobId", getenv("LSB_JOBID")),
	 castor::dlf::Param("Mover Path", progfullpath),
	 castor::dlf::Param("ErrorMessage", sstrerror(errno)),
	 castor::dlf::Param(args.subRequestUuid)};
      castor::dlf::dlf_writep(args.requestUuid, DLF_LVL_ERROR,
			      MOVERNOTEXEC, 4, params, &args.fileId);

      // Throw an exception
      castor::exception::Exception e(SEINTERNAL);
      e.getMessage() << "Mover program cannot be executed";
      throw e;
    }
  }

  // Answer the client so that it can connect to the mover
  castor::rh::IOResponse ioResponse;
  ioResponse.setStatus(castor::stager::SUBREQUEST_READY);
  ioResponse.setSubreqId(args.rawSubRequestUuid);
  ioResponse.setReqAssociated(args.rawRequestUuid);
  ioResponse.setId(args.subRequestId);
  ioResponse.setFileId(args.fileId.fileid);
  ioResponse.setServer(context.host);
  ioResponse.setPort(context.port);
  ioResponse.setProtocol(args.protocol);
  ioResponse.setFileName(context.fullDestPath);
  sendResponse(args.client, ioResponse);

  // And call upper level for the actual work
  RawMoverPlugin::postForkHook(args, context);
}

//------------------------------------------------------------------------------
// execMover
//------------------------------------------------------------------------------
void castor::job::stagerjob::XRootPlugin::execMover
(InputArguments &args, PluginContext &context)
  throw(castor::exception::Exception) {

  // List of paths to the movers
  std::vector<std::string> paths;
  paths.push_back("/usr/local/xroot/bin/XrdCS2e");
  paths.push_back("/opt/xrootd/bin/x2castorjob");

  // Determine which command line exists
  std::string progfullpath = "";
  for (unsigned int i = 0; i < paths.size(); i++) {
     if (access(paths[i].c_str(), F_OK) == 0) {
       progfullpath = paths[i];
       break;
     }
  }

  // Check mover executable
  if ((progfullpath == "") || (access(progfullpath.c_str(), X_OK) != 0)) {
    dlf_shutdown(5);
    exit(EXIT_FAILURE);
  }

  // Determine the name of the program
  std::string::size_type loc = progfullpath.find_last_of("/");
  std::string progname = progfullpath.substr(loc + 1);

  // Launch mover
  loc = progfullpath.find("x2castorjob", 0);
  if (loc != std::string::npos) {
    std::ostringstream subRequestId;
    subRequestId << args.subRequestId;
    execl (progfullpath.c_str(), progname.c_str(),
	   subRequestId.str().c_str(), NULL);
  } else {
    std::ostringstream fid;
    fid << args.fileId.fileid << "@" << args.fileId.server;
    execl (progfullpath.c_str(), progname.c_str(),
	   args.rawSubRequestUuid.c_str(), fid.str().c_str(),
	   context.fullDestPath.c_str(), NULL);
  }
}
