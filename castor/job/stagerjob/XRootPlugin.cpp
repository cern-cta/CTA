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
  InstrumentedMoverPlugin("xroot") {
}

//------------------------------------------------------------------------------
// postForkHook
//------------------------------------------------------------------------------
void castor::job::stagerjob::XRootPlugin::postForkHook
(InputArguments &args, PluginContext &context)
  throw(castor::exception::Exception) {
  // log the mover command line on behalf of the mover process
  // that cannot log
  const char *xrootsys_default="/usr/local/xroot/bin";
  const char *xrootsys = getenv("XROOTSYS");
  if (xrootsys == NULL) {
    xrootsys = getconfent("XROOT","XROOTSYS",0);
    if (xrootsys == NULL) {
      xrootsys = xrootsys_default;
    }
  }
  std::string progfullpath = xrootsys;
  progfullpath += "/XrdCS2e";
  std::ostringstream cmdLine;
  cmdLine << "Executed " << progfullpath
          << " " << args.rawSubRequestUuid << " "
          << args.fileId.fileid << "@" << args.fileId.server
          << " " << context.fullDestPath.c_str()
          << " (pid=" << context.childPid << ")";
  // "Mover fork uses the following command line"
  castor::dlf::Param params[] =
    {castor::dlf::Param("JobId", getenv("LSB_JOBID")),
     castor::dlf::Param("command line" , cmdLine.str()),
     castor::dlf::Param(args.subRequestUuid)};
  castor::dlf::dlf_writep(args.requestUuid, DLF_LVL_DEBUG,
                          MOVERFORK, 3, params, &args.fileId);

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
  // answer the client so that it can connect to the mover
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
  // then wait for the child to complete and inform stager
  waitChildAndInformStager(args, context);
}

//------------------------------------------------------------------------------
// execMover
//------------------------------------------------------------------------------
void castor::job::stagerjob::XRootPlugin::execMover
(InputArguments &args, PluginContext &context)
  throw(castor::exception::Exception) {
  // get XrdCS2e executable full path
  const char *xrootsys_default="/usr/local/xroot/bin";
  const char *xrootsys = getenv("XROOTSYS");
  if (xrootsys == NULL) {
    xrootsys = getconfent("XROOT","XROOTSYS",0);
    if (xrootsys == NULL) {
      xrootsys = xrootsys_default;
    }
  }
  // Check mover executable
  std::string progfullpath = xrootsys;
  progfullpath += "/XrdCS2e";
  std::string progname = "XrdCS2e";
  if (access(progfullpath.c_str(), X_OK) != 0) {
    dlf_shutdown(5);
    exit(EXIT_FAILURE);
  }
  // Launch mover
  std::ostringstream fid;
  fid << args.fileId.fileid << "@" << args.fileId.server;
  execl (progfullpath.c_str(), progname.c_str(),
         args.rawSubRequestUuid.c_str(), fid.str().c_str(),
         context.fullDestPath.c_str(), NULL);
}
