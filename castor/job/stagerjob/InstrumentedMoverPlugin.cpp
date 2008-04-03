/******************************************************************************
 *                      InstrumentedMoverPlugin.cpp
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
 * Abstract plugin of the stager job suitable for instrumented
 * movers. That is movers that were modified to inform the
 * CASTOR framework of their behavior
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include <rfio.h>
#include <string>
#include "castor/dlf/Dlf.hpp"
#include "castor/rh/IOResponse.hpp"
#include "castor/stager/IJobSvc.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/job/stagerjob/InstrumentedMoverPlugin.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::job::stagerjob::InstrumentedMoverPlugin::InstrumentedMoverPlugin
(std::string protocol) throw():
  BasePlugin(protocol) {
}

//------------------------------------------------------------------------------
// postForkHook
//------------------------------------------------------------------------------
void castor::job::stagerjob::InstrumentedMoverPlugin::postForkHook
(InputArguments &args, PluginContext &context)
  throw(castor::exception::Exception) {
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
  ioResponse.setFileName(args.rawSubRequestUuid);
  sendResponse(args.client, ioResponse);
  // Wait for children
  int childFailed = waitForChild(args);
  // If all was fine, just return
  if (!childFailed) return;
  // else inform CASTOR
  if (args.accessMode == ReadOnly ||
      args.accessMode == ReadWrite) {
    context.jobSvc->getUpdateFailed
      (args.subRequestId, args.fileId.fileid, args.fileId.server);
  } else {
    // Do a stat of the output file
    rfio_errno = serrno = 0;
    struct stat64 statbuf;
    if (rfio_stat64((char*)context.fullDestPath.c_str(),&statbuf) == 0) {
      castor::stager::SubRequest subrequest;
      subrequest.setId(args.subRequestId);
      context.jobSvc->prepareForMigration
        (&subrequest, (u_signed64) statbuf.st_size,
         time(NULL), args.fileId.fileid, args.fileId.server);
    } else {
      castor::dlf::Param params[] =
        {castor::dlf::Param("JobId", getenv("LSB_JOBID")),
         castor::dlf::Param("Path", context.fullDestPath),
         castor::dlf::Param("Error", rfio_serror()),
         castor::dlf::Param(args.subRequestUuid)};
      castor::dlf::dlf_writep(args.requestUuid, DLF_LVL_ERROR,
                              STAT64FAIL, 4, params, &args.fileId);
      context.jobSvc->putFailed
        (args.subRequestId, args.fileId.fileid, args.fileId.server);
      castor::exception::Internal e;
      e.getMessage() << "rfio_stat64 error";
      throw e;
    }
  }
}
