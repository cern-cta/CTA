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
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <string>
#include "castor/dlf/Dlf.hpp"
#include "castor/rh/IOResponse.hpp"
#include "castor/stager/IJobSvc.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/job/stagerjob/InputArguments.hpp"
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
  ioResponse.setFileName(args.rawSubRequestUuid);
  sendResponse(args.client, ioResponse);
  // Then wait for the child to complete and inform stager
  waitChildAndInformStager(args, context);
}

//------------------------------------------------------------------------------
// waitChildAndInformStager
//------------------------------------------------------------------------------
void castor::job::stagerjob::InstrumentedMoverPlugin::waitChildAndInformStager
(InputArguments &args, PluginContext &context)
  throw(castor::exception::Exception) {
  // Wait for children
  bool childFailed = waitForChild(args);
  // No longer waiting for any mover processes
  context.childPid = 0;
  // If all was fine, just return
  if (!childFailed) return;
  // else inform CASTOR
  if (args.accessMode == ReadOnly ||
      args.accessMode == ReadWrite) {
    context.jobSvc->getUpdateFailed
      (args.subRequestId, args.fileId.fileid, args.fileId.server);
  } else {
    // Do a stat of the output file to see whether there are data to keep
    errno = 0;
    struct stat64 statbuf;
    int rc = stat64((char*)context.fullDestPath.c_str(), &statbuf);
    if (rc == 0) {
      if (0 != statbuf.st_size) {
        // some file found, call prepareForMigration to save the data we have
        context.jobSvc->prepareForMigration
          (args.subRequestId, (u_signed64) statbuf.st_size,
           time(NULL), args.fileId.fileid, args.fileId.server);
      } else {
        // empty file on disk, log an error and drop it
        castor::dlf::Param params[] =
          {castor::dlf::Param("JobId", getenv("LSB_JOBID")),
           castor::dlf::Param("Path", context.fullDestPath),
           castor::dlf::Param(args.subRequestUuid)};
        castor::dlf::dlf_writep(args.requestUuid, DLF_LVL_ERROR,
                                NODATAWRITTEN, 3, params, &args.fileId);
        // try to drop the file
        errno = 0;
        if (unlink((char*)context.fullDestPath.c_str()) != 0) {
          castor::dlf::Param params[] =
            {castor::dlf::Param("JobId", getenv("LSB_JOBID")),
             castor::dlf::Param("Path", context.fullDestPath),
             castor::dlf::Param("Error", strerror(errno)),
             castor::dlf::Param(args.subRequestUuid)};
          castor::dlf::dlf_writep(args.requestUuid, DLF_LVL_ERROR,
                                  UNLINKFAIL, 4, params, &args.fileId);
        }
        // and call putFailed
        context.jobSvc->putFailed
          (args.subRequestId, args.fileId.fileid, args.fileId.server);
        castor::exception::Internal e;
        e.getMessage() << "No data transfered";
        throw e;
      }
    } else {
      // No file found on disk, log an error
      castor::dlf::Param params[] =
        {castor::dlf::Param("JobId", getenv("LSB_JOBID")),
         castor::dlf::Param("Path", context.fullDestPath),
         castor::dlf::Param("Error", strerror(errno)),
         castor::dlf::Param(args.subRequestUuid)};
      castor::dlf::dlf_writep(args.requestUuid, DLF_LVL_ERROR,
                              STAT64FAIL, 4, params, &args.fileId);
      // and call putFailed
      context.jobSvc->putFailed
        (args.subRequestId, args.fileId.fileid, args.fileId.server);
      castor::exception::Internal e;
      e.getMessage() << "stat64 error";
      throw e;
    }
  }
}
