/******************************************************************************
 *                      RawMoverPlugin.cpp
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
 * Abstract plugin of the stager job suitable for raw
 * movers, that is movers that were unmodified and do not
 * inform CASTOR in any way
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <common.h>
#include <errno.h>
#include <string.h>
#include <map>
#include <attr/xattr.h>
#include "castor/dlf/Dlf.hpp"
#include "castor/rh/IOResponse.hpp"
#include "castor/stager/IJobSvc.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/TimeOut.hpp"
#include "castor/exception/NoValue.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/job/stagerjob/InputArguments.hpp"
#include "castor/job/stagerjob/RawMoverPlugin.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::job::stagerjob::RawMoverPlugin::RawMoverPlugin
(std::string protocol) throw():
  BasePlugin(protocol) {
}

//------------------------------------------------------------------------------
// preForkHook
//------------------------------------------------------------------------------
void castor::job::stagerjob::RawMoverPlugin::preForkHook
(InputArguments &args, PluginContext &context)
  throw(castor::exception::Exception) {
  // For raw movers we use the inetd mode :
  // we send the I/O response before forking the mover
  castor::rh::IOResponse ioResponse;
  ioResponse.setStatus(castor::stager::SUBREQUEST_READY);
  ioResponse.setSubreqId(args.rawSubRequestUuid);
  ioResponse.setReqAssociated(args.rawRequestUuid);
  ioResponse.setId(args.subRequestId);
  ioResponse.setFileId(args.fileId.fileid);
  ioResponse.setServer(context.host);
  ioResponse.setPort(context.port);
  ioResponse.setProtocol(args.protocol);
  ioResponse.setFileName(args.rawRequestUuid);
  sendResponse(args.client, ioResponse);
  // Do ourselves a timed out accept() so that the mover can work in inetd mode
  struct timeval timeval;
  fd_set readmask;
  FD_ZERO(&readmask);
  FD_SET (context.socket, &readmask);
  int saccept;
  int rc_select;
  timeval.tv_sec = getSelectTimeOut();
  timeval.tv_usec = 0;
  if ((rc_select = select(context.socket + 1, &readmask, (fd_set *) NULL,
                          (fd_set *) NULL, &timeval)) <= 0) {
    if (rc_select == 0) {
      castor::exception::TimeOut e;
      e.getMessage() << "In call to select()";
      throw e;
    } else {
      castor::exception::Exception e(errno);
      e.getMessage() << "In call to select()";
      throw e;
    }
  }
  struct sockaddr_in from;
  socklen_t fromlen = sizeof(from);
  struct hostent *hp;
  char *clienthostptr;
  // Connection established
  if ((saccept = accept (context.socket, (struct sockaddr *) &from, &fromlen)) < 0) {
    castor::exception::Exception e(errno);
    e.getMessage() << "In call to accept()";
    throw e;
  }
  if (getpeername (saccept, (struct sockaddr*)&from, &fromlen) < 0) {
    castor::exception::Exception e(errno);
    e.getMessage() << "In call to getpeername()";
    throw e;
  }
  hp = Cgethostbyaddr((char *)(&from.sin_addr), sizeof(struct in_addr), from.sin_family);
  if (hp == NULL) {
    clienthostptr = inet_ntoa(from.sin_addr);
  } else {
    clienthostptr = hp->h_name;
  }
  // We can close the original socket
  if (close(context.socket) < 0) {
    // This is not fatal
    castor::dlf::Param params[] =
      {castor::dlf::Param("Error Code", errno),
       castor::dlf::Param("Error Message", strerror(errno)),
       castor::dlf::Param(args.subRequestUuid)};
    castor::dlf::dlf_writep(args.requestUuid, DLF_LVL_ERROR,
                            SCLOSEFAILED, 3, params, &args.fileId);
  }
  // Applications below: please use s
  context.socket = saccept;
  castor::dlf::Param params[] =
    {castor::dlf::Param("Client machine", clienthostptr),
     castor::dlf::Param("File descriptor", context.socket),
     castor::dlf::Param(args.subRequestUuid)};
  castor::dlf::dlf_writep(args.requestUuid, DLF_LVL_DEBUG,
                          ACCEPTCONN, 3, params, &args.fileId);
}

//------------------------------------------------------------------------------
// postForkHook
//------------------------------------------------------------------------------
void castor::job::stagerjob::RawMoverPlugin::postForkHook
(InputArguments &args, PluginContext &context,
 bool useChkSum, int moverStatus)
  throw(castor::exception::Exception) {
  bool childFailed;
  if (moverStatus == -1) {
    // Wait for children
    childFailed = waitForChild(args);
  } else {
    childFailed = moverStatus;
  }
  // No longer waiting for any mover processes
  context.childPid = 0;
  // Inform CASTOR
  if (args.accessMode == ReadOnly ||
      args.accessMode == ReadWrite) {
    if (childFailed) {
      context.jobSvc->getUpdateFailed
        (args.subRequestId, args.fileId.fileid, args.fileId.server);
    } else {
      context.jobSvc->getUpdateDone
        (args.subRequestId, args.fileId.fileid, args.fileId.server);
    }
  } else {
    // Do a stat of the output file
    errno = 0;
    struct stat64 statbuf;
    if (stat64((char*)context.fullDestPath.c_str(), &statbuf) == 0) {
      // deal with the case of an empty file when the child failed
      if (childFailed && (0 == statbuf.st_size)) {
        // "No data transfered"
        castor::dlf::Param params[] =
          {castor::dlf::Param("JobId", getenv("LSB_JOBID")),
           castor::dlf::Param("Path", context.fullDestPath),
           castor::dlf::Param(args.subRequestUuid)};
        castor::dlf::dlf_writep(args.requestUuid, DLF_LVL_USER_ERROR,
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
        castor::exception::NoValue e;
        e.getMessage() << "No data transfered";
        throw e;
      }
      // Extract checksum information from extended attributes if instructed
      // to do so by the mover.
      char csumvalue[CA_MAXCKSUMLEN + 1];
      char csumtype[CA_MAXCKSUMNAMELEN + 1];
      // Initialise checksum values
      memset(csumvalue, 0, CA_MAXCKSUMLEN + 1);
      memset(csumtype, 0, CA_MAXCKSUMNAMELEN + 1);
      // A list of supported checksum types
      std::map<std::string, std::string> supportedTypes;
      supportedTypes["ADLER32"] = "AD";
      supportedTypes["CRC32"]   = "CS";
      supportedTypes["MD5"]     = "MD";
      if (useChkSum) {
        if ((getxattr((char*)context.fullDestPath.c_str(),
                      "user.castor.checksum.value",
                      csumvalue, CA_MAXCKSUMLEN) == -1) ||
            (getxattr((char*)context.fullDestPath.c_str(),
                      "user.castor.checksum.type",
                      csumtype, CA_MAXCKSUMNAMELEN) == -1)) {
          // Failed to get checksum information from extended attributes
          castor::dlf::Param params[] =
            {castor::dlf::Param("Path", context.fullDestPath),
             castor::dlf::Param("Error", strerror(errno)),
             castor::dlf::Param(args.subRequestUuid)};
          castor::dlf::dlf_writep(args.requestUuid, DLF_LVL_WARNING,
                                  GETATTRFAILED, 3, params, &args.fileId);
          // Reset checksum information
          csumvalue[0] = csumtype[0] = '\0';
        } else {
          std::map<std::string, std::string>::const_iterator it =
            supportedTypes.find(csumtype);
          if (it == supportedTypes.end()) {
            // Unsupported checksum type, ignoring checksum information
            castor::dlf::Param params[] =
              {castor::dlf::Param("Type", csumtype),
               castor::dlf::Param(args.subRequestUuid)};
            castor::dlf::dlf_writep(args.requestUuid, DLF_LVL_WARNING,
                                    CSTYPENOTSOP, 2, params, &args.fileId);
            // Reset checksum information
            csumvalue[0] = csumtype[0] = '\0';
          }
          // Set csumtype to its abbreviated equivalent.
          strcpy(csumtype, it->second.c_str());
          csumtype[2] = '\0';
        }
      }
      // Call prepareForMigration
      context.jobSvc->prepareForMigration
        (args.subRequestId, (u_signed64) statbuf.st_size,
         time(NULL), args.fileId.fileid, args.fileId.server,
         csumtype, csumvalue);
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
