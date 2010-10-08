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
#include "castor/dlf/Dlf.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/job/stagerjob/InputArguments.hpp"
#include "castor/job/stagerjob/XRootPlugin.hpp"
#include "castor/rh/IOResponse.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "getconfent.h"
#include "socket_timeout.h"
#include <string.h>

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <sstream>
#include <string>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdio.h>

// Defaults
#define MSGIDENTLEN  43
#define MSGCLOSELEN  8

#define SELECT_TIMEOUT_XROOT_OPEN  30
#define SELECT_TIMEOUT_XROOT_CLOSE 172800 // 2 days

// Static instance of the XRootPlugin
castor::job::stagerjob::XRootPlugin xrootPlugin;

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::job::stagerjob::XRootPlugin::XRootPlugin() throw():
  RawMoverPlugin("xroot") {
  m_openTimeout  = SELECT_TIMEOUT_XROOT_OPEN;
  m_closeTimeout = SELECT_TIMEOUT_XROOT_CLOSE;
}

//------------------------------------------------------------------------------
// recvMessage
//------------------------------------------------------------------------------
void castor::job::stagerjob::XRootPlugin::recvMessage
(int socket, char *buf, ssize_t len, int timeout)
  throw(castor::exception::Exception) {

  // Read the data from the socket
  int n = netread_timeout(socket, buf, len, timeout);
  if (n <= 0) {
    castor::exception::Exception ex((serrno == 0) ? errno : serrno);
    ex.getMessage() << "Unable to receive message body";
    throw ex;
  } else if (n != len) {
    castor::exception::Internal ex;
    ex.getMessage() <<"Received message body is too short : only " << n
                    << " bytes of " << len << " transferred";
    throw ex;
  }
  buf[len - 1] = '\0';
}

//------------------------------------------------------------------------------
// preForkHook
//------------------------------------------------------------------------------
void castor::job::stagerjob::XRootPlugin::preForkHook
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
  ioResponse.setFileName(context.fullDestPath);
  sendResponse(args.client, ioResponse);

  // Variables
  sockaddr_in from;
  socklen_t   fromlen = sizeof(from);
  timeval     timeval;
  fd_set      read_set;
  int         rc;
  int         saccept;
  char        buf[CA_MAXLINELEN + 1];
  char        *value;
  std::string ident;

  // Parse timeout configuration for xrootd. A pair of values representing the
  // timeout for open and close calls.
  if ((value = getconfent("XROOT", "TIMEOUTS", 0))) {
    std::istringstream iss(value);
    iss >> m_openTimeout;
    iss >> m_closeTimeout;
    if ((m_openTimeout < 1) || (m_closeTimeout < 1)) {
      // Construct default value
      std::ostringstream buf("");
      buf << SELECT_TIMEOUT_XROOT_OPEN << " "
          << SELECT_TIMEOUT_XROOT_CLOSE;
      // "Invalid value for XROOT/TIMEOUTS option, using default"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Value", value),
         castor::dlf::Param("Default", buf.str()),
         castor::dlf::Param(args.subRequestUuid)};
      castor::dlf::dlf_writep(args.requestUuid, DLF_LVL_WARNING,
                              XROOTBADTIMEOUT, 3, params, &args.fileId);
      // Set defaults
      m_openTimeout  = SELECT_TIMEOUT_XROOT_OPEN;
      m_closeTimeout = SELECT_TIMEOUT_XROOT_CLOSE;
    }
  }

  // The accept() call doesn't provide for timeouts (like most system calls)
  // so we do it ourselves. Here we wait for an incoming connection from the
  // local xrootd daemon on the machine.

  // Set the timeout value
  timeval.tv_sec  = m_openTimeout;
  timeval.tv_usec = 0;

  // Setup the file descriptor set that we should wait on
  FD_ZERO(&read_set);
  FD_SET(context.socket, &read_set);

  // Waiting for a read event to occur on the socket, this is an indication of
  // a new connection.
  rc = select(context.socket + 1, &read_set,
              (fd_set *)NULL, (fd_set *)NULL, &timeval);
  if (rc == 0) {
    castor::exception::Internal ex;
    ex.getMessage() << "Timeout waiting for xrootd callback";
    throw ex;
  } else if (rc < 0) {
    castor::exception::Exception ex(errno);
    ex.getMessage() << "Error waiting for xrootd callback";
    throw ex;
  }

  // Accept the new connection
  if ((saccept = accept(context.socket, (sockaddr *)&from, &fromlen)) < 0) {
    castor::exception::Exception ex(errno);
    ex.getMessage() << "In call to accept()";
    throw ex;
  }

  // Close the original listening socket, it is no longer needed. We trap the
  // error here even though it's not fatal.
  if (close(context.socket) < 0) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("Error Code", errno),
       castor::dlf::Param("Error Message", strerror(errno)),
       castor::dlf::Param(args.subRequestUuid)};
    castor::dlf::dlf_writep(args.requestUuid, DLF_LVL_ERROR,
                            SCLOSEFAILED, 3, params, &args.fileId);
  }
  context.socket = saccept;

  // Read the message from the client. We use netread_timeout here because it's
  // convenient. It's not perfect though as it doesn't return until all the
  // required number of bytes have been read or the timeout has been reached.
  // This implies that we know the size of the message beforehand. Which in
  // this situation we don't as the protocol is clear text. No marshalling or
  // unmarshalling required! Ideally we would have something that returns on
  // '\n' (newline).
  //
  // Here we wait for the IDENT message to be sent by the client. The IDENT
  // message has two purposes: A) it verifies that the connected client is
  // transferring the file associated with the current stagerjob process and
  // B) that xrootd has opened the file.
  try {
    recvMessage(context.socket, buf, MSGIDENTLEN, 5);
  } catch (castor::exception::Exception& e) {
    castor::exception::Exception ex(e.code());
    ex.getMessage() << "Failed to receive IDENT message from xrootd: "
                    << e.getMessage().str();
    throw e;
  }

  // Verfity that the IDENT is correct
  ident = "IDENT " + args.rawSubRequestUuid;
  if (strcmp(buf, ident.c_str())) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage() << "Invalid IDENT message received: " << buf;
    throw ex;
  }
}

//------------------------------------------------------------------------------
// postForkHook
//------------------------------------------------------------------------------
void castor::job::stagerjob::XRootPlugin::postForkHook
(InputArguments &args, PluginContext &context)
  throw(castor::exception::Exception) {

  // Variables
  bool moverStatus;
  char buf[CA_MAXLINELEN + 1];

  // Now we wait for the CLOSE message to indicate that xrootd has finished
  // with the transfer and that stagerjob can perform any post mover operations
  // such as prepareForMigration, getUpdateDone etc... on its behalf.
  try {
    recvMessage(context.socket, buf, MSGCLOSELEN, m_closeTimeout);
  } catch (castor::exception::Exception& e) {
    castor::exception::Exception ex(e.code());
    ex.getMessage() << "Failed to receive CLOSE message from xrootd: "
                    << e.getMessage().str();
    throw e;
  }

  // Interpret the CLOSE message
  if (!strcmp(buf, "CLOSE 0")) {
    moverStatus = 0;
  } else if (!strcmp(buf, "CLOSE 1")) {
    moverStatus = 1;
  } else {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage() << "Invalid CLOSE message received: " << buf;
    throw ex;
  }

  // Call the upper level postForkHook
  try {
    RawMoverPlugin::postForkHook(args, context, true, moverStatus);

    // Notify xrootd to the successful close of the file from CASTOR's
    // perspective.
    strcpy(buf, "0 Success\n");
    netwrite_timeout(context.socket, buf, strlen(buf), 5);
  } catch (castor::exception::Exception& e) {

    // Notify xrootd to the failure to close the file so that it can notify its
    // associated client.
    snprintf(buf, sizeof(buf), "%d %s\n",
             e.code(), e.getMessage().str().c_str());
    netwrite_timeout(context.socket, buf, strlen(buf), 5);

    // Let the callee see the error
    throw e;
  }
}
