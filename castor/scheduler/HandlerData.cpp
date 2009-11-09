/******************************************************************************
 *                      HandlerData.cpp
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
 * @(#)$RCSfile: HandlerData.cpp,v $ $Revision: 1.6 $ $Release$ $Date: 2008/10/02 12:17:40 $ $Author: waldron $
 *
 * @author Dennis Waldron
 *****************************************************************************/

// Include files
#include "castor/scheduler/HandlerData.hpp"
#include "castor/scheduler/Constants.hpp"
#include <errno.h>
#include <string.h>


//-----------------------------------------------------------------------------
// Constructor
//-----------------------------------------------------------------------------
castor::scheduler::HandlerData::HandlerData(void *resreq)
  throw(castor::exception::Exception) :
  reqId(nullCuuid),
  subReqId(nullCuuid),
  jobName(""),
  protocol(""),
  xsize(0),
  defaultFileSize(0),
  svcClass(""),
  requestedFileSystems(""),
  openFlags(""),
  requestType(0),
  sourceDiskServer(""),
  sourceFileSystem(""),
  sourceSvcClass(""),
  notifyFile(""),
  selectedDiskServer(""),
  selectedFileSystem(""),
  jobId(0),
  matches(0),
  creationTime(time(NULL)) {

  // Initialize Cns_fileid structure
  memset(&fileId, 0, sizeof(fileId));

  // Get the user specified resource requirements attached to the job, posted
  // during the lsb_submit(3) call with option2 SUB_EXTSCHED
  int msgCnt = 0;
  char **msgArray = lsb_resreq_getextresreq(resreq, &msgCnt);
  if ((msgCnt < 1) || (msgArray == NULL) || (msgArray[1][0] == 0)) {
    castor::exception::Exception e(EINVAL);
    e.getMessage() << "Missing external scheduler option for job";
    throw e;
  }

  // Build a stream from the external scheduler options
  std::istringstream extsched(msgArray[1]);
  extsched.exceptions((std::ios_base::iostate)(std::ios_base::eofbit ||
					       std::ios_base::failbit));

  // Parse the stream
  try {
    std::string token, excludedHostsStr;
    std::getline(extsched, token, '=');                // SIZE
    extsched >> xsize;
    std::getline(extsched, token, '=');                // DEFSIZE
    extsched >> defaultFileSize;
    std::getline(extsched, token, '=');                // RFS
    std::getline(extsched, requestedFileSystems, ';');
    std::getline(extsched, token, '=');                // PROTOCOL
    std::getline(extsched, protocol, ';');
    std::getline(extsched, token, '=');                // SVCCLASS
    std::getline(extsched, svcClass, ';');
    std::getline(extsched, token, '=');                // REQUESTID
    std::getline(extsched, token, ';');
    string2Cuuid(&reqId, (char *)token.c_str());
    std::getline(extsched, token, '=');                // SUBREQUESTID
    std::getline(extsched, jobName, ';');
    string2Cuuid(&subReqId, (char *)jobName.c_str());
    std::getline(extsched, token, '=');                // OPENFLAGS
    std::getline(extsched, openFlags, ';');
    std::getline(extsched, token, '=');                // FILEID
    extsched >> fileId.fileid;
    std::getline(extsched, token, '=');                // NSHOST
    std::getline(extsched, token, ';');
    strncpy(fileId.server, (char *)token.c_str(),
	    sizeof(fileId.server));
    std::getline(extsched, token, '=');                // TYPE
    extsched >> requestType;
    std::getline(extsched, token, '=');                // SRCSVCCLASS
    std::getline(extsched, sourceSvcClass, ';');
    std::getline(extsched, token, '=');                // EXCLUDEDHOSTS
    extsched >> excludedHostsStr;

    // Convert the list of requested filesystems into a vector for easier
    // processing later
    if (requestedFileSystems != "") {
      std::istringstream buf(requestedFileSystems);
      buf.exceptions(std::ios_base::failbit);
      try {

	// For StageDiskCopyReplicaRequest's the RFS is actually the source
	// diskserver and filesystem which holds the copy of the castor file
	// to be replicated
	if (requestType == OBJ_StageDiskCopyReplicaRequest) {
	  std::getline(buf, sourceDiskServer, ':');
	  std::getline(buf, sourceFileSystem, '|');
	} else {
	  for (;;) {
	    std::string diskServer, fileSystem;
	    std::getline(buf, diskServer, ':');
	    std::getline(buf, fileSystem, '|');

	    // We store information in the requested filesystems vector both
	    // in terms of diskservers and diskserver:filesystem combination
	    // for search convenience later.
	    std::ostringstream key;
	    key << diskServer << ":" << fileSystem;
	    rfs.push_back(diskServer);
	    rfs.push_back(key.str());
	  }
	}
      } catch (std::ios_base::failure e) {
	// End of stream
      }
    }

    // Convert the string representing the list of hosts to exclude into
    // a vector
    if (excludedHostsStr != "") {
      std::istringstream buf(excludedHostsStr);
      buf.exceptions(std::ios_base::failbit);
      try {
	std::string diskServer;
	while (std::getline(buf, diskServer, '|')) {
	  excludedHosts.push_back(diskServer);
	}
      } catch (std::ios_base::failure e) {
	// End of stream
      }
    }
  } catch (std::ios_base::failure e) {
    castor::exception::Exception ex(EINVAL);
    ex.getMessage() << "Unable to parse external scheduler option: "
		    << "'" << msgArray[1] << "' Error was : " << e.what();
    throw ex;
  }
}
