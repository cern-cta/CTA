/******************************************************************************
 *                      InputArguments.cpp
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
 * @(#)$RCSfile: InputArguments.cpp,v $ $Revision: 1.2 $ $Release$ $Date: 2008/06/25 12:36:07 $ $Author: waldron $
 *
 * small struct holding the list of arguments passed to stagerJob
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include <string>
#include <sstream>
#include "common.h"
#include "getconfent.h"
#include "castor/dlf/Dlf.hpp"
#include "castor/IClientFactory.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/job/SharedResourceHelper.hpp"
#include "castor/job/stagerjob/StagerJob.hpp"
#include "castor/job/stagerjob/InputArguments.hpp"

// default number of retries and interval between retries
// for the sharedResourceHelper
#define DEFAULT_RETRY_ATTEMPTS 60
#define DEFAULT_RETRY_INTERVAL 10

// -----------------------------------------------------------------------
// constructor
// -----------------------------------------------------------------------
castor::job::stagerjob::InputArguments::InputArguments(int argc, char** argv)
  throw (castor::exception::Exception) {
  // We expect nine arguments :
  // fileid@nshost requestuuid subrequestuuid rfeatures
  // subrequest_id@type host:fs mode clientString securityOption Euid EGid  Creation Time

  if (argc != 13) {
    castor::exception::InvalidArgument e;
    e.getMessage() << "Wrong number of arguments given";
    throw e;
  }

  // fileid and nshost
  std::string input = argv[1];
  std::string::size_type atPos = input.find('@');
  if (atPos == std::string::npos) {
    castor::exception::InvalidArgument e;
    e.getMessage() << "First argument should be <fileid>@<nshost>. "
                   << "No '@' found.";
    throw e;
  }
  if (input.find_first_not_of("0123456789") != atPos) {
    castor::exception::InvalidArgument e;
    e.getMessage() << "First argument should be <fileid>@<nshost> "
                   << "and <fileid> should be a numerical value";
    throw e;
  }
  strcpy(fileId.server, input.substr(atPos+1).c_str());
  fileId.fileid = strtou64(input.substr(0, atPos).c_str());

  // request uuid
  rawRequestUuid = argv[2];
  if (string2Cuuid(&requestUuid,argv[2]) != 0) {
    castor::exception::InvalidArgument e;
    e.getMessage() << "Invalid request uuid : " << argv[2];
    throw e;
  }

  // subRequest uuid
  rawSubRequestUuid = argv[3];
  if (string2Cuuid(&subRequestUuid,argv[3]) != 0) {
    castor::exception::InvalidArgument e;
    e.getMessage() << "Invalid request uuid : " << argv[3];
    throw e;
  }

  // rfeatures
  protocol = argv[4];
  if (protocol == "rfio3") protocol = "rfio";
  if (protocol!= "rfio" &&
      protocol!= "root" &&
      protocol!= "xroot" &&
      protocol!= "gsiftp") {
    castor::exception::InvalidArgument e;
    e.getMessage() << "Unsupported protocol " << protocol;
    throw e;
  }

  // subrequest_id and type
  input = argv[5];
  atPos = input.find('@');
  if (atPos == std::string::npos) {
    castor::exception::InvalidArgument e;
    e.getMessage() << "Fifth argument should be <subreqId>@<type>. "
                   << "No '@' found.";
    throw e;
  }
  if (input.find_first_not_of("0123456789") != atPos) {
    castor::exception::InvalidArgument e;
    e.getMessage() << "Fifth argument should be <subreqId>@<type> "
                   << "and <subreqId> should be a numerical value";
    throw e;
  }
  if (input.find_last_not_of("0123456789") != atPos) {
    castor::exception::InvalidArgument e;
    e.getMessage() << "Fifth argument should be <subreqId>@<type> "
                   << "and <type> should be a numerical value";
    throw e;
  }
  subRequestId = strtou64(input.substr(0, atPos).c_str());
  type = atoi(input.substr(atPos+1).c_str());

  // Determine the number of times that the shared resource helper should try
  // to download the resource file from the shared resource URL
  char *value = getconfent("DiskCopy", "RetryAttempts", 0);
  int attempts = DEFAULT_RETRY_ATTEMPTS;
  if (value) {
    attempts = std::strtol(value, 0, 10);
    if (attempts < 1) {
      // "Invalid DiskCopy/RetryInterval option, using default"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Default", attempts),
         castor::dlf::Param(subRequestUuid)};
      castor::dlf::dlf_writep
        (requestUuid, DLF_LVL_WARNING,
         castor::job::stagerjob::INVRETRYINT, 2, params, &fileId);
    }
  }

  // Extract the value of the retry interval. This value determines how long
  // we sleep between retry attempts
  value = getconfent("DiskCopy", "RetryInterval", 0);
  int interval = DEFAULT_RETRY_INTERVAL;
  if (value) {
    interval = std::strtol(value, 0, 10);
    if (interval < 1) {
      // "Invalid DiskCopy/RetryAttempts option, using default"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Default", interval),
         castor::dlf::Param(subRequestUuid)};
      castor::dlf::dlf_writep
        (requestUuid, DLF_LVL_WARNING,
         castor::job::stagerjob::INVRETRYNBAT, 2, params, &fileId);
    }
  }

  // Get the diskserver and filesystem from the resource file
  std::string path = argv[6];
  if (path[path.size()-1] != '/') path += '/';
  char* lsbJobId = getenv("LSB_JOBID");
  if (0 == lsbJobId) {
    castor::exception::Internal e;
    e.getMessage() << "environment variable LSB_JOBID should be set but is not";
    throw e;
  }
  path += lsbJobId;
  castor::job::SharedResourceHelper resHelper(attempts, interval);
  resHelper.setUrl(path);
  std::string content = resHelper.download(false);
  std::istringstream iss(content);
  std::getline(iss, diskServer, ':');
  std::getline(iss, fileSystem, '\n');
  if (iss.fail() || diskServer.empty() || fileSystem.empty()) {
    castor::exception::Internal e;
    e.getMessage() << "Sixth argument should be a resource file containing "
                   << "diskserver:filesystem";
    throw e;
  }

  // Retrieve mode
  char* mode = argv[7];
  if (mode[0] == 'r') {
    accessMode = castor::job::stagerjob::ReadOnly;
  } else if (mode[0] == 'w') {
    accessMode = castor::job::stagerjob::WriteOnly;
  } else if (mode[0] == 'o') {
    accessMode = castor::job::stagerjob::ReadWrite;
  } else {
    castor::exception::InvalidArgument e;
    e.getMessage() << "Seventh argument should be a mode. "
                   << "Legal values are 'r', 'w' and 'o'";
    throw e;
  }

  // clientString
  client = castor::IClientFactory::string2Client(argv[8]);

  // security Option
  std::string secureFlag = argv[9];
  isSecure = false;
  if (secureFlag == "1") {
    isSecure = true;
  }
 
  // request creation time
  requestCreationTime = atoi(argv[10]);
  
  // euid
  euid = strtou64(argv[11]);

  // egid
  egid = strtou64(argv[12]);
}
