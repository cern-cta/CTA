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
 * @(#)$RCSfile: InputArguments.cpp,v $ $Revision: 1.5 $ $Release$ $Date: 2009/07/23 12:21:57 $ $Author: waldron $
 *
 * small struct holding the list of arguments passed to stagerjob
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include <string>
#include <sstream>
#include "common.h"
#include "getconfent.h"
#include "castor/dlf/Dlf.hpp"
#include "castor/Constants.hpp"
#include "castor/IClientFactory.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "castor/job/SharedResourceHelper.hpp"
#include "castor/job/stagerjob/StagerJob.hpp"
#include "castor/job/stagerjob/InputArguments.hpp"

// Default number of retries and interval between retries
// for the sharedResourceHelper
#define DEFAULT_RETRY_ATTEMPTS 60
#define DEFAULT_RETRY_INTERVAL 10

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::job::stagerjob::InputArguments::InputArguments(int argc, char** argv)
  throw (castor::exception::Exception) :
  requestUuid(nullCuuid),
  subRequestUuid(nullCuuid),
  rawRequestUuid(""),
  rawSubRequestUuid(""),
  protocol("rfio"),
  subRequestId(0),
  type(0),
  diskServer(""),
  fileSystem(""),
  accessMode(castor::job::stagerjob::ReadOnly),
  client(0),
  isSecure(false),
  euid(0),
  egid(0),
  requestCreationTime(0),
  resourceFile(""),
  svcClass("") {

  // Initialize the Cns_fileid structure
  memset(&fileId, 0, sizeof(fileId));

  // Supported command line options
  Coptions_t longopts[] = {
    // These options are for logging purposes only!
    { "request",       REQUIRED_ARGUMENT, NULL, 'r' },
    { "subrequest",    REQUIRED_ARGUMENT, NULL, 's' },

    // The nameserver invariants
    { "fileid",        REQUIRED_ARGUMENT, NULL, 'F' },
    { "nshost",        REQUIRED_ARGUMENT, NULL, 'H' },

    // Resources
    { "svcclass",      REQUIRED_ARGUMENT, NULL, 'S' },
    { "resfile",       REQUIRED_ARGUMENT, NULL, 'R' },

    // Mover specific
    { "protocol",      REQUIRED_ARGUMENT, NULL, 'p' },
    { "subreqid",      REQUIRED_ARGUMENT, NULL, 'i' },
    { "reqtype",       REQUIRED_ARGUMENT, NULL, 'T' },
    { "mode",          REQUIRED_ARGUMENT, NULL, 'm' },
    { "clientstring",  REQUIRED_ARGUMENT, NULL, 'C' },
    { "euid",          REQUIRED_ARGUMENT, NULL, 'u' },
    { "egid",          REQUIRED_ARGUMENT, NULL, 'g' },
    { "secure",        REQUIRED_ARGUMENT, NULL, 'X' },

    // Logging/statistics
    { "rcreationtime", REQUIRED_ARGUMENT, NULL, 't' },
    { NULL,            0,                 NULL, 0   }
  };

  Coptind   = 1;
  Copterr   = 1;
  Coptreset = 1;

  // Parse command line arguments
  char c;
  while ((c = Cgetopt_long(argc, argv, "r:s:F:H:R:p:i:T:m:C:u:g:X:t:S:", longopts, NULL)) != -1) {
    switch (c) {
    case 'r':
      rawRequestUuid = Coptarg;
      string2Cuuid(&requestUuid, Coptarg);
      break;
    case 's':
      rawSubRequestUuid = Coptarg;
      string2Cuuid(&subRequestUuid, Coptarg);
      break;
    case 'F':
      fileId.fileid = strutou64(Coptarg);
      break;
    case 'H':
      strncpy(fileId.server, Coptarg, CA_MAXHOSTNAMELEN);
      fileId.server[CA_MAXHOSTNAMELEN] = '\0';
      break;
    case 'R':
      resourceFile = Coptarg;
      break;
    case 'p':
      protocol = Coptarg;
      if (protocol == "rfio3") protocol = "rfio";
      if (protocol!= "rfio" &&
          protocol!= "root" &&
          protocol!= "xroot" &&
          protocol!= "gsiftp") {
        castor::exception::InvalidArgument e;
        e.getMessage() << "Unsupported protocol " << protocol;
        throw e;
      }
      break;
    case 'i':
      subRequestId = strtou64(Coptarg);
      break;
    case 'T':
      type = atoi(Coptarg);
      if ((type < 0) || ((unsigned int)type > castor::ObjectsIdsNb)) {
        castor::exception::InvalidArgument e;
        e.getMessage() << "Invalid request type: " << type;
        throw e;
      }
      break;
    case 'm': {
        char *mode = Coptarg;
        if (mode[0] == 'r') {
          accessMode = castor::job::stagerjob::ReadOnly;
        } else if (mode[0] == 'w') {
          accessMode = castor::job::stagerjob::WriteOnly;
        } else if (mode[0] == 'o') {
          accessMode = castor::job::stagerjob::ReadWrite;
        } else {
          castor::exception::InvalidArgument e;
          e.getMessage() << "Invalid mode option: " << mode << ". "
                         << "Valid values are 'r', 'w' and 'o'";
          throw e;
        }
      }
      break;
    case 'C':
      client = castor::IClientFactory::string2Client(Coptarg);
      break;
    case 'u':
      euid = strtou64(Coptarg);
      break;
    case 'g':
      egid = strtou64(Coptarg);
      break;
    case 'X': {
        std::string secureFlag = Coptarg;
        if ((secureFlag == "1") || (secureFlag == "yes")) {
          isSecure = true;
        } else if ((secureFlag != "0") && (secureFlag != "no")) {
          castor::exception::InvalidArgument e;
          e.getMessage() << "Invalid secure option: " << secureFlag << ". "
                         << "Valid values are 'yes' (1) or 'no' (0)";
          throw e;
        }
      }
      break;
    case 't':
      requestCreationTime = strutou64(Coptarg);
      break;
    case 'S':
      svcClass = Coptarg;
      break;
    default:
      castor::exception::InvalidArgument e;
      e.getMessage() << "Unknown argument given";
      throw e;
    }
  }

  // Check that all mandatory command line options have been specified
  if (resourceFile.empty() || !fileId.fileid || !fileId.server[0] ||
      !client || !type || !subRequestId) {
    castor::exception::InvalidArgument e;
    e.getMessage() << "Mandatory command line arguments missing";
    throw e;
  }

  // Check to make sure the request and subrequest uuid's are set
  if (!Cuuid_compare(&requestUuid, &nullCuuid) ||
      !Cuuid_compare(&subRequestUuid, &nullCuuid)) {
    castor::exception::InvalidArgument e;
    e.getMessage() << "Invalid request and/or subrequest uuid";
    throw e;
  }

  // Don't allow to be run as root
  if ((euid == 0) || (egid == 0)) {
    castor::exception::InvalidArgument e;
    e.getMessage() << "Invalid euid and egid: (" << euid << ":" << egid << ")";
    throw e;
  }

  // Determine the number of times that the shared resource helper should try
  // to download the resource file from the shared resource URL
  char *value = getconfent("Job", "RetryAttempts", 0);
  int attempts = DEFAULT_RETRY_ATTEMPTS;
  if (value) {
    attempts = strtol(value, 0, 10);
    if (attempts < 1) {
      // "Invalid Job/RetryInterval option, using default"
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
  value = getconfent("Job", "RetryInterval", 0);
  int interval = DEFAULT_RETRY_INTERVAL;
  if (value) {
    interval = strtol(value, 0, 10);
    if (interval < 1) {
      // "Invalid Job/RetryAttempts option, using default"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Default", interval),
         castor::dlf::Param(subRequestUuid)};
      castor::dlf::dlf_writep
        (requestUuid, DLF_LVL_WARNING,
         castor::job::stagerjob::INVRETRYNBAT, 2, params, &fileId);
    }
  }

  // Download the resource file
  std::string content("");
  castor::job::SharedResourceHelper resHelper(attempts, interval);
  try {
    // "Downloading resource file"
    castor::dlf::Param params[] =
      {castor::dlf::Param("ResourceFile", resourceFile),
       castor::dlf::Param("MaxAttempts", resHelper.retryAttempts()),
       castor::dlf::Param("RetryInterval", resHelper.retryInterval()),
       castor::dlf::Param(subRequestUuid)};
    castor::dlf::dlf_writep
      (requestUuid, DLF_LVL_DEBUG, DOWNRESFILE, 4, params, &fileId);

    resHelper.setUrl(resourceFile);
    content = resHelper.download(false);
  } catch (castor::exception::Exception& e) {
    if (e.code() == EINVAL) {

      // "Invalid Uniform Resource Indicator, cannot download resource file"
      castor::dlf::Param params[] =
        {castor::dlf::Param("URI", resourceFile.substr(0, 7))};
      castor::dlf::dlf_writep
        (requestUuid, DLF_LVL_ERROR, INVALIDURI, 1, params, &fileId);
    } else if (e.code() == SERTYEXHAUST) {

      // "Exceeded maximum number of attempts trying to download resource file"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Error", resHelper.errorBuffer() != "" ?
                            resHelper.errorBuffer() : "no message"),
         castor::dlf::Param("MaxAttempts", resHelper.retryAttempts()),
         castor::dlf::Param("URL", resourceFile),
         castor::dlf::Param(subRequestUuid)};
      castor::dlf::dlf_writep
        (requestUuid, DLF_LVL_ERROR, MAXATTEMPTS, 4, params, &fileId);
    } else {

      // "Exception caught trying to download resource file"
      castor::dlf::Param params[] =
        {castor::dlf::Param("Message", e.getMessage().str()),
         castor::dlf::Param("Filename", resourceFile.substr(7)),
         castor::dlf::Param(subRequestUuid)};
      castor::dlf::dlf_writep
        (requestUuid, DLF_LVL_ERROR, DOWNEXCEPT, 3, params, &fileId);
    }
    throw e;
  }

  std::istringstream iss(content);
  std::getline(iss, diskServer, ':');
  std::getline(iss, fileSystem, '\n');

  // "The content of the resource file is invalid"
  if (iss.fail() || diskServer.empty() || fileSystem.empty()) {
    castor::dlf::Param params[] =
      {castor::dlf::Param("RequiredFormat", "diskserver:filesystem"),
       castor::dlf::Param(subRequestUuid)};
    castor::dlf::dlf_writep
      (requestUuid, DLF_LVL_ERROR, INVALRESCONT, 2, params, &fileId);

    castor::exception::Internal e;
    e.getMessage() << "Invalid resource file";
    throw e;
  }
}
