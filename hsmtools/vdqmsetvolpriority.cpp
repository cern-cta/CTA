/******************************************************************************
 *                      vdqmsetvolpriority.cpp
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
 *
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "castor/BaseObject.hpp"
#include "castor/Constants.hpp"
#include "castor/Services.hpp"
#include "castor/Services.hpp"
#include "castor/db/DbParamsSvc.hpp"
#include "castor/vdqm/IVdqmSvc.hpp"
#include "h/Castor_limits.h"

#include <Cgetopt.h>
#include <iostream>
#include <string>
#include <string.h>
#include <unistd.h>


const std::string VDQMSCHEMAVERSION = "2_1_7_4";


void usage(const std::string programName) {
  std::cerr << "Usage: " << programName << " [options]\n"
    "\n"
    "where options can be:\n"
    "\n"
    "\t-v, --vid VID             Volume visual Identifier\n"
    "\t-a, --tapeAccessMode mode Tape access mode 0 or 1\n"
    "\t-p, --priority priority   Volume priority between 0 and INT_MAX\n"
    "\t-h, --help                Print this help and exit\n"
    "\n"
    "Comments to: Castor.Support@cern.ch" << std::endl;
}


static struct Coptions longopts[] = {
  {"vid"           , REQUIRED_ARGUMENT, NULL, 'v'},
  {"tapeAccessMode", REQUIRED_ARGUMENT, NULL, 'a'},
  {"priority"      , REQUIRED_ARGUMENT, NULL, 'p'},
  {"help"          , NO_ARGUMENT      , NULL, 'h'}
};


void parseCommandLine(int argc, char **argv, std::string &vid, int &tpMode,
  int &priority) {

  bool vidSet      = false;
  bool tpModeSet   = false;
  bool prioritySet = false;
  char c;


  Coptind = 1;
  Copterr = 0;

  while ((c = Cgetopt_long (argc, argv, "v:a:p:h", longopts, NULL)) != -1) {
    switch (c) {
    case 'v':
      vid    = Coptarg;
      vidSet = true;
      break;
    case 'a':
      if((strcmp(Coptarg,"0") != 0) && (strcmp(Coptarg,"1") != 0)) {
        std::cerr
          << std::endl
          << "Error: Invalid tape access mode: " << Coptarg
          << std::endl << std::endl;
        usage(argv[0]);
        exit(1);
      }
      tpMode = atoi(Coptarg);
      tpModeSet = true;
      break;
    case 'p':
      priority = atoi(Coptarg);
      if(priority < 0) {
        std::cerr
          << std::endl
          << "Error: Invalid priority: " << priority
          << std::endl << std::endl;
        usage(argv[0]);
        exit(1);
      }
      prioritySet = true;
      break;
    case 'h':
      usage(argv[0]);
      exit(0);
    case '?':
      std::cerr
        << std::endl
        << "Error: Unknown command-line option: " << (char)Coptopt
        << std::endl << std::endl;
      usage(argv[0]);
      exit(1);
    case ':':
      std::cerr
        << std::endl
        << "Error: An option is missing a parameter"
        << std::endl << std::endl;
      usage(argv[0]);
      exit(1);
    default:
      std::cerr
        << std::endl
        << "Internal error: Cgetopt_long returned the following unknown value: "
        << "0x" << std::hex << (int)c << std::dec
        << std::endl << std::endl;
      exit(1);
    }
  }

  if(Coptind > argc) {
    std::cerr
      << std::endl
      << "Internal error.  Invalid value for Coptind: " << Coptind
      << std::endl;
    exit(1);
  }

  // Best to abort if there is some extra text on the command-line which has
  // not been parsed as it could indicate that a valid option never got parsed
  if(Coptind < argc)
  {
    std::cerr
      << std::endl
      << "Error:  Unexpected command-line argument: "
      << argv[Coptind]
      << std::endl << std::endl;
    usage(argv[0]);
    exit(1);
  }

  if(!(vidSet && tpModeSet && prioritySet)) {
    std::cerr
      << std::endl
      << "Error:  VID, tape access mode and priority must be provided"
      << std::endl << std::endl;
    usage(argv[0]);
    exit(1);
  }
}


castor::vdqm::IVdqmSvc *retrieveVdqmSvc() {
  // Retrieve a Services object
  castor::Services *svcs = castor::BaseObject::sharedServices();
  if(svcs == NULL) {
    std::cerr
      << std::endl
      << "Failed to retrieve Services object"
      << std::endl << std::endl;
    exit(1);
  }

  // General purpose pointer to a service
  castor::IService* svc = NULL;

  // Retrieve a DB parameters service
  svc = svcs->service("DbParamsSvc", castor::SVC_DBPARAMSSVC);
  if(svc == NULL) {
    std::cerr
      << std::endl
      << "Failed to retrieve DB parameters service"
      << std::endl << std::endl;
    exit(1);
  }
  castor::db::DbParamsSvc* paramsSvc =
    dynamic_cast<castor::db::DbParamsSvc*>(svc);
  if(paramsSvc == NULL) {
    std::cerr
      << std::endl
      << "Failed to dynamic cast the DB parameters service"
      << std::endl << std::endl;
    exit(1);
  }

  // Tell the DB parameter service of the VDQM schema version and the DB
  // connection details file
  paramsSvc->setSchemaVersion(VDQMSCHEMAVERSION);
  paramsSvc->setDbAccessConfFile(ORAVDQMCONFIGFILE);

  // Retrieve the VDQM DB service
  svc = svcs->service("DbVdqmSvc", castor::SVC_DBVDQMSVC);
  if(svc == NULL) {
    std::cerr
      << std::endl
      << "Failed to retrieve the VDQM DB service"
      << std::endl << std::endl;
    exit(1);
  }
  castor::vdqm::IVdqmSvc *vdqmSvc = dynamic_cast<castor::vdqm::IVdqmSvc*>(svc);
  if(vdqmSvc == NULL) {
    std::cerr
      << std::endl
      << "Failed to dynamic cast the VDQM DB service."
      << std::endl << std::endl;
    exit(1);
  }

  return vdqmSvc;
}


int main(int argc, char **argv) {
  std::string vid      = "";
  int         tpMode   = 0;
  int         priority = 0;

  parseCommandLine(argc, argv, vid, tpMode, priority);

  // Initializing the log
  castor::BaseObject::initLog(argv[0], castor::SVC_NOMSG);

  // Retrieve the VDQM DB service
  castor::vdqm::IVdqmSvc *vdqmSvc = retrieveVdqmSvc();

  try {
    // Get the standard host name for the current machine
    char hostname[CA_MAXHOSTNAMELEN];
    gethostname(hostname,CA_MAXHOSTNAMELEN);

    // Ensure the host name is null-terminated.
    hostname[sizeof(hostname)-1] = '\0';

    // Set the volume priority
    // Last parameter = lifespanType = unlimited = 1
    vdqmSvc->setVolPriority(priority, geteuid(), getegid(), hostname, vid,
      tpMode, 1);
  } catch(castor::exception::Exception &e) {
    std::cerr
      << std::endl
      << "Failed to set volume priority: " << e.getMessage().str()
      << std::endl << std::endl;
  }

  return 0;
}
