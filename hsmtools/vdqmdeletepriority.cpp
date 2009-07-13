/******************************************************************************
 *                      vdqmdeletepriority.cpp
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
#include "castor/vdqm/Constants.hpp"
#include "castor/vdqm/IVdqmSvc.hpp"
#include "h/Castor_limits.h"

#include <Cgetopt.h>
#include <iostream>
#include <string>
#include <string.h>


void usage(const std::string programName) {
  std::cerr << "Usage: " << programName <<
    " [-c config-file] -v VID -a mode [ -l type ] [ -h ]\n"
    "\n"
    "where options can be:\n"
    "\n"
    "\t-c, --config config-file  Configuration file\n"
    "\t-v, --vid VID             Volume visual Identifier\n"
    "\t-a, --tapeAccessMode mode Tape access mode. Valid values are \"read\"\n"
    "\t                          and \"write\"\n"
    "\t-l, --lifespanType type   Lifespan type. Valid values are\n"
    "\t                          \"singleMount\" and \"unlimited\".\n"
    "\t                          The default value is \"unlimited\".\n"
    "\t-h, --help                Print this help and exit\n"
    "\n"
    "Comments to: Castor.Support@cern.ch" << std::endl;
}


static struct Coptions longopts[] = {
  {"config"        , REQUIRED_ARGUMENT, NULL, 'c'},
  {"vid"           , REQUIRED_ARGUMENT, NULL, 'v'},
  {"tapeAccessMode", REQUIRED_ARGUMENT, NULL, 'a'},
  {"lifespanType"  , REQUIRED_ARGUMENT, NULL, 'l'},
  {"help"          , NO_ARGUMENT      , NULL, 'h'},
  {0, 0, 0, 0}
};


void parseCommandLine(int argc, char **argv, std::string &vid, int &tpMode,
  int &lifespanType) {

  bool vidSet    = false;
  bool tpModeSet = false;
  char c;


  Coptind = 1;
  Copterr = 0;

  while ((c = Cgetopt_long (argc, argv, "c:v:a:l:h", longopts, NULL)) != -1) {
    switch (c) {
    case 'c':
      {
        FILE *fp = fopen(Coptarg,"r");
        if(fp) {
          // The configuration file exists
          fclose(fp);
        } else {
          // The configuration files does not exist
          std::cerr
            << std::endl
            << "Error: Configuration file \"" << Coptarg
            << "\" does not exist"
            << std::endl << std::endl;
          usage(argv[0]);
          exit(1);
        }
      }
      setenv("PATH_CONFIG", Coptarg, 1);
      break;
    case 'v':
      vid    = Coptarg;
      vidSet = true;
      break;
    case 'a':
      if(strcmp(Coptarg,"read") == 0) {
        tpMode = 0;
        tpModeSet = true;
      } else if (strcmp(Coptarg,"write") == 0) {
        tpMode = 1;
        tpModeSet = true;
      } else {
        std::cerr
          << std::endl
          << "Error: Invalid tape access mode: " << Coptarg
          << std::endl << std::endl;
        usage(argv[0]);
        exit(1);
      }
      break;
    case 'l':
      if(strcmp(Coptarg, "singleMount") == 0) {
        lifespanType = 0; // single-mount
      } else if(strcmp(Coptarg, "unlimited") == 0) {
        lifespanType = 1; // unlimited
      } else {
        std::cerr
          << std::endl
          << "Error: Invalid lifespanType: " << Coptarg
          << std::endl << std::endl;
        usage(argv[0]);
        exit(1);
      }
      break;
    case 'h':
      usage(argv[0]);
      exit(0);
    case '?':
      std::cerr
        << std::endl
        << "Error: Unknown command-line option"
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

  if(!(vidSet && tpModeSet)) {
    std::cerr
      << std::endl
      << "Error:  VID and tape access mode must be provided"
      << std::endl << std::endl;
    usage(argv[0]);
    exit(1);
  }
}


castor::vdqm::IVdqmSvc *retrieveVdqmSvc() {
  castor::Services *svcs = NULL;

  // Retrieve a Services object
  try {
    svcs = castor::BaseObject::sharedServices();
  } catch(castor::exception::Exception &ex) {
    std::cerr
      << std::endl
      << "Failed to retrieve a services object: "
      << ex.getMessage().str()
      << std::endl << std::endl;
    exit(1);
  }
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
  try {
    svc = svcs->service("DbParamsSvc", castor::SVC_DBPARAMSSVC);
  } catch(castor::exception::Exception &ex) {
    std::cerr
      << std::endl
      << "Failed to retrieve a DB parameters service: "
      << ex.getMessage().str()
      << std::endl << std::endl;
    exit(1);
  }
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
  paramsSvc->setSchemaVersion(castor::vdqm::VDQMSCHEMAVERSION);
  paramsSvc->setDbAccessConfFile(ORAVDQMCONFIGFILE);

  // Retrieve the VDQM DB service
  try {
    svc = svcs->service("DbVdqmSvc", castor::SVC_DBVDQMSVC);
  } catch(castor::exception::Exception &ex) {
    std::cerr
      << std::endl
      << "Failed to retrieve the VDQM DB service: "
      << ex.getMessage().str()
      << std::endl << std::endl;
    exit(1);
  }
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
  int         lifespanType = 1; // Default = unlimited = 1

  parseCommandLine(argc, argv, vid, tpMode, lifespanType);

  // Retrieve the VDQM DB service
  castor::vdqm::IVdqmSvc *const vdqmSvc = retrieveVdqmSvc();

  try {
    int priority           = 0;
    int clientUID          = 0;
    int clientGID          = 0;
    std::string clientHost = "";

    const u_signed64 volPriorityId = vdqmSvc->deleteVolPriority(vid, tpMode,
      lifespanType, &priority, &clientUID, &clientGID, &clientHost);
    vdqmSvc->commit();

    // If no volume priority was deleted
    if(volPriorityId == 0) {
      std::cerr
        << std::endl
        << "No volume priority was deleted"
        << std::endl << std::endl;
      exit(1);
    }
  } catch(castor::exception::Exception &e) {
    std::cerr
      << std::endl
      << "Failed to delete volume priority: " << e.getMessage().str()
      << std::endl << std::endl;
  }

  return 0;
}
