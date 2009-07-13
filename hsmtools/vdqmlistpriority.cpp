/******************************************************************************
 *                      vdqmlistpriority.cpp
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
#include <memory>
#include <string>
#include <string.h>


// Possible types of volume priority list that can be displayed
enum PriorityListType {
  NONE_PRIO_LIST_TYPE,
  ALL_PRIO_LIST_TYPE,
  EFFECTIVE_PRIO_LIST_TYPE,
  LIFESPAN_TYPE_PRIO_LIST_TYPE
};


void usage(const std::string programName) {
  std::cerr << "Usage: " << programName <<
    " [-c config-file] [ -a | -l lifespanType | -e ] [ -v ] [ -h ]\n"
    "\n"
    "where options can be:\n"
    "\n"
    "\t-c, --config config-file Configuration file\n"
    "\t-a, --all                List all priorities. This is the default.\n"
    "\t-l, --lifespanType type  List priorities with specified lifespan type.\n"
    "\t                         Valid values are \"singleMount\" and "
    "\"unlimited\".\n"
    "\t-e, --effective          List effective priorities.\n"
    "\t-v, --verbose            Display column heads.\n"
    "\t-h, --help               Print this help and exit.\n"
    "\n"
    "Please note that options -a, -l and -e are mutually exclusive\n"
    "\n"
    "Comments to: Castor.Support@cern.ch" << std::endl;
}


static struct Coptions longopts[] = {
  {"config"      , REQUIRED_ARGUMENT, NULL, 'c'},
  {"all"         , NO_ARGUMENT      , NULL, 'a'},
  {"lifespanType", REQUIRED_ARGUMENT, NULL, 'l'},
  {"effective"   , NO_ARGUMENT      , NULL, 'e'},
  {"verbose"     , NO_ARGUMENT      , NULL, 'v'},
  {"help"        , NO_ARGUMENT      , NULL, 'h'},
  {0, 0, 0, 0}
};


void parseCommandLine(int argc, char **argv, PriorityListType &listType,
  int &lifespanType, bool &displayColumnHeadings) {

  int nbListTypesSet = 0;
  char c;


  listType = NONE_PRIO_LIST_TYPE;
  Coptind  = 1;
  Copterr  = 0;

  while ((c = Cgetopt_long(argc, argv, "c:al:evh", longopts, NULL)) != -1) {
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
    case 'a':
      listType = ALL_PRIO_LIST_TYPE;
      nbListTypesSet++;
      break;
    case 'l':
      listType = LIFESPAN_TYPE_PRIO_LIST_TYPE;
      nbListTypesSet++;
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
    case 'e':
      listType = EFFECTIVE_PRIO_LIST_TYPE;
      nbListTypesSet++;
      break;
    case 'v':
      displayColumnHeadings = true;
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

  // List types are mutually exclusive, i.e. only one type of list can be
  // printed
  if(nbListTypesSet > 1) {
    std::cerr
      << std::endl
      << "Error:  More than one type of list. -a, -e and -l are mutually"
         " exclusive"
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


void printPriorityList(std::list<castor::vdqm::IVdqmSvc::VolPriority>
  &priorities, const bool displayColumnHeadings) {
    if(displayColumnHeadings) {
      std::cout << "VID\tMODE\tLIFESPAN\tPRIORITY" << std::endl;
    }

    for(std::list<castor::vdqm::IVdqmSvc::VolPriority>::iterator itor =
      priorities.begin(); itor != priorities.end(); itor++) {
      std::cout << itor->vid;
      std::cout << "\t";
      switch(itor->tpMode) {
      case 0:
        std::cout << "read";
        break;
      case 1:
        std::cout << "write";
        break;
      default:
        std::cout << "UNKNOWN";
      }
      std::cout << "\t";
      switch(itor->lifespanType) {
      case 0:
        std::cout << "singleMount";
        break;
      case 1:
        std::cout << "unlimited";
        break;
      default:
        std::cout << "UNKNOWN";
      }
      std::cout << "\t";
      std::cout << itor->priority;
      std::cout << std::endl;
    }
}


void printAllPriorities(castor::vdqm::IVdqmSvc *const vdqmSvc,
  const bool displayColumnHeadings) {
  try {
    // Get and print the list of volume priorities
    std::list<castor::vdqm::IVdqmSvc::VolPriority> priorities;
    vdqmSvc->getAllVolPriorities(priorities);
    printPriorityList(priorities, displayColumnHeadings);
  } catch(castor::exception::Exception &e) {
    std::cerr
      << std::endl
      << "Failed to get the list of volume priorities: " << e.getMessage().str()
      << std::endl << std::endl;
    exit(1);
  }
}


void printEffectivePriorities(castor::vdqm::IVdqmSvc *const vdqmSvc,
  const bool displayColumnHeadings) {
  try {
    // Get and print the list of volume priorities
    std::list<castor::vdqm::IVdqmSvc::VolPriority> priorities;
    vdqmSvc->getEffectiveVolPriorities(priorities);
    printPriorityList(priorities, displayColumnHeadings);
  } catch(castor::exception::Exception &e) {
    std::cerr
      << std::endl
      << "Failed to get the list of volume priorities: " << e.getMessage().str()
      << std::endl << std::endl;
    exit(1);
  }
}


void printLifespanTypePriorities(castor::vdqm::IVdqmSvc *const vdqmSvc,
  const int lifespanType, const bool displayColumnHeadings) {
  try {
    // Get and print the list of volume priorities
    std::list<castor::vdqm::IVdqmSvc::VolPriority> priorities;
    vdqmSvc->getVolPriorities(priorities, lifespanType);
    printPriorityList(priorities, displayColumnHeadings);
  } catch(castor::exception::Exception &e) {
    std::cerr
      << std::endl
      << "Failed to get the list of volume priorities: " << e.getMessage().str()
      << std::endl << std::endl;
    exit(1);
  }
}


int main(int argc, char **argv) {
  PriorityListType listType     = NONE_PRIO_LIST_TYPE;
  int              lifespanType = 0; // 0 = single-mount
  bool             displayColumnHeadings = false;


  parseCommandLine(argc, argv, listType, lifespanType, displayColumnHeadings);

  // Retrieve the VDQM DB service
  castor::vdqm::IVdqmSvc *const vdqmSvc = retrieveVdqmSvc();

  switch(listType) {
  case NONE_PRIO_LIST_TYPE:
    // The default is to display all priorities
  case ALL_PRIO_LIST_TYPE:
    printAllPriorities(vdqmSvc, displayColumnHeadings);
    break;
  case EFFECTIVE_PRIO_LIST_TYPE:
    printEffectivePriorities(vdqmSvc, displayColumnHeadings);
    break;
  case LIFESPAN_TYPE_PRIO_LIST_TYPE:
    printLifespanTypePriorities(vdqmSvc, lifespanType, displayColumnHeadings);
    break;
  default:
    std::cerr
      << std::endl
      << "Internal error: Unknown listType: " << listType
      << std::endl << std::endl;
    exit(1);
  }

  return 0;
}
