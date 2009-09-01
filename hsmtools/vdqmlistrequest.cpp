/******************************************************************************
 *                      vdqmlistrequest.cpp
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
#include "castor/db/DbParamsSvc.hpp"
#include "castor/vdqm/Constants.hpp"
#include "castor/vdqm/IVdqmSvc.hpp"
#include "h/Castor_limits.h"

#include <Cgetopt.h>
#include <iostream>
#include <memory>
#include <string>
#include <sstream>
#include <string.h>
#include <time.h>


void usage(const std::string programName) {
  std::cerr << "Usage: " << programName <<
    " [-c config-file] [ -v ] [ -h ]\n"
    "\n"
    "where options can be:\n"
    "\n"
    "\t-c, --config config-file Configuration file\n"
    "\t-v, --verbose Display column heads.\n"
    "\t-h, --help    Print this help and exit.\n"
    "\n"
    "Comments to: Castor.Support@cern.ch" << std::endl;
}


static struct Coptions longopts[] = {
  {"config" , REQUIRED_ARGUMENT, NULL, 'c'},
  {"verbose", NO_ARGUMENT      , NULL, 'v'},
  {"help"   , NO_ARGUMENT      , NULL, 'h'},
  {0, 0, 0, 0}
};


void parseCommandLine(int argc, char **argv, bool &displayColumnHeadings) {
  Coptind  = 1;
  Copterr  = 0;

  char c;


  while ((c = Cgetopt_long (argc, argv, "c:vh", longopts, NULL)) != -1) {
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
  paramsSvc->setSchemaVersion(castor::vdqm::VDQMSCHEMAVERSION);
  paramsSvc->setDbAccessConfFile(ORAVDQMCONFIGFILE);

  // Retrieve the VDQM DB service
  try {
    svc = svcs->service("DbVdqmSvc", castor::SVC_DBVDQMSVC);
  } catch(castor::exception::Exception &ex) {
    std::cerr
      << std::endl
      << "Failed to retrieve the VDQM DB service"
      << ": " << ex.getMessage().str()
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


void addPadding(std::ostream &os, const int nbSpaces) {
  for(int i=0; i<nbSpaces; i++) {
    os << " ";
  }
}


void addPaddedNumColumn(std::ostream &os, const u_signed64 number,
  const int width) {
  std::stringstream oss;

  // Convert the number to a string
  oss << number;

  // Calculate the necessary padding
  int len = oss.str().size();
  int padding = 0;
  if(width > len) {
    padding = width - len;
  }

  // Write the padding to the stream
  addPadding(os, padding);

  // Write the number as a string to the stream
  os << oss.str();
}


void addPaddedTxtColumn(std::ostream &os, std::string text, const int width) {
  // Write the text to the stream
  os << text;

  // Calculate the necessary padding
  int len = text.size();
  int padding = 0;
  if(width > len) {
    padding = width - len;
  }

  // Write the padding to the stream
  addPadding(os, padding);
}


void printRequestList(castor::vdqm::IVdqmSvc::VolRequestList &requests,
  const bool displayColumnHeadings) {
  time_t t = 0;
  struct tm *tp = NULL;
  char strftime_format[] = "%b %d %H:%M:%S";
  char timestr[64]; // Time in its ASCII format


  if(displayColumnHeadings) {
    std::cout << "REQID   "         //  8 characters wide
                 "DGN     "         //  8 characters wide
                 "VID     "         //  8 characters wide
                 "MODE    "         //  8 characters wide
                 "CREATED         " // 16 characters wide
                 "REMOTECOPYTYPE  " // 16 characters wide
                 "PRIORITY"         //  8 characters wide
      << std::endl;
  }

  for(castor::vdqm::IVdqmSvc::VolRequestList::iterator itor =
    requests.begin(); itor != requests.end(); itor++) {
    addPaddedNumColumn(std::cout, (*itor)->id    , 7);
    std::cout << " ";
    addPaddedTxtColumn(std::cout, (*itor)->dgName, 8);
    addPaddedTxtColumn(std::cout, (*itor)->vid   , 8);
    switch((*itor)->accessMode) {
    case 0: // read
      std::cout << "read    "; // 8 characters wide
      break;
    case 1: // write
      std::cout << "write   "; // 8 characters wide
      break;
    default:
      std::cout << "UNKNOWN "; // 8 characters wide
    }
    t = (*itor)->creationTime;
    tp = localtime(&t);
    strftime(timestr,sizeof(timestr),strftime_format, tp);
    timestr[sizeof(timestr)-1] = '\0';
    addPaddedTxtColumn(std::cout, timestr                , 16);
    addPaddedTxtColumn(std::cout, (*itor)->remoteCopyType, 16);
    addPaddedNumColumn(std::cout, (*itor)->volumePriority,  8);
    std::cout << std::endl;
  }
}


int main(int argc, char **argv) {
  bool displayColumnHeadings = false;

  parseCommandLine(argc, argv, displayColumnHeadings);

  // Retrieve the VDQM DB service
  castor::vdqm::IVdqmSvc *const vdqmSvc = retrieveVdqmSvc();

  try {
    std::string dgn          = "";
    std::string requestedSrv = "";

    // Get and print the list of volume requests
    castor::vdqm::IVdqmSvc::VolRequestList requests;
    vdqmSvc->getVolRequestsPriorityOrder(requests, dgn, requestedSrv);
    printRequestList(requests, displayColumnHeadings);
  } catch(castor::exception::Exception &e) {
    std::cerr
      << std::endl
      << "Failed to get the list of volume requests: " << e.getMessage().str()
      << std::endl << std::endl;
    exit(1);
  }

  return 0;
}
