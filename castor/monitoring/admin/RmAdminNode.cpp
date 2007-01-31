/******************************************************************************
 *                              RmAdminNode.cpp
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
 * @(#)$RCSfile: RmAdminNode.cpp,v $ $Revision: 1.2 $ $Release$ $Date: 2007/01/31 17:18:06 $ $Author: sponcec3 $
 *
 * command line that allows to change a node status and admin status in RmMaster
 *
 * @author sponcec3
 *****************************************************************************/

#include <iostream>
#include <stdlib.h>
#include <string.h>
#include "common.h"
#include <Cgetopt.h>
#include <castor/IObject.hpp>
#include <castor/MessageAck.hpp>
#include <castor/io/ClientSocket.hpp>
#include <castor/monitoring/AdminStatusCodes.hpp>
#include <castor/stager/FileSystemStatusCodes.hpp>
#include <castor/monitoring/admin/DiskServerAdminReport.hpp>
#include <castor/monitoring/admin/FileSystemAdminReport.hpp>

#define RMMASTER_DEFAULT_PORT 15003

static struct Coptions longopts[] = {
  {"help",        NO_ARGUMENT,       NULL, 'h'},
  {"host",        REQUIRED_ARGUMENT, NULL, 'o'},
  {"port",        REQUIRED_ARGUMENT, NULL, 'p'},
  {"node",        REQUIRED_ARGUMENT, NULL, 'n'},
  {"state",       REQUIRED_ARGUMENT, NULL, 's'},
  {"adminState",  REQUIRED_ARGUMENT, NULL, 'a'},
  {"mountPoint",  REQUIRED_ARGUMENT, NULL, 'm'},
  {"fsState",     REQUIRED_ARGUMENT, NULL, 't'},
  {"fsAdminState",REQUIRED_ARGUMENT, NULL, 'd'},
  {NULL, 0, NULL, 0}
};

void usage(char *cmd) {
  std::cerr << "usage : " << cmd << " [options] where options are :\n"
            << "-h,--help displays this help\n"
            << "--host <Rmmaster host>\n"
            << "--port <Rmmaster port>\n"
            << "-n, --node <Name> of the node to administrate. Required.\n"
            << "-s, --state {state} New node's state\n"
            << "-a, --adminState {adminState} New node's state\n"
            << "-m, --mountPoint <Filesystem mountPoint> Filesystem on node\n"
            << "-t, --fsState {fsstate} New filesystem's state\n"
            << "-d, --fsAdminState {fsstate} New filesystem's state\n"
            << "Notes:\n"
            << "\t{state} and {fsState} can be: \"Production\", \"Draining\" or \"Disabled\"\n"
            << "\t{adminState} and {fsAdminState} can be: \"None\", \"Force\" or \"Release\"\n"
	    << "\tdefault states (when not provided) are \"Production\" and \"None\"\n"
            << "\tmountPoints usually end with a '/', for example /data01/\n"
            << "\tonly the node name is required, not giving any mountPoint will not change any fileSystem state\n"
            << std::endl;
}

int main(int argc, char *argv[]) {

  try {
    
    // global variable declarations
    char* progName = argv[0];
    char* rmHostName = 0;
    int rmPort = -1;
    char* nodeName = 0;
    char* stateName = 0;
    char* adminStateName = 0;
    char* mountPoint = 0;
    char* fsStateName = 0;
    char* fsAdminStateName = 0;
    
    // Deal with options
    Coptind = 1;
    Copterr = 1;
    int ch;
    while ((ch = Cgetopt_long(argc,argv,"hn:s:a:m:t:d:",longopts,NULL)) != EOF) {
      switch (ch) {
      case 'h':
	usage(progName);
	return 0;
      case 'o':
	rmHostName = Coptarg;
	break;
      case 'p':
	rmPort = atoi(Coptarg);
	break;
      case 'n':
	nodeName = Coptarg;
	break;
      case 's':
	stateName = Coptarg;
	break;
      case 'a':
	adminStateName = Coptarg;
	break;
      case 'm':
	mountPoint = Coptarg;
	break;
      case 't':
	fsStateName = Coptarg;
	break;
      case 'd':
	fsAdminStateName = Coptarg;
	break;
      case '?':
        usage(progName);
        return 1;
      default:
        break;
      }
    }

    // Parse the arguments
    argc -= Coptind;
    argv += Coptind;
    if (argc != 0) {
      std::cerr << "Error : arguments were given and will be ignored" << std::endl;
    }
    if (0 == nodeName) {
      std::cerr << "Missing node name. Please use -n,--node option !" << std::endl;
      exit(1);
    }
    if ((0 != mountPoint) &&
	((0 != stateName) || (0 != adminStateName))) {
      std::cerr << "One cannot set the status of a node and a deal with a "
		<< "fileSystem at the same time !" << std::endl;
      exit(1);      
    }
    castor::stager::DiskServerStatusCode state = castor::stager::DISKSERVER_PRODUCTION;
    if (0 != stateName) {
      if (0 == strcmp(stateName, "Draining")) {
	state = castor::stager::DISKSERVER_DRAINING;
      } else if (0 == strcmp(stateName, "Disabled")) {
	state = castor::stager::DISKSERVER_DISABLED;
      } else if (0 != strcmp(stateName, "Production")) {
	// Error
	std::cerr << "Invalid node status '" << stateName << "'\n"
		  << "Valid status are \"Production\"(default), \"Draining\" and \"Disabled\""
		  << std::endl;
	exit(1);
      }
    }
    castor::monitoring::AdminStatusCodes adminState = castor::monitoring::ADMIN_NONE;
    if (0 != adminStateName) {
      if (0 == strcmp(adminStateName, "Force")) {
	adminState = castor::monitoring::ADMIN_FORCE;
      } else if (0 == strcmp(adminStateName, "Release")) {
	adminState = castor::monitoring::ADMIN_RELEASE;
      } else {
	// Error
	std::cerr << "Invalid node admin status '" << adminStateName << "'\n"
		  << "Valid status are \"Force\" and \"Release\""
		  << std::endl;
	exit(1);
      }
    }
    castor::stager::FileSystemStatusCodes fsState = castor::stager::FILESYSTEM_PRODUCTION;
    if (0 != fsStateName) {
      if (0 == strcmp(fsStateName, "Draining")) {
	fsState = castor::stager::FILESYSTEM_DRAINING;
      } else if (0 == strcmp(fsStateName, "Disabled")) {
	fsState = castor::stager::FILESYSTEM_DISABLED;
      } else if (0 != strcmp(fsStateName, "Production")) {
	// Error
	std::cerr << "Wrong mountPoint status '" << fsStateName << "'\n"
		  << "Valid status are \"Production\"(default), \"Draining\" and \"Disabled\""
		  << std::endl;
	exit(1);
      }
    }
    castor::monitoring::AdminStatusCodes fsAdminState = castor::monitoring::ADMIN_NONE;
    if (0 != fsAdminStateName) {
      if (0 == strcmp(fsAdminStateName, "Force")) {
	fsAdminState = castor::monitoring::ADMIN_FORCE;
      } else if (0 == strcmp(fsAdminStateName, "Release")) {
	fsAdminState = castor::monitoring::ADMIN_RELEASE;
      } else if (0 != strcmp(fsAdminStateName, "None")) {
	// Error
	std::cerr << "Invalid fileSystem Admin status '" << fsAdminStateName << "'\n"
		  << "Valid status are \"None\"(default), \"Force\" and \"Release\""
		  << std::endl;
	exit(1);
      }
    }

    // Get default values for some arguments when needed
    // RmMaster Host Name
    if (0 == rmHostName) {
      // check environment
      rmHostName = getenv ("RM_HOST");
      if (0 == rmHostName) {
	// check configuration file
	rmHostName = getconfent ("RM", "HOST", 0);
	if (0 == rmHostName) {
	  // error
	  std::cerr << "Error : Could not figure out the RmMaster host\n"
		    << "Please use --host option, set RM_HOST environment variable "
		    <<	"or add and entry for RM/HOST in configuration file" << std::endl;
	  exit(1);
	}
      }
    }
    // RmMaster Port
    if (-1 ==  rmPort) {
      char* p;
      // check environment
      p = getenv("RM_PORT");
      if (0 == p) {
	// check configuration file
	p = getconfent ("RM", "PORT", 0);
	if (0 == p) {
	  // take default
	  rmPort = RMMASTER_DEFAULT_PORT;
	} else {
	  rmPort = atoi(p);
	}
      } else {
	rmPort = atoi(p);
      }
    }

    // build request
    castor::IObject* obj;
    if (0 == mountPoint) {
      castor::monitoring::admin::DiskServerAdminReport* report =
	new castor::monitoring::admin::DiskServerAdminReport();
      obj = report;
      report->setDiskServerName(nodeName);
      report->setAdminStatus(adminState);
      report->setStatus(state);
    } else {
      castor::monitoring::admin::FileSystemAdminReport* report =
	new castor::monitoring::admin::FileSystemAdminReport();
      obj = report;
      report->setDiskServerName(nodeName);
      report->setMountPoint(mountPoint);
      report->setAdminStatus(fsAdminState);
      report->setStatus(fsState);
    }

    // send request to rmMaster
    castor::io::ClientSocket s(rmPort, rmHostName);
    s.connect();
    s.sendObject(*obj);

    // check the acknowledgment
    castor::IObject* ackObj = s.readObject();
    castor::MessageAck* ack =
      dynamic_cast<castor::MessageAck*>(ackObj);
    if (0 == ack) {
      std::cerr << "No Acknowledgement from the RmMaster\n"
	       << "Not sure the action was taken into account"
	       << std::endl;
      exit(1);
    }

  } catch (castor::exception::Exception e) {
    std::cerr << "Caught exception : "
              << e.getMessage().str() << std::endl;    
  } catch (std::exception e) {
    std::cerr << "Caught standard exception : "
              << e.what() << std::endl;    
  } catch (...) {
    std::cerr << "Caught unknown exception !";
  }
  
}
