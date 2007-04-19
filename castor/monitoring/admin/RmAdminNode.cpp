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
 * @(#)$RCSfile: RmAdminNode.cpp,v $ $Revision: 1.8 $ $Release$ $Date: 2007/04/19 13:22:47 $ $Author: sponcec3 $
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
  {"force",       REQUIRED_ARGUMENT, NULL, 'f'},
  {"release",     NO_ARGUMENT,       NULL, 'r'},
  {"delete",      NO_ARGUMENT,       NULL, 'd'},
  {"mountPoint",  REQUIRED_ARGUMENT, NULL, 'm'},
  {"recursive",   NO_ARGUMENT,       NULL, 'R'},
  {NULL, 0, NULL, 0}
};

void usage(char *cmd) {
  std::cerr << "usage : " << cmd
            << "[-h] "
            << "[--host <Rmmaster host>] "
            << "[--port <Rmmaster port>] "
            << "-n <name> "
            << "[-f <state> "
            << "| -r "
            << "| -d] "
            << "[-m <mountPoint>] "
            << "[-R]\n"
            << "  where state can be \"Production\", \"Draining\" or \"Disabled\""
            << std::endl;
}

int main(int argc, char *argv[]) {

  try {
    
    // global variable declarations
    char* progName = argv[0];
    char* rmHostName = 0;
    int rmPort = -1;
    char* nodeName = 0;
    bool force = false;
    char* stateName = 0;
    bool release = false;
    bool deleteFlag = false;
    bool recursive = false;
    char* mountPoint = 0;

    // issue help if no options given
    if (1 == argc) {
      usage(progName);
      return 0;
    }

    // Deal with options
    Coptind = 1;
    Copterr = 1;
    int ch;
    while ((ch = Cgetopt_long(argc,argv,"ho:p:n:f:rdm:R",longopts,NULL)) != EOF) {
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
      case 'f':
	stateName = Coptarg;
	force = true;
	if (release || deleteFlag) {
	  std::cerr << "Error : incompatible options used (force and "
		    << (release ? "release" : "delete")
		    << ")" << std::endl;
	  usage(progName);
	  return -1;
	}
	break;
      case 'r':
	release = true;
	if (force || deleteFlag) {
	  std::cerr << "Error : incompatible options used (release and "
		    << (force ? "force" : "delete")
		    << ")" << std::endl;
	  usage(progName);
	  return -1;
	}
	break;
      case 'd':
	deleteFlag = true;
	if (release || force) {
	  std::cerr << "Error : incompatible options used (delete and "
		    << (release ? "release" : "force")
		    << ")" << std::endl;
	  usage(progName);
	  return -1;
	}
	break;
      case 'm':
	mountPoint = Coptarg;
	break;
      case 'R':
	recursive = true;
	break;
      case '?':
	std::cerr << "Unable to parse options" << std::endl;
        usage(progName);
        return 1;
      default:
        break;
      }
    }
    // Compute the admin state
    castor::monitoring::AdminStatusCodes adminState = castor::monitoring::ADMIN_NONE;
    if (force) {
      adminState = castor::monitoring::ADMIN_FORCE;
    } else if (release) {
      adminState = castor::monitoring::ADMIN_RELEASE;
    } else if (deleteFlag) {
      adminState = castor::monitoring::ADMIN_DELETED;
    } else {
      // Error
      std::cerr << "Missing option : one of force, release or delete must be given"
		<< std::endl;
      return -1;
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

    // build request
    castor::IObject* obj;
    if (0 == mountPoint) {
      castor::monitoring::admin::DiskServerAdminReport* report =
	new castor::monitoring::admin::DiskServerAdminReport();
      obj = report;
      report->setDiskServerName(nodeName);
      report->setAdminStatus(adminState);
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
      report->setStatus(state);
      report->setRecursive(recursive);
    } else {
      castor::monitoring::admin::FileSystemAdminReport* report =
	new castor::monitoring::admin::FileSystemAdminReport();
      obj = report;
      report->setDiskServerName(nodeName);
      report->setMountPoint(mountPoint);
      report->setAdminStatus(adminState);
      castor::stager::FileSystemStatusCodes state = castor::stager::FILESYSTEM_PRODUCTION;
      if (0 != stateName) {
	if (0 == strcmp(stateName, "Draining")) {
	  state = castor::stager::FILESYSTEM_DRAINING;
	} else if (0 == strcmp(stateName, "Disabled")) {
	  state = castor::stager::FILESYSTEM_DISABLED;
	} else if (0 != strcmp(stateName, "Production")) {
	  // Error
	  std::cerr << "Invalid node status '" << stateName << "'\n"
		    << "Valid status are \"Production\"(default), \"Draining\" and \"Disabled\""
		    << std::endl;
	  exit(1);
	}
      }
      report->setStatus(state);
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
    if (!ack->status()) {
      std::cerr << strerror(ack->errorCode()) << "\n"
		<< ack->errorMessage() << std::endl;
    }
  } catch (castor::exception::Exception e) {
    std::cerr << e.getMessage().str() << std::endl;    
  } catch (std::exception e) {
    std::cerr << "Caught standard exception : "
              << e.what() << std::endl;    
  } catch (...) {
    std::cerr << "Caught unknown exception !";
  }
  
}
