/******************************************************************************
 *                      listPriority.cpp
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
 * @(#)$RCSfile: listPriority.cpp,v $ $Revision: 1.2 $ $Release$ $Date: 2008/07/28 16:56:48 $ $Author: waldron $
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include <iostream>
#include <castor/stager/SvcClass.hpp>
#include <castor/stager/IStagerSvc.hpp>
#include <castor/Services.hpp>
#include <castor/BaseAddress.hpp>
#include <castor/Constants.h>
#include <Cgetopt.h>
#include <castor/BaseObject.hpp>
#include <castor/stager/PriorityMap.hpp>

static struct Coptions longopts[] = {
  { "help",     NO_ARGUMENT,       NULL, 'h'},
  { "uid",      REQUIRED_ARGUMENT, NULL, 'u'},
  { "gid",      REQUIRED_ARGUMENT, NULL, 'g'}, 
  { "priority", REQUIRED_ARGUMENT, NULL, 'p'}, 
  { NULL,       0,                 NULL,  0 }
};

void usage(char *cmd) {
  std::cout << "Usage : " << cmd
            << " [-h] [-u uid] [-g gid] [-p priority]"
            << std::endl;
}

int main(int argc, char *argv[]) {
  int muid = -1;
  int mgid = -1;
  int mpriority = -1;
    
  char* progName = argv[0];
  
  // Deal with options
  Coptind = 1;
  Copterr = 1;
  int ch;
  while ((ch = Cgetopt_long(argc, argv, "hu:g:p:", longopts, NULL)) != EOF) {
    switch (ch) {
    case 'h':
      usage(progName);
      return 0;
    case 'u':
      muid = atoi(Coptarg);
      break;
    case 'g':
      mgid = atoi(Coptarg);
      break;
    case 'p':
      mpriority = atoi(Coptarg);
      break;
    default:
      usage(progName);
      return 0;
    }
  }
  
  try {
    
    // Initializing the log
    castor::BaseObject::initLog("NewStagerLog", castor::SVC_NOMSG);
    
    // retrieve a Services object
    castor::Services* svcs = castor::BaseObject::services();
    // retrieve the DB service
    castor::IService* isvc = svcs->service("DB", castor::SVC_DBSTAGERSVC);
    if (0 == isvc) {
      std::cerr << "Unable to retrieve Stager Service." << std::endl
                << "Please check your configuration." << std::endl;
      exit(1);
    }

    castor::stager::IStagerSvc* stgsvc =
      dynamic_cast<castor::stager::IStagerSvc*>(isvc);
    
    // list the priorities
    std::vector<castor::stager::PriorityMap*> listPriority = 
      stgsvc->selectPriority(muid, mgid, mpriority);
    
    // print out the returned results
    std::vector<castor::stager::PriorityMap*>::iterator line = listPriority.begin();
    if (listPriority.empty()){
      fprintf(stdout, "No priorities found\n");
    } else {
      fprintf(stdout, "%-8s %-8s %-8s\n", "Uid", "Gid", "Priority");
      while (line != listPriority.end()){ 
	fprintf(stdout, "%-8llu %-8llu %-8llu\n", 
		(*line)->euid(),
		(*line)->egid(),
		(*line)->priority());
	line++;
      } 
    }

    // cleanup
    stgsvc->release();

  } catch (castor::exception::Exception e) {
    std::cerr << "Caught exception :\n"
              << e.getMessage().str() << std::endl;
    exit(1);  
  }
}



