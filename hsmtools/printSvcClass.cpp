/******************************************************************************
 *                      printSvcClass.c
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
 * @(#)$RCSfile: printSvcClass.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2005/07/29 15:31:43 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include <iostream>
#include <castor/BaseObject.hpp>
#include <castor/stager/SvcClass.hpp>
#include <castor/stager/IStagerSvc.hpp>
#include <castor/Services.hpp>
#include <castor/BaseAddress.hpp>
#include <castor/Constants.h>
#include <Cgetopt.h>

int help_flag =0;

static struct Coptions longopts[] = {
  {"help",NO_ARGUMENT,&help_flag,'h'},
  {NULL, 0, NULL, 0}
};

void usage(char *cmd) {
  std::cout << "Usage : " << cmd
            << " [-h|--help] SvcClassName"
            << std::endl;
}

int main(int argc, char *argv[]) {

  try {
    
    char* progName = argv[0];
    
    // Deal with options
    Coptind = 1;
    Copterr = 1;
    int ch;
    while ((ch = Cgetopt_long(argc,argv,"h",longopts,NULL)) != EOF) {
      switch (ch) {
      case 'h':
        help_flag = 1;
        break;
      case '?':
        usage(progName);
        exit(1);
      default:
        break;
      }
    }

    // Display help if required
    if (help_flag != 0) {
      usage(progName);
      return 0;
    }

    // Check the arguments
    argc -= Coptind;
    argv += Coptind;
    if (argc != 1) {
      std::cerr << "Error : wrong number of arguments." << std::endl;
      usage(progName);
      return(1);
    }
    char* svcClassName = argv[0];    

    // Initializing the log
    castor::BaseObject::initLog("NewStagerLog", castor::SVC_NOMSG);
    // retrieve a Services object
    castor::Services* svcs = castor::BaseObject::services();
    // retrieve the DB service
    castor::IService* isvc = svcs->service("DB",castor::SVC_DBSTAGERSVC);
    if (0 == isvc) {
      std::cerr << "Unable to retrieve Stager Service." << std::endl
                << "Please check your configuration." << std::endl;
      exit(1);
    }
    castor::stager::IStagerSvc* stgsvc =
      dynamic_cast<castor::stager::IStagerSvc*>(isvc);

    // get the SvcClass
    castor::stager::SvcClass* svcclass = stgsvc->selectSvcClass(svcClassName);
    if (0 == svcclass) {
      std::cerr << "SvcClass " << svcClassName << " does not exist"
                << std::endl;
      if (0 != stgsvc) stgsvc->release();
      return(0);
    }
    
    // get DiskPools and TapePools
    castor::BaseAddress ad;
    ad.setCnvSvcName("DbCnvSvc");
    ad.setCnvSvcType(castor::SVC_DBCNV);
    svcs->fillObj(&ad, svcclass, castor::OBJ_TapePool);
    svcs->fillObj(&ad, svcclass, castor::OBJ_DiskPool);
    
    // print out everything
    svcclass->print();
  
    // cleanup
    delete svcclass;
    if (0 != stgsvc) stgsvc->release();

  } catch (castor::exception::Exception e) {
    std::cerr << "Caught exception :\n"
              << e.getMessage().str() << std::endl;    
  }
  
}



