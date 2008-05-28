/******************************************************************************
 *                      showPriority.c
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
 * @(#)$RCSfile: showPriority.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2008/05/28 08:07:11 $ $Author: gtaur $
 *
 * 
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

int help_flag =0;

static struct Coptions longopts[] = {
  {"help",NO_ARGUMENT,&help_flag,'h'},
  {"help",NO_ARGUMENT,&help_flag,'h'},
  {"uid", REQUIRED_ARGUMENT, NULL,'u'},
  {"guid",REQUIRED_ARGUMENT, NULL,'u'}, 
  {"priority",REQUIRED_ARGUMENT, NULL,'p'}, 
  {NULL, 0, NULL, 0}
};

void usage(char *cmd) {
  std::cout << "Usage : " << cmd
            << " [-h] [-u uid] [-g gid] [-p priority]"
            << std::endl;
}

int main(int argc, char *argv[]) {
  int muid=-1;
  int mgid=-1;
  int mpriority=-1; 
  try {
    
    char* progName = argv[0];
    
    // Deal with options
    Coptind = 1;
    Copterr = 1;
    int ch;
    while ((ch = Cgetopt_long(argc,argv,"hu:g:p:",longopts,NULL)) != EOF) {
      switch (ch) {
      case 'h':
	usage(progName);
	return 0;
      case 'u':
        muid=atoi(Coptarg);
	break;
      case 'g':
	mgid=atoi(Coptarg);
	break;
      case 'p':
	mpriority=atoi(Coptarg);
	break;
      default:
	 usage(progName);
	 return 0;
         
      }
    }
   

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


    std::vector<castor::stager::PriorityMap*> listPriority=stgsvc->selectPriority(muid,mgid,mpriority);
  
    // surf the verctor to print the result
    std::vector<castor::stager::PriorityMap*>::iterator line= listPriority.begin();
    if (listPriority.empty()){
      std::cout << "No entry selected using the provided input"<<std::endl;

    } else {
      std::cout <<"===========================================" <<std::endl;
      std::cout<<"        uid        gid      priority" <<std::endl;
      std::cout <<"===========================================" <<std::endl;
      while (line != listPriority.end()){ 
	std::cout<<std::setw(10) << (*line)->euid() <<"          "<< (*line)->egid()<<"         " << (*line)->priority()<<std::endl;
	line++;
      } 
    } 


    // cleanup
    
    if (0 != stgsvc) stgsvc->release();

  } catch (castor::exception::Exception e) {
    std::cerr << "Caught exception :\n"
              << e.getMessage().str() << std::endl;    
  }
  
}



