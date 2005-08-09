/******************************************************************************
 *                      vdqmDBInit.cpp
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
 * @(#)RCSfile: vdqmDBInit.cpp  Revision: 1.0  Release Date: Aug 9, 2005  Author: mbraeger 
 *
 *
 *
 * @author Matthias Braeger
 *****************************************************************************/

#include <iostream>
#include <castor/BaseObject.hpp>
#include <castor/Services.hpp>
#include <castor/BaseAddress.hpp>
#include <castor/Constants.h>
#include <castor/ICnvSvc.hpp>
#include <castor/IService.hpp>
#include <Cgetopt.h>

// Includes for VMGR
#include <sys/types.h>
#include "vmgr_api.h"

// for WRITE_ENABLE WRITE_DISABLE
#include <Ctape_constants.h>

#include <castor/vdqm/IVdqmSvc.hpp>
#include <castor/vdqm/DeviceGroupName.hpp>
#include <castor/vdqm/TapeAccessSpecification.hpp>

int help_flag =0;

static struct Coptions longopts[] = {
  {"help",NO_ARGUMENT,&help_flag,'h'},
  {NULL, 0, NULL, 0}
};

void usage(char *cmd) {
  std::cout << "Usage : " << cmd
            << " [-h|--help] FileClassName"
            << std::endl;
}


//------------------------------------------------------------------------------
// handleRequest
//------------------------------------------------------------------------------
void handleRequest(castor::IObject* fr, castor::Services* svcs)
  throw (castor::exception::Exception) {
  
  // Stores it into the data base
  castor::BaseAddress ad;
  
  ad.setCnvSvcName("DbCnvSvc");
  ad.setCnvSvcType(castor::SVC_DBCNV);
  
  
  try {
  	//Create a new entry in the table
    svcs->createRep(&ad, fr, true);
							
    castor::vdqm::DeviceGroupName *dgName =
      dynamic_cast<castor::vdqm::DeviceGroupName*>(fr);    
    if ( 0 != dgName ) {
			std::cout << "New DeviceGroupName row inserted into db: "
								<< "dgName = " << dgName->dgName()
								<< ", libraryName = " << dgName->libraryName()
								<< std::endl;
								  									
	  	dgName = 0;
    }
    
    castor::vdqm::TapeAccessSpecification* tapeAccessSpec =
      dynamic_cast<castor::vdqm::TapeAccessSpecification*>(fr);
      
    if ( 0 != tapeAccessSpec ) {
			std::cout << "New TapeAccessSpecification row inserted into db: "
								<< "accessMode = " << tapeAccessSpec->accessMode()
								<< ", density = " << tapeAccessSpec->density()
								<< ", tapeModel = " << tapeAccessSpec->tapeModel()								
								<< std::endl;
	  	
	  	tapeAccessSpec = 0;
  	}							
  } catch (castor::exception::Exception e) {
    svcs->rollback(&ad);
    
    castor::vdqm::DeviceGroupName *dgName =
      dynamic_cast<castor::vdqm::DeviceGroupName*>(fr);    
    if ( 0 != dgName ) {
			std::cerr << "Unable to insert a new DeviceGroupName into db."
							  << std::endl;
	  									
	  	dgName = 0;
    }
    
    castor::vdqm::TapeAccessSpecification* tapeAccessSpec =
      dynamic_cast<castor::vdqm::TapeAccessSpecification*>(fr);
      
    if ( 0 != tapeAccessSpec ) {
	  	std::cerr << "Unable to insert a new TapeAccessSpecification into db."
							  << std::endl;					
	  	
	  	tapeAccessSpec = 0;
  	}
  }
}


/**
 * Main loop for initializing the data base
 */
void initDB(castor::Services* svcs, castor::vdqm::IVdqmSvc* iVdqmService) 
	throw (castor::exception::Exception) {

	bool dgnUpdate, tasUpdate;
	int flags;
	vmgr_list list;
	struct vmgr_tape_denmap *denmap;
	struct vmgr_tape_dgnmap *dgnmap;
	
	castor::vdqm::DeviceGroupName* dgName;
	castor::vdqm::TapeAccessSpecification* tapeAccessSpec;
	
	dgnUpdate = false;
	tasUpdate = false;
	
	/**
	 * First we try to update the DeviceGroupName table
	 */
	
	std::cout << "Try to update DeviceGroupName table in db..."
						<< std::endl;
	
	flags = VMGR_LIST_BEGIN;
	while ((dgnmap = vmgr_listdgnmap (flags, &list)) != NULL) {
				
		dgName = iVdqmService->selectDeviceGroupName(dgnmap->dgn);
		
		// MANUAL is not a valid entry for CASTOR
		if ( dgName == NULL && std::strcmp(dgnmap->dgn, "MANUAL") != 0) {
			dgName = new castor::vdqm::DeviceGroupName();
			
			dgName->setDgName(dgnmap->dgn);
			dgName->setLibraryName(dgnmap->library);
			
			handleRequest(dgName, svcs);
			
			dgnUpdate = true;
			delete dgName;
		}
		
		flags = VMGR_LIST_CONTINUE;
	}
	(void) vmgr_listdgnmap (VMGR_LIST_END, &list);
	
	
	if ( !dgnUpdate ) {
		std::cout << "The entries in the DeviceGroupName table are already up to date!"						
							<< std::endl;
	}	
	

	/**
	 * Now, we try to update the TapeAccessSpecification table
	 */
	std::cout << std::endl
						<< "Try to update TapeAccessSpecification table in db..."
						<< std::endl;
						
	flags = VMGR_LIST_BEGIN;
	while ((denmap = vmgr_listdenmap (flags, &list)) != NULL) {
		
		tapeAccessSpec = 
			iVdqmService->selectTapeAccessSpecification(WRITE_DISABLE, denmap->md_density, denmap->md_model);
		
		if ( tapeAccessSpec == NULL) {
			tapeAccessSpec = new castor::vdqm::TapeAccessSpecification();
			
			tapeAccessSpec->setAccessMode(WRITE_DISABLE);
			tapeAccessSpec->setDensity(denmap->md_density);
			tapeAccessSpec->setTapeModel(denmap->md_model);			
			
			handleRequest(tapeAccessSpec, svcs);
			
			tasUpdate = true;
			delete tapeAccessSpec;
		}	
		
		
		tapeAccessSpec = 
			iVdqmService->selectTapeAccessSpecification(WRITE_ENABLE, denmap->md_density, denmap->md_model);
		
		if ( tapeAccessSpec == NULL) {
			tapeAccessSpec = new castor::vdqm::TapeAccessSpecification();
			
			tapeAccessSpec->setAccessMode(WRITE_ENABLE);
			tapeAccessSpec->setDensity(denmap->md_density);
			tapeAccessSpec->setTapeModel(denmap->md_model);			
			
			handleRequest(tapeAccessSpec, svcs);
			
			tasUpdate = true;
			delete tapeAccessSpec;
		}			
		
		flags = VMGR_LIST_CONTINUE;
	}
	(void) vmgr_listdenmap (VMGR_LIST_END, &list);
	
	if ( !tasUpdate ) {
		std::cout << "The entries in the TapeAccessSpecification table are already up to date!"						
							<< std::endl;
	}
}

int main(int argc, char *argv[]) {

  try {
		char* progName = argv[0];
    int ch;
    
    castor::Services* svcs = NULL;
    castor::ICnvSvc* cnvSvc = NULL;
    castor::IService* iService = NULL;
    castor::vdqm::IVdqmSvc* iVdqmService = NULL;
    
    // Deal with options
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

    // Initializing the log
    castor::BaseObject::initLog("VdqmDbInit", castor::SVC_NOMSG);
   	// retrieve a Services object
    svcs = castor::BaseObject::services();
       
    // create db conversion service
    // so that it is not deleted and recreated all the time
    cnvSvc =
      svcs->cnvService("DbCnvSvc", castor::SVC_DBCNV);
    if (0 == cnvSvc) {
      std::cerr << "Unable to retrieve Vdqm Service." << std::endl
                << "Please check your configuration." << std::endl;
      exit(1);
    }
    
		/**
		 * Getting DbVdqmSvc: It can be the OraVdqmSvc or the MyVdqmSvc
		 */
		iService = svcs->service("DbVdqmSvc", castor::SVC_DBVDQMSVC);
	  if (0 == iService) {
	    std::cerr << "Could not get DbVdqmSvc" << std::endl;
	    
	    exit(1);
	  }
	  
	  iVdqmService = dynamic_cast<castor::vdqm::IVdqmSvc*>(iService);
	  if (0 == iVdqmService) {
	    std::cerr << "Got a bad DbVdqmSvc: "
	    					<< "ID=" << iService->id()
	    					<< ", Name=" << iService->name()
	    					<< std::endl;
	
	    exit(1);
	  }    
    
    
    // start the main loop to initialize the db
    initDB(svcs, iVdqmService);

    // release the Oracle Conversion Service
    cnvSvc->release();
    
    delete iVdqmService;

  } catch (castor::exception::Exception e) {
    std::cerr << "Caught exception :\n"
              << e.getMessage().str() << std::endl;    
  }
}
