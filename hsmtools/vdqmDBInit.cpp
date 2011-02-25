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

#include <Cgetopt.h>
#include <Ctape_constants.h> // For WRITE_ENABLE WRITE_DISABLE
#include <iostream>
#include <castor/BaseAddress.hpp>
#include <castor/BaseObject.hpp>
#include <castor/Constants.h>
#include <castor/ICnvSvc.hpp>
#include <castor/IService.hpp>
#include <castor/Services.hpp>
#include <castor/db/DbParamsSvc.hpp>
#include <castor/vdqm/Constants.hpp>
#include <castor/vdqm/DeviceGroupName.hpp>
#include <castor/vdqm/IVdqmSvc.hpp>
#include <castor/vdqm/TapeAccessSpecification.hpp>
#include <h/vmgr_api.h> // For VMGR
#include <sys/types.h> // For VMGR
#include <string.h>
#include <stdio.h>

static struct Coptions longopts[] = {
  {"config", REQUIRED_ARGUMENT, NULL, 'c'},
  {"help"  , NO_ARGUMENT      , NULL, 'h'},
  {NULL    , 0                , NULL, 0  }
};

void usage(char *cmd) {
  std::cout << "Usage : " << cmd
            << " [-c|--config config-file] [-h|--help]"
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
  } catch (castor::exception::Exception& e) {
    svcs->rollback(&ad);

    castor::vdqm::DeviceGroupName *dgName =
      dynamic_cast<castor::vdqm::DeviceGroupName*>(fr);
    if ( 0 != dgName ) {
      std::cerr << "Unable to insert a new DeviceGroupName into db: "
        << e.getMessage().str() << std::endl;

      dgName = 0;
    }

    castor::vdqm::TapeAccessSpecification* tapeAccessSpec =
      dynamic_cast<castor::vdqm::TapeAccessSpecification*>(fr);

    if ( 0 != tapeAccessSpec ) {
      std::cerr << "Unable to insert a new TapeAccessSpecification into db: "
        << e.getMessage().str() << std::endl;

      tapeAccessSpec = 0;
    }
  }
}


/**
 * Main loop for initializing the data base
 */
void initDB(castor::Services* svcs, castor::vdqm::IVdqmSvc* iVdqmService)
  throw (castor::exception::Exception) {

  bool dgnUpdate = false;
  bool tasUpdate = false;
  int flags = 0;
  vmgr_list list;
  struct vmgr_tape_denmap_byte_u64 *denmap = NULL;
  struct vmgr_tape_dgnmap *dgnmap = NULL;

  castor::vdqm::DeviceGroupName* dgName = NULL;
  castor::vdqm::TapeAccessSpecification* tapeAccessSpec = NULL;

  /**
   * First we try to update the DeviceGroupName table
   */

  std::cout << "Try to update DeviceGroupName table in db..."
            << std::endl;

  flags = VMGR_LIST_BEGIN;
  while ((dgnmap = vmgr_listdgnmap (flags, &list)) != NULL) {

    dgName = iVdqmService->selectDeviceGroupName(dgnmap->dgn);

    // MANUAL is not a valid entry for CASTOR
    if ( dgName == NULL && strcmp(dgnmap->dgn, "MANUAL") != 0) {
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
  while ((denmap = vmgr_listdenmap_byte_u64 (flags, &list)) != NULL) {

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
  (void) vmgr_listdenmap_byte_u64 (VMGR_LIST_END, &list);

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

    Coptind = 1;
    Copterr = 0;

    // Deal with options
    while ((ch = Cgetopt_long(argc, argv, "c:h", longopts, NULL)) != -1) {
      switch (ch) {
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
            usage(progName);
            exit(1);
          }
        }
        setenv("PATH_CONFIG", Coptarg, 1);
        break;
      case 'h':
        usage(progName);
        exit(0);
      case '?':
        std::cerr
          << std::endl
          << "Error: Unknown command-line option: " << (char)Coptopt
          << std::endl << std::endl;
        usage(progName);
        exit(1);
      case ':':
        std::cerr
          << std::endl
          << "Error: An option is missing a parameter"
          << std::endl << std::endl;
        usage(progName);
        exit(1);
      default:
        std::cerr
          << std::endl
          << "Internal error: "
          << "Cgetopt_long returned the following unknown value: "
          << "0x" << std::hex << (int)ch << std::dec
          << std::endl << std::endl;
        exit(1);
      }
    }

    // Tell the DB service the VDQM schema version and DB connection details
    // file
    castor::IService* s =
      castor::BaseObject::sharedServices()->service("DbParamsSvc",
      castor::SVC_DBPARAMSSVC);
    castor::db::DbParamsSvc* params = dynamic_cast<castor::db::DbParamsSvc*>(s);
    if(params == 0) {
      std::cerr << "Could not instantiate the parameters service" << std::endl;
      exit(1);
    }
    params->setSchemaVersion(castor::vdqm::VDQMSCHEMAVERSION);
    params->setDbAccessConfFile(ORAVDQMCONFIGFILE);

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

  } catch (castor::exception::Exception& e) {
    std::cerr << "Caught exception :\n"
              << e.getMessage().str() << std::endl;
  }
}
