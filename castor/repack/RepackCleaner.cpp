/******************************************************************************
 *                      RepackCleaner.cpp
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
 *
 * @author Giulia Taurelli
 *****************************************************************************/


#include "FileListHelper.hpp"
#include "RepackCleaner.hpp"
#include "RepackUtility.hpp"
#include "vmgr_api.h"
#include "IRepackSvc.hpp"
#include "castor/Services.hpp"
#include "castor/IService.hpp"
#include <u64subr.h>

namespace castor{
	namespace repack {
		
//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RepackCleaner::RepackCleaner(RepackServer* svr){
  ptr_server = svr;

}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
RepackCleaner::~RepackCleaner() throw() {
}
		
//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------	
void RepackCleaner::run(void*) {
  std::vector<RepackSubRequest*> tapes;
  std::vector<RepackSubRequest*>::iterator tape; 

   // connect to the db
   // service to access the database
  castor::IService* dbSvc = castor::BaseObject::services()->service("OraRepackSvc", castor::SVC_ORAREPACKSVC);
  castor::repack::IRepackSvc* oraSvc = dynamic_cast<castor::repack::IRepackSvc*>(dbSvc);
  

  if (0 == oraSvc) {    
   castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 1 , 0, NULL);
   return;
  }

  try {
  
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 41, 0, 0);

    // get all the tape
    tapes = oraSvc->getSubRequestsByStatus(RSUBREQUEST_TOBECLEANED,false);
    

      // loop over the tapes
    tape=tapes.begin();

    while (tape!=tapes.end()){
      castor::dlf::Param params[] =
	{castor::dlf::Param("VID", (*tape)->vid()),
	 castor::dlf::Param("ID", (*tape)->id()),
	 castor::dlf::Param("STATUS",RepackSubRequestStatusCodeStrings[(*tape)->status()])};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 42, 3, params);
     
      if ( (*tape) != NULL )	{  
	try {
	  checkTape(*tape);
	  oraSvc->updateSubRequest((*tape));
	} catch (castor::exception::Exception e){
	  
	    castor::dlf::Param params[] =
	      {
		castor::dlf::Param("Precise Message", e.getMessage().str()),
		castor::dlf::Param("VID", (*tape)->vid())
	      };
	    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 53, 2, params);


	}
	freeRepackObj((*tape)); 
	tape++;
      }
    } 

  // we check if we are able to restart one of the subrequest on-hold

    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 43, 0, 0);
    
    try {
      
      oraSvc->resurrectTapesOnHold(ptr_server->maxFiles(),ptr_server->maxTapes());
      
    } catch (castor::exception::Exception e){

       // log the error in Dlf

      castor::dlf::Param params[] =
	{
	  castor::dlf::Param("Precise Message", e.getMessage().str())
	};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 54, 1, params);   

      
    }

  } catch (castor::exception::Exception e) {

    // db error asking the tapes to clean

    castor::dlf::Param params[] = {castor::dlf::Param("ErrorCode", e.code()),
				   castor::dlf::Param("Precise Message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 52, 2,params);
  }

}

//-----------------------------------------------------------------------------
// checkTape
//-----------------------------------------------------------------------------


void  RepackCleaner::checkTape(RepackSubRequest* tape) throw (castor::exception::Exception){
 
//file list helper get files on tape
  Cuuid_t cuuid;

  FileListHelper filelisthelper(ptr_server->getNsName());
  int fileOnTape=filelisthelper.countFilesOnTape(tape->vid()); 
  int filesDone=0;
  if (tape->files() > fileOnTape)
    filesDone=tape->files()-fileOnTape;

  tape->setFilesStaged(filesDone);
  tape->setFilesFailed(fileOnTape);
  tape->setFilesFailedSubmit(0);
  tape->setFilesStaging(0);
  tape->setFilesMigrating(0);

  u_signed64 data=tape->xsize();
  
  // not everything transfered 

  if (fileOnTape){
    if (tape->files())
      data=tape->xsize()/tape->files();
    data=data*filesDone;
  }
      
  char buf[21];
  u64tostru(data, buf, 0);
  
  castor::dlf::Param params[] =
    {castor::dlf::Param("VID", tape->vid()),
     castor::dlf::Param("STATUS",  RepackSubRequestStatusCodeStrings[tape->status()]),
     castor::dlf::Param("transferedDataVolume", buf)};


  if (fileOnTape == 0){
   //tape done

    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 45, 3, params);
    tape->setStatus(RSUBREQUEST_DONE);


    castor::dlf::Param params1[] =
      {castor::dlf::Param("VID", tape->vid())};
    
    // reclaim the tape

    if (tape->repackrequest()->reclaim() == 1){
      if (vmgr_reclaim((char*) tape->vid().c_str())<0){
	castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 48, 1, params1);
      } else {
	castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 49, 1, params1);
      }     
    }
   
    //change tapepool

    if  (!tape->repackrequest()->finalPool().empty()){
      if (vmgr_modifytape((char*) tape->vid().c_str(),NULL,NULL,NULL,NULL,NULL,NULL,(char *)tape->repackrequest()->finalPool().c_str(),-1)<0){
	castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 50, 1, params1);
      } else {
	castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 51, 1, params1);
      }
    }

  } else {
    // tape failed

    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 44, 3, params);
    tape->setStatus(RSUBREQUEST_FAILED);

    // Retry

     // we faced problem, we need to retry
     int numRetry= tape->retryNb();

     if (numRetry > 0){
       castor::dlf::Param params[] =
	 {castor::dlf::Param("VID", tape->vid())};
       castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 38, 1, params);
       
       numRetry--;
       tape->setRetryNb(numRetry);
       tape->setStatus(RSUBREQUEST_TOBERESTARTED);
       
     } 
  }   
 }

	
	
//------------------------------------------------------------------------------
// stop
//------------------------------------------------------------------------------
void RepackCleaner::stop() throw(){
	
}

		
	} //END NAMESPACE REPACK
} //END NAMESPACE CASTOR

