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
 * @author Felix Ehm
 *****************************************************************************/



#include "RepackCleaner.hpp"
#include "castor/Services.hpp"
#include "RepackUtility.hpp"

namespace castor{
	namespace repack {
		
//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RepackCleaner::RepackCleaner(RepackServer* svr){
  m_dbSvc = NULL;
  ptr_server = svr;
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
RepackCleaner::~RepackCleaner() throw() {
    if (m_dbSvc != NULL) delete m_dbSvc;
    m_dbSvc=NULL;
}
		
//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------	
void RepackCleaner::run(void* param) {
  std::vector<RepackSubRequest*> tapes;
  std::vector<RepackSubRequest*>::iterator tape; 
  try {
    if (m_dbSvc == 0){
 // service to access the database
      castor::IService* orasvc = castor::BaseObject::services()->service("OraRepackSvc", castor::SVC_ORAREPACKSVC);
 
      m_dbSvc = dynamic_cast<castor::repack::IRepackSvc*>(orasvc);

      if (m_dbSvc == 0) {
	// FATAL ERROR
	castor::dlf::Param params0[] =
	  {castor::dlf::Param("Standard Message", "RepackCleaner fatal error")};
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 3, 1, params0);
	return;
      }
    
    }
  
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 41, 0, 0);

  // get all the tape
    tapes = m_dbSvc->getSubRequestsByStatus(RSUBREQUEST_TOBECLEANED,false);

    if (tapes.empty()) return;

  // loop over the tapes
    tape=tapes.begin();

    while (tape!=tapes.end()){
      castor::dlf::Param params[] =
	{castor::dlf::Param("VID", (*tape)->vid()),
	 castor::dlf::Param("ID", (*tape)->id()),
	 castor::dlf::Param("STATUS", (*tape)->status())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 42, 3, params);
     
      if ( (*tape) != NULL )	{  
	checkTape(*tape);
	m_dbSvc->updateSubRequest((*tape));
	freeRepackObj((*tape)); 
	tape++;
      }
    } 

  // we check if we are able to restart one of the subrequest on-hold

    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 43, 0, 0);

    m_dbSvc->resurrectTapesOnHold(ptr_server->maxFiles(),ptr_server->maxTapes());

  } catch (...) {
     if (!tapes.empty()){
      tape=tapes.begin();
      while (tape != tapes.end()){
	freeRepackObj(*tape);
	tape++;
      }
    }
  }

}

//-----------------------------------------------------------------------------
// checkTape
//-----------------------------------------------------------------------------


void  RepackCleaner::checkTape(RepackSubRequest* tape){
 
//file list helper get files on tape
  Cuuid_t cuuid;

  FileListHelper filelisthelper(ptr_server->getNsName());
  u_signed64 fileOnTape=filelisthelper.countFilesOnTape(tape->vid()); 
  u_signed64 filesDone=0;
  if (tape->files() > fileOnTape)
    filesDone=tape->files()-fileOnTape;
  tape->setFilesStaged(filesDone);
  tape->setFilesFailed(fileOnTape);
  tape->setFilesFailedSubmit(0);
  tape->setFilesStaging(0);
  tape->setFilesMigrating(0);

  if (fileOnTape == 0){
   //tape done
    castor::dlf::Param params[] =
      {castor::dlf::Param("VID", tape->vid()),
       castor::dlf::Param("STATUS", "RSUBREQUEST_DONE" )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 45, 2, params);
    tape->setStatus(RSUBREQUEST_DONE);


    castor::dlf::Param params1[] =
      {castor::dlf::Param("VID", tape->vid())};
    
    // reclaim the tape

    if (tape->repackrequest()->reclaim() == 1){
      if (vmgr_reclaim((char*) tape->vid().c_str())<0){
	castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 61, 1, params1);
      } else {
	castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 62, 1, params1);
      }
      
    }
   
    //change tapepool

    if  (!tape->repackrequest()->finalPool().empty()){
      if (vmgr_modifytape((char*) tape->vid().c_str(),NULL,NULL,NULL,NULL,NULL,NULL,(char *)tape->repackrequest()->finalPool().c_str(),-1)<0){
	castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 63, 1, params1);
      } else {
	castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 64, 1, params1);
      }
    }

  } else {
    // tape failed
    castor::dlf::Param params[] =
      {castor::dlf::Param("VID", tape->vid()),
       castor::dlf::Param("STATUS", "RSUBREQUEST_FAILED" )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 44, 2, params);
    tape->setStatus(RSUBREQUEST_FAILED);

    // Retry

     // we faced problem, we need to retry
     int numRetry= tape->retryNb();

     if (numRetry > 0){
	  numRetry--;
	  tape->setRetryNb(numRetry);
	  tape->setStatus(RSUBREQUEST_TOBERESTARTED);
	  m_dbSvc->updateSubRequest(tape);

	  castor::dlf::Param params[] =
	   {castor::dlf::Param("VID", tape->vid()),
	    castor::dlf::Param("STATUS", tape->status())};
	    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 38, 2, params);
	   
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

