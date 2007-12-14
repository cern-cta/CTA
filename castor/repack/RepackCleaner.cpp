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

namespace castor{
	namespace repack {
		
//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RepackCleaner::RepackCleaner(RepackServer* svr){
  m_dbhelper = NULL;
  ptr_server = svr;
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
RepackCleaner::~RepackCleaner() throw() {
	delete m_dbhelper;
}
		
//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------	
void RepackCleaner::run(void* param) {
	
  RepackSubRequest* tape = NULL;
  Cuuid_t cuuid;
  if (m_dbhelper==NULL) {
    m_dbhelper = new DatabaseHelper();
  }

  try {
    tape = m_dbhelper->checkSubRequestStatus(SUBREQUEST_READYFORCLEANUP);

    if ( tape != NULL )	{  
      m_dbhelper->remove(tape);
      if (checkTape(tape)) {
	//tape done
        castor::dlf::Param params[] =
	  {castor::dlf::Param("VID", tape->vid()),
	   castor::dlf::Param("STATUS", "SUBREQUEST_DONE" )};
	castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 40, 2, params);
	

      } else {
	// tape failed
	castor::dlf::Param params[] =
	  {castor::dlf::Param("VID", tape->vid()),
	   castor::dlf::Param("STATUS", "SUBREQUEST_FAILED" )};
	castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 40, 2, params);
      } 
      m_dbhelper->updateSubRequest(tape,false,cuuid);
      freeRepackObj(tape->repackrequest()); 
      tape = NULL;
    }
    


  }catch (castor::exception::Exception e){
    castor::dlf::Param params[] =
    {castor::dlf::Param("Message", e.getMessage().str() )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 5, 1, params);
    
  }
}


//-----------------------------------------------------------------------------
// checkTape
//-----------------------------------------------------------------------------


bool  RepackCleaner::checkTape(RepackSubRequest* tape){
 
//file list helper get files on tape
 FileListHelper filelisthelper(ptr_server->getNsName());
 u_signed64 fileOnTape=filelisthelper.countFilesOnTape(tape->vid());
 tape->setFilesStaged(tape->files()-fileOnTape);
 tape->setFilesFailed(fileOnTape);
 tape->setFilesFailedSubmit(0);
 tape->setFilesStaging(0);
 tape->setFilesMigrating(0);

 if (fileOnTape ==0){
   tape->setStatus(SUBREQUEST_DONE);
   return true;
 }
 tape->setStatus(SUBREQUEST_FAILED);
 return false;   
 }

	
	
//------------------------------------------------------------------------------
// stop
//------------------------------------------------------------------------------
void RepackCleaner::stop() throw(){
	
}

		
	} //END NAMESPACE REPACK
} //END NAMESPACE CASTOR

