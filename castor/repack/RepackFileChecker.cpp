/******************************************************************************
 *                      RepackFileChecker.cpp
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



#include "RepackFileChecker.hpp"
#include "RepackSubRequestStatusCode.hpp"
#include "RepackUtility.hpp"
#include "FileListHelper.hpp"
#include "RepackSubRequest.hpp"
#include "IRepackSvc.hpp"

#include "castor/Services.hpp"
#include "castor/IService.hpp"


namespace castor{
	namespace repack {
		
//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RepackFileChecker::RepackFileChecker(RepackServer* svr ){
  ptr_server = svr;
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
RepackFileChecker::~RepackFileChecker() throw(){
}
		
//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------	
void RepackFileChecker::run(void* param) throw(){
  std::vector<RepackSubRequest*> sreqs;
  std::vector<RepackSubRequest*>::iterator sreq;

 // connect to the db
   // service to access the database
  castor::IService* dbSvc = castor::BaseObject::services()->service("OraRepackSvc", castor::SVC_ORAREPACKSVC);
  castor::repack::IRepackSvc* oraSvc = dynamic_cast<castor::repack::IRepackSvc*>(dbSvc);
  

  if (0 == oraSvc) {    
   castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 1 , 0, NULL);
   return;
  }



  try {
    Cuuid_t cuuid = nullCuuid;
    FileListHelper m_filehelper (ptr_server->getNsName());

    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 13, 0, 0);
    sreqs = oraSvc->getSubRequestsByStatus(RSUBREQUEST_TOBECHECKED,false);

    sreq=sreqs.begin();
    
    // Analyze the subrequests

    while (sreq != sreqs.end()){
      if ( (*sreq) != NULL ){
	cuuid = stringtoCuuid((*sreq)->cuuid());
	castor::dlf::Param params[] =
	  {castor::dlf::Param("VID", (*sreq)->vid()),
	   castor::dlf::Param("ID", (*sreq)->id()),
	   castor::dlf::Param("STATUS", RepackSubRequestStatusCodeStrings[(*sreq)->status()])};
	castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 14, 3, params);
    
      /** get the Segs for this tape  */

      /** get the Segs for this tape from the Name Server */
	
	m_filehelper.getFileListSegs(*sreq);

	if ( (*sreq)->repacksegment().size() == 0 ){
	  // no segment for this tape
	  castor::dlf::dlf_writep(cuuid, DLF_LVL_WARNING, 15, 3, params);
	  (*sreq)->setStatus(RSUBREQUEST_TOBECLEANED);
	  oraSvc->updateSubRequest(*sreq);
	  sreq++;
	  continue;
	}
  
	(*sreq)->setFiles((*sreq)->repacksegment().size());
	(*sreq)->setStatus(RSUBREQUEST_ONHOLD);
       
	try {

	  oraSvc->insertSubRequestSegments(*sreq);
        
	  // validate that none of the files on the tape is involved in another repack process.

	  bool ret=oraSvc->validateRepackSubRequest(*sreq,ptr_server->maxFiles(),ptr_server->maxTapes());

	  if (ret){
	    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 16, 1, params);
	  } else {
	    castor::dlf::dlf_writep(cuuid, DLF_LVL_WARNING, 17, 1, params);  
	  }
       
	} catch (castor::exception::Exception e ){

	  castor::dlf::Param params[] =
	    {
	      castor::dlf::Param("Precise Message", e.getMessage().str()),
	      castor::dlf::Param("VID", (*sreq)->vid())
	    };
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 56, 2, params);

	}
	freeRepackObj(*sreq);
      }
      sreq++;  
    }

  } catch (castor::exception::Exception e){
    // db error asking the tapes to check

    castor::dlf::Param params[] = {castor::dlf::Param("ErrorCode", e.code()),
				   castor::dlf::Param("Precise Message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 55, 2,params);


  }
}

	
//------------------------------------------------------------------------------
// stop
//------------------------------------------------------------------------------
void RepackFileChecker::stop() throw(){
	
}

	} //END NAMESPACE REPACK
} //END NAMESPACE CASTOR

