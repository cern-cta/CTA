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
#include "castor/Services.hpp"
#include "RepackSubRequestStatusCode.hpp"
#include "RepackUtility.hpp"

namespace castor{
	namespace repack {
		
//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RepackFileChecker::RepackFileChecker(RepackServer* svr){
  m_dbSvc = NULL;
  ptr_server = svr;
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
RepackFileChecker::~RepackFileChecker() throw(){
    if (m_dbSvc != NULL) delete m_dbSvc;
    m_dbSvc=NULL;
}
		
//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------	
void RepackFileChecker::run(void* param) throw(){
  std::vector<RepackSubRequest*> sreqs;
  std::vector<RepackSubRequest*>::iterator sreq;

  try {
    if (m_dbSvc == NULL ) {
 // service to access the database
      castor::IService* orasvc = castor::BaseObject::services()->service("OraRepackSvc", castor::SVC_ORAREPACKSVC);
  
      m_dbSvc = dynamic_cast<castor::repack::IRepackSvc*>(orasvc);
      if (m_dbSvc == 0) {
	// FATAL ERROR
	castor::dlf::Param params0[] =
	  {castor::dlf::Param("Standard Message", "RepackFileChecker fatal error"),};
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 2, 1, params0);
	return;
      }
    
    }
    Cuuid_t cuuid = nullCuuid;
    FileListHelper m_filehelper (ptr_server->getNsName());

    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 13, 0, 0);
    sreqs = m_dbSvc->getSubRequestsByStatus(RSUBREQUEST_TOBECHECKED,false);

    sreq=sreqs.begin();
    while (sreq != sreqs.end()){
      if ( (*sreq) != NULL ){
	cuuid = stringtoCuuid((*sreq)->cuuid());
	castor::dlf::Param params[] =
	  {castor::dlf::Param("VID", (*sreq)->vid()),
	   castor::dlf::Param("ID", (*sreq)->id()),
	   castor::dlf::Param("STATUS", (*sreq)->status())};
	castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 14, 3, params);
    
      /** get the Segs for this tape  */

      /** get the Segs for this tape from the Name Server */
	
	m_filehelper.getFileListSegs(*sreq);

	if ( (*sreq)->repacksegment().size() == 0 ){
	  // no segment for this tape
	  castor::dlf::dlf_writep(cuuid, DLF_LVL_WARNING, 15, 3, params);
	  (*sreq)->setStatus(RSUBREQUEST_DONE);
	  m_dbSvc->updateSubRequest(*sreq);
	  sreq++;
	  continue;
	}
  
	(*sreq)->setFiles((*sreq)->repacksegment().size());
	(*sreq)->setStatus(RSUBREQUEST_ONHOLD);
       
	m_dbSvc->insertSubRequestSegments(*sreq);
        
	// validate that none of the files on the tape is involved in another repack process.

	bool ret= m_dbSvc->validateRepackSubRequest(*sreq);

	if (ret){
	  stage_trace(3,"Found %d files, RepackSubRequest for Tape %s ready for Staging ",(*sreq)->files(),(char*)(*sreq)->vid().c_str());
	  castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 16, 1, params);
	} else {
	  castor::dlf::dlf_writep(cuuid, DLF_LVL_WARNING, 17, 1, params);  
	}
       
      }
      freeRepackObj(*sreq);
      sreq++;  
    }

  } catch (...){
    if (!sreqs.empty()){
      sreq=sreqs.begin();
      while (sreq != sreqs.end()){
	freeRepackObj(*sreq);
	sreq++;
      }
    }
  }
}

	
//------------------------------------------------------------------------------
// stop
//------------------------------------------------------------------------------
void RepackFileChecker::stop() throw(){
	
}

	} //END NAMESPACE REPACK
} //END NAMESPACE CASTOR

