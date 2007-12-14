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

namespace castor{
	namespace repack {
		
//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RepackFileChecker::RepackFileChecker(RepackServer* svr){
  m_dbhelper = NULL;
  ptr_server = svr;
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
RepackFileChecker::~RepackFileChecker() throw(){
	delete m_dbhelper;
}
		
//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------	
void RepackFileChecker::run(void* param) throw(){
  if (m_dbhelper == NULL){
    m_dbhelper = new DatabaseHelper();
  }
  RepackSubRequest* sreq = NULL;
  Cuuid_t cuuid = nullCuuid;
  FileListHelper m_filehelper (ptr_server->getNsName());

  try {
    /** get a candidate */
    sreq = m_dbhelper->checkSubRequestStatus(SUBREQUEST_READYFORSTAGING);

  }catch (castor::exception::Exception e){
      castor::dlf::Param params[] = 
      {castor::dlf::Param("Error", e.getMessage().str() )};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 5, 1, params);
      return;
  }
  if ( sreq != NULL ){
    cuuid = stringtoCuuid(sreq->cuuid());
    castor::dlf::Param params[] =
    {castor::dlf::Param("VID", sreq->vid()),
      castor::dlf::Param("ID", sreq->id()),
      castor::dlf::Param("STATUS", sreq->status())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 29, 3, params);
    
    /** get the Segs for this tape  */

    try {

      /** get the Segs for this tape  */
      m_filehelper.getFileListSegs(sreq);
      /** check, if we got something back */
      if ( sreq->repacksegment().size() == 0 ){
        castor::dlf::Param params[] =
        {castor::dlf::Param("VID", sreq->vid())};
        castor::dlf::dlf_writep(cuuid, DLF_LVL_WARNING, 39, 1, params);
        sreq->setStatus(SUBREQUEST_DONE);
        m_dbhelper->updateSubRequest(sreq,false, cuuid);
        
        return;
        }
  
	sreq->setFiles(sreq->repacksegment().size());
	sreq->setStatus(SUBREQUEST_TOBESTAGED);
	m_dbhelper->updateSubRequest(sreq,true, cuuid);
	stage_trace(3,"Found %d files, RepackSubRequest for Tape %s ready for Staging ",sreq->files(),(char*)sreq->vid().c_str());
       
    }catch (castor::exception::Exception e){
       castor::dlf::Param params[] =
        {castor::dlf::Param("Message","Exception caugt inRepack FileChecker"), 
        castor::dlf::Param("Error Message",e.getMessage().str())};
        castor::dlf::dlf_writep(cuuid, DLF_LVL_WARNING, 5, 2, params);
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

