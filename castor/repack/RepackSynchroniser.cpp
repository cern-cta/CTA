/******************************************************************************
 *                      RepackSynchroniser.cpp
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



#include "RepackSynchroniser.hpp"
#include "castor/stager/StageRmRequest.hpp"

namespace castor{
	namespace repack {
		
//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RepackSynchroniser::RepackSynchroniser(RepackServer* svr){
  m_dbhelper = new DatabaseHelper();
  ptr_server = svr;
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
RepackSynchroniser::~RepackSynchroniser(){
	delete m_dbhelper;
}
		
//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------	
void RepackSynchroniser::run(void* param) throw(){
	
  std::vector<RepackSubRequest*>* tapelist = NULL;
  std::vector<RepackSubRequest*>::iterator tape;
  int i = 0;
  bool error = false;
  std::string answer;
  FileListHelper m_filehelper (ptr_server->getNsName());
  

  try {
    /** get the possible tapelist */
    tapelist = m_dbhelper->getAllSubRequestsStatus(SUBREQUEST_READYFORSTAGING);

    if ( tapelist != NULL )	{

      while ( tapelist->size() ){
        error = false;

        /** list tapes for choose */
        std::cout << "Which tape do you want to synchronise (q/Q for quit) :";
        tape = tapelist->begin();
        while ( tape != tapelist->end() )
          std::cout << (*tape)->vid() << std::endl;
  
        std::cin >> answer; 

        /** abort */ 
        if ( answer == "q" || answer == "Q") return;
        
        /** find the tape in the list */
        tape = tapelist->begin();
        while ( tape != tapelist->end() ){
          if ( (*tape)->vid() == answer ) break;
          else error = true;
        }

        /** get the segs the cuuid and update the status to STAGING */
        if ( !error ) {
          m_filehelper.getFileListSegs((*tape));
          (*tape)->setStatus(SUBREQUEST_STAGING);
          
          std::string cuuid;
          std::cout << "Please give the Stager CUUID the former RepackRequest was sent with: ";
          
          std::cin >> answer;
          (*tape)->setCuuid(answer);
          
          m_dbhelper->updateSubRequest((*tape), true, nullCuuid);
          
          std::cout << "Tape " << (*tape)->vid() 
                    << "with "<< (*tape)->segment().size() << "Files" 
                    << "and CUUID " << (*tape)->cuuid() << "has been updated to status " 
                    << SUBREQUEST_STAGING <<std::endl;
          
          freeRepackObj((*tape)->requestID());
          tapelist->erase(tape);
        }
      }
    }
  }catch (castor::exception::Exception e){
    castor::dlf::Param params[] =
    {castor::dlf::Param("Message", e.getMessage().str() )};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 5, 1, params);
  }
}
		
	
//------------------------------------------------------------------------------
// stop
//------------------------------------------------------------------------------
void RepackSynchroniser::stop() throw(){
	
}

	} //END NAMESPACE REPACK
} //END NAMESPACE CASTOR

