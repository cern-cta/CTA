/******************************************************************************
 *                      RepackRestarter.cpp
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
 * @(#)$RCSfile: RepackRestarter.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2009/02/27 10:25:35 $ $Author: gtaur $
 *
 *
 *
 * @author Giulia Taurelli
 *****************************************************************************/
 
#include "RepackRestarter.hpp"
#include "RepackSubRequestStatusCode.hpp"
#include "RepackUtility.hpp"

namespace castor {
	namespace repack {
		
	
		
//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RepackRestarter::RepackRestarter(RepackServer* srv) 
{
  ptr_server = srv;
  	
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
RepackRestarter::~RepackRestarter() throw() {
}


//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void RepackRestarter::run(void *param) throw() {
  std::vector<RepackSubRequest*> sreqs;
  std::vector<RepackSubRequest*>::iterator sreq;
  try {

    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 64, 0, 0);
         
 	 
    // restart
        
    sreqs = ptr_server->repackDbSvc()->getSubRequestsByStatus(RSUBREQUEST_TOBERESTARTED,false); // segments not needed
    sreq=sreqs.begin();
    while (sreq != sreqs.end()){
      castor::dlf::Param params[] =
	{castor::dlf::Param("VID", (*sreq)->vid()),
	  castor::dlf::Param("ID", (*sreq)->id()),
	 castor::dlf::Param("STATUS", (*sreq)->status())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 20, 3, params);
      try {

	ptr_server->repackDbSvc()->restartSubRequest((*sreq)->id());

      } catch (castor::exception::Exception e) {
	   // log the error in Dlf

	castor::dlf::Param params[] =
	{
	  castor::dlf::Param("Precise Message", e.getMessage().str()),
	  castor::dlf::Param("VID", (*sreq)->vid())
	};
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 62, 2, params);   	
      }

      freeRepackObj(*sreq);
      sreq++;
    }
     
  }catch (castor::exception::Exception e) {
    // db error asking the tapes to restart

    castor::dlf::Param params[] = {castor::dlf::Param("ErrorCode", e.code()),
				   castor::dlf::Param("Precise Message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 61, 2,params);
  }    

}


//------------------------------------------------------------------------------
// stop
//------------------------------------------------------------------------------
void RepackRestarter::stop() throw() {
}





		
	} //END NAMESPACE REPACK
}//END NAMESPACE CASTOR
