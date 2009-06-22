/******************************************************************************
 *                      RepackKiller.cpp
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
 * @(#)$RCSfile: RepackKiller.cpp,v $ $Revision: 1.3 $ $Release$ $Date: 2009/06/22 09:26:14 $ $Author: gtaur $
 *
 *
 *
 * @author Giulia Taurelli
 *****************************************************************************/
 
#include "RepackKiller.hpp"
#include "FileListHelper.hpp"
#include "castor/stager/StageRmRequest.hpp"
#include "RepackSubRequestStatusCode.hpp"
#include "RepackUtility.hpp"
#include "castor/client/BaseClient.hpp"
#include "castor/rh/Response.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/client/VectorResponseHandler.hpp"
#include "castor/exception/Internal.hpp" 
#include "IRepackSvc.hpp"

#include "castor/Services.hpp"
#include "castor/IService.hpp"


namespace castor {
	namespace repack {
		
	
		
//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RepackKiller::RepackKiller(RepackServer* srv) 
{
  ptr_server = srv;
  	
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
RepackKiller::~RepackKiller() throw() {
}


//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void RepackKiller::run(void *param) throw() {
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

    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 63, 0, 0);
        
    // remove
    
    sreqs =  oraSvc->getSubRequestsByStatus(RSUBREQUEST_TOBEREMOVED,true); // segments needed
    sreq=sreqs.begin();
    
    while (sreq != sreqs.end()){
      castor::dlf::Param params[] =
	 {castor::dlf::Param("VID", (*sreq)->vid()),
	   castor::dlf::Param("ID", (*sreq)->id()),
	   castor::dlf::Param("STATUS", RepackSubRequestStatusCodeStrings[(*sreq)->status()])};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 19, 3, params);

      try {
	abortRepack(*sreq,oraSvc);
      } catch (castor::exception::Exception e) {
	castor::dlf::Param params[] =
	  {
	    castor::dlf::Param("Precise Message", e.getMessage().str()),
	    castor::dlf::Param("VID", (*sreq)->vid())
	  };
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 60, 2, params);
      }

      freeRepackObj(*sreq);
      sreq++;
    }
  
     
  } catch ( castor::exception::Exception e) {
    // db error asking the tapes to stage

    castor::dlf::Param params[] = {castor::dlf::Param("ErrorCode", e.code()),
				   castor::dlf::Param("Precise Message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 59, 2,params);
  }       

}


//------------------------------------------------------------------------------
// stop
//------------------------------------------------------------------------------
void RepackKiller::stop() throw() {
}



//------------------------------------------------------------------------------
// abortRepack
//------------------------------------------------------------------------------

void  RepackKiller::abortRepack(RepackSubRequest* sreq, castor::repack::IRepackSvc* oraSvc) throw (castor::exception::Exception){
  _Cuuid_t cuuid = stringtoCuuid(sreq->cuuid());
  try {
    sendRepackRemoveRequest(sreq);
  }catch (castor::exception::Exception ex){
    castor::dlf::Param params[] =
    {castor::dlf::Param("Standard Message", sstrerror(ex.code())),
     castor::dlf::Param("Precise Message", ex.getMessage().str()),
     castor::dlf::Param("VID", sreq->vid())
    };

    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 24, 3, params);
    sreq->setStatus(RSUBREQUEST_TOBECLEANED); // we failed the request even if the abort failed
   
  }

  sreq->setStatus(RSUBREQUEST_TOBECLEANED);
  oraSvc->updateSubRequest(sreq);
  castor::dlf::dlf_writep(stringtoCuuid(sreq->cuuid()), DLF_LVL_DEBUG, 25, 0, 0);
 
}




//-----------------------------------------------------------------------------------------------------------------
// sendRepackRemoveRequest
//-----------------------------------------------------------------------------------------------------------------


void  RepackKiller::sendRepackRemoveRequest( RepackSubRequest*sreq)throw(castor::exception::Exception)
{
  
  struct stage_options opts;
  castor::stager::StageRmRequest req;

 
  if ( sreq == NULL ){
    castor::exception::Internal ex;
    ex.getMessage() << "passed SubRequest is NULL" << std::endl; 
    throw ex;
  }
  
  /* get the files using the Filelisthelper */
  FileListHelper flp(ptr_server->getNsName());
  Cuuid_t cuuid = stringtoCuuid(sreq->cuuid());
  std::vector<std::string> filelist = flp.getFilePathnames(sreq);
  std::vector<std::string>::iterator filename = filelist.begin();

  if ( !filelist.size() ) {
    /* this means that there are no files for a repack tape in SUBREQUEST_READYFORCLEANUP
     * => ERROR, this must not happen!
     * but we leave it to the Monitor Service to solve that problem
     */
    castor::exception::Internal ex;
    ex.getMessage() << "No files found during cleanup phase for " << sreq->vid() 
              << std::endl;
    throw ex;
  }

  /* add the SubRequests to the Request */
  while (filename != filelist.end()) {
    castor::stager::SubRequest *subreq = new castor::stager::SubRequest();
    subreq->setFileName((*filename));
    subreq->setRequest(&req);
     req.addSubRequests(subreq);
     filename++;
  }
  
  /* we need the stager name and service class for correct removal
   * the port has to be set by the BaseClient. Nevertheless the 
   * values has to be 0.
   */
  opts.stage_port = 0; 

  castor::client::BaseClient client(stage_getClientTimeout(),stage_getClientTimeout());

  /* set the service class information from repackrequest */
  
  getStageOpts(&opts, sreq);
  client.setOptions(&opts);

  std::vector<castor::rh::Response *> respvec;
  castor::client::VectorResponseHandler rh(&respvec);

  client.setAuthorizationId(sreq->repackrequest()->userId(), sreq->repackrequest()->groupId());

  client.sendRequest(&req,&rh); 

  /** we ignore the results from the stager */

  for (unsigned int i=0; i<respvec.size(); i++) {
    delete respvec[i];
  }
}

		
	} //END NAMESPACE REPACK
}//END NAMESPACE CASTOR
