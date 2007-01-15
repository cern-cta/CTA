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
#include "castor/stager/StageRmRequest.hpp"

namespace castor{
	namespace repack {
		
//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RepackCleaner::RepackCleaner(RepackServer* svr){
  m_dbhelper = new DatabaseHelper();
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
void RepackCleaner::run(void* param) throw(){
	
  RepackSubRequest* tape = NULL;
  Cuuid_t cuuid;


	
  try {
    tape = m_dbhelper->checkSubRequestStatus(SUBREQUEST_READYFORCLEANUP);

    if ( tape != NULL )	{
      //m_dbhelper->lock(tape);
      cuuid = stringtoCuuid(tape->cuuid());
      if ( cleanupTape(tape) ){
        castor::dlf::Param params[] =
        {castor::dlf::Param("VID", tape->vid())};
        castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 35, 1, params);
      }
      m_dbhelper->unlock();
      cuuid = nullCuuid;
      freeRepackObj(tape->requestID()); 
      tape = NULL;
    }
    


  }catch (castor::exception::Exception e){
    castor::dlf::Param params[] =
    {castor::dlf::Param("Message", e.getMessage().str() )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 5, 1, params);
    
  }
}
		
	
//------------------------------------------------------------------------------
// stop
//------------------------------------------------------------------------------
void RepackCleaner::stop() throw(){
	
}

//------------------------------------------------------------------------------
// getStageStatus
//------------------------------------------------------------------------------
int RepackCleaner::cleanupTape(RepackSubRequest* sreq) throw(castor::exception::Internal)
{ 
    Cuuid_t cuuid = stringtoCuuid(sreq->cuuid());

    /// we remove the files from stager
    /// and set the status to SUBREQUEST_DONE. This indicates
    /// that the RepackSubRequest can now be archived
    removeFilesFromStager(sreq);
    m_dbhelper->remove(sreq);
    sreq->setStatus(SUBREQUEST_DONE);
    m_dbhelper->updateSubRequest(sreq,false,cuuid);
    return true;
}

//------------------------------------------------------------------------------
// getStageStatus
//------------------------------------------------------------------------------
void RepackCleaner::removeFilesFromStager(RepackSubRequest* sreq) throw(castor::exception::Internal)
{
  
  struct stage_options opts;
  castor::stager::StageRmRequest req;

 
  if ( sreq == NULL ){
    castor::exception::Internal ex;
    ex.getMessage() << "passed SubRequest ist NULL" << std::endl; 
    throw ex;
  }
  
  /* get the files using the Filelisthelper */
  FileListHelper flp(ptr_server->getNsName());
  Cuuid_t cuuid = stringtoCuuid(sreq->cuuid());
  std::vector<std::string>* filelist = flp.getFilePathnames(sreq);
  std::vector<std::string>::iterator filename = filelist->begin();

  if ( !filelist->size() ) {
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
  while (filename != filelist->end()) {
    castor::stager::SubRequest *subreq = new castor::stager::SubRequest();
    subreq->setFileName((*filename));
    subreq->setRequest(&req);
     req.addSubRequests(subreq);
     filename++;
  }
  
  /* we need the stager name and service class for correct removal
   * the port and version has to be set be the BaseClient. Nevertheless the 
   * values have to be 0.
   */
  opts.stage_port = 0; 
  opts.stage_version = 0;

  
  castor::client::BaseClient client(stage_getClientTimeout());
  client.setOption(NULL);       /** to initialize the RH,stager, etc. */
  /* set the service class information from repackrequest */
  getStageOpts(&opts, sreq);
  client.setOption(&opts);

  castor::stager::RequestHelper reqh(&req);
  std::vector<castor::rh::Response *>respvec;
  castor::client::VectorResponseHandler rh(&respvec);
  reqh.setOptions(&opts);
  client.setAuthorizationId(sreq->requestID()->userid(), sreq->requestID()->groupid());
  client.sendRequest(&req,&rh); 

  /** we ignore the results from the stager */
  for (int i=0; i<respvec.size(); i++) {
    delete respvec[i];
  }
}



		
	} //END NAMESPACE REPACK
} //END NAMESPACE CASTOR

