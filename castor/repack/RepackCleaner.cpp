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
RepackCleaner::~RepackCleaner(){
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
      cuuid = nullCuuid;
      freeRepackObj(tape->requestID()); 
      tape = NULL;
    }
    //m_dbhelper->unlock();


    tape = m_dbhelper->checkSubRequestStatus(SUBREQUEST_RESTART);
    if ( tape != NULL ){
      //m_dbhelper->lock(tape);
      cuuid = stringtoCuuid(tape->cuuid());
      castor::dlf::Param params[] =
        {castor::dlf::Param("VID", tape->vid())};
        castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 37, 1, params);
      restartRepack(tape);
      freeRepackObj(tape->requestID());
      tape = NULL;
      //m_dbhelper->unlock();
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



void RepackCleaner::restartRepack(RepackSubRequest* sreq) throw(castor::exception::Internal)
{
    Cuuid_t cuuid = stringtoCuuid(sreq->cuuid());
    
    /// for a restart, we just need to remove the segments from the RepackSubRequest
    /// so the RepackFileStager will again send the file query to the stager
    
    /// the DB Helper removes the segs from DB and puts the RepackSubRequest into ARCHIVED
    m_dbhelper->remove(sreq);
    
    /// of course, now we have to change the status, so the process begins again
    sreq->setStatus(SUBREQUEST_READYFORSTAGING);
    sreq->setFiles(0);
    sreq->setFilesMigrating(0);
    sreq->setFilesStaging(0);
    m_dbhelper->updateSubRequest(sreq,false,cuuid);
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
  if ( sreq == NULL ){
    castor::exception::Internal ex;
    ex.getMessage() << "passed SubRequest ist NULL" << std::endl; 
    throw ex;
  }
  /// get the files using the Filelisthelper
  FileListHelper flp;
  Cuuid_t cuuid = stringtoCuuid(sreq->cuuid());
  std::vector<std::string>* filelist = flp.getFilePathnames(sreq,cuuid);
  std::string reqid; /// the requestid from the stager, which we get, 
                     /// when we send the remove request
  
  struct stage_options opts;
  
  /// we need the stager name and service class for correct removal
  /// the port and version has to be set be the BaseClient. Nevertheless the 
  /// values have to be 0.
  opts.stage_host = (char*)ptr_server->getStagerName().c_str();
  opts.stage_port = 0; 
  opts.stage_version = 0;
  /// set the service class information from repackrequest
  getServiceClass(&opts, sreq);
  
  if ( !filelist->size() ) {
    /// this means that there are no files for a repack tape in SUBREQUEST_READYFORCLEANUP
    /// => ERROR, this must not happen!
    /// but we leave it to the Monitor Service to solve that problem
    castor::exception::Internal ex;
    ex.getMessage() << "No files found during cleanup phase for " << sreq->vid() 
              << std::endl;
    throw ex;
  }
  /* 
  /// here we build the request 
  castor::stager::StageRmRequest* req = new castor::stager::StageRmRequest();
  for ( int i=0; i< filelist->size(); i++){
    castor::stager::SubRequest* subreq = new castor::stager::SubRequest();
    subreq->setFileName(filelist->at(i) ); 
    subreq->setRequest(req);
    req->addSubRequests(subreq);
  }

  /// .. and send it to the stager
  std::vector<castor::rh::FileResponse*>* result = NULL;
  try {
    result = sendStagerFileRequest(req, &reqid, &opts);
  }catch ( castor::exception::Exception e){
    castor::exception::Internal internal;
    internal.getMessage() << "Sending Request to Stager failed!" 
                          << e.getMessage()
                          << std::endl;
    throw internal;

  }
  */
  struct stage_filereq requests[filelist->size()];
  struct stage_fileresp* responses;
  for ( int i=0; i< filelist->size(); i++)
   requests[i].filename = (char*)filelist->at(i).c_str();
  int nbresps =0;
  int rc =0;

  ///send the request to the server to remove files 
  rc = stage_rm( requests,filelist->size(),&responses, &nbresps, NULL ,&opts); 
  if ( rc >= 0 ) {
    /// we don't need the answers
    for (int i=0; i< nbresps; i++) {
      if (responses[i].errorCode) free(responses[i].errorMessage);
      free(responses[i].filename);
    }
    free(responses);
    //result->clear();
  }
}



		
	} //END NAMESPACE REPACK
} //END NAMESPACE CASTOR

