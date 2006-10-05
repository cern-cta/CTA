/******************************************************************************
 *                      castor/repack/RepackMonitor.cpp
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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

// Include Files
#include "RepackMonitor.hpp"



namespace castor {
  namespace repack {


//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
  RepackMonitor::RepackMonitor(RepackServer *svr){
    m_dbhelper = new DatabaseHelper();
    ptr_server = svr;
  }


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
  RepackMonitor::~RepackMonitor(){
    delete m_dbhelper;
  }


//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
  void RepackMonitor::run(void *param) throw() {

    //TODO: own query for this purpose
    Cuuid_t cuuid;
    std::vector<RepackSubRequest*>* tapelist = NULL;
    std::vector<RepackSubRequest*>* tapelist2 = NULL;
    try {
      tapelist = m_dbhelper->getAllSubRequestsStatus(SUBREQUEST_STAGING);
      tapelist2 = m_dbhelper->getAllSubRequestsStatus(SUBREQUEST_MIGRATING);
    }catch ( castor::exception::Exception e){
      castor::dlf::Param params[] =
       {castor::dlf::Param("Error Message", e.getMessage().str() ) };
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 5, 1, params);
      return;
    }
  
    try{
      if (tapelist != NULL ) { 
        for (int i=0; i< tapelist->size(); i++){
          //m_dbhelper->lock(tapelist->at(i));
          cuuid = stringtoCuuid(tapelist->at(i)->cuuid());
          updateTape(tapelist->at(i));
          m_dbhelper->unlock();
          freeRepackObj(tapelist->at(i)->requestID());
	      }
        tapelist->clear();
        delete tapelist;
      }
      if ( tapelist2 != NULL ) {
        for (int i=0; i< tapelist2->size(); i++){
          //m_dbhelper->lock(tapelist2->at(i));
          cuuid = stringtoCuuid(tapelist2->at(i)->cuuid());
          updateTape(tapelist2->at(i));
          m_dbhelper->unlock();
          freeRepackObj(tapelist2->at(i)->requestID());
        }
        tapelist2->clear();
        delete tapelist2;
      }
      
    }
    catch ( castor::exception::Exception e){
      castor::dlf::Param params[] =
       {castor::dlf::Param("Error Message", e.getMessage().str() ),
        castor::dlf::Param("ErrorCode", e.code() )};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 5, 2, params);
    }
  }

//------------------------------------------------------------------------------
// stop
//------------------------------------------------------------------------------
  void RepackMonitor::stop() throw() {
    
  }

//------------------------------------------------------------------------------
// getStats
//------------------------------------------------------------------------------
int RepackMonitor::getStats(RepackSubRequest* sreq, 
                            struct stage_filequery_resp **responses,
                            int* nbresps) 
throw (castor::exception::Internal)
{
   
  int rc;
  Cuuid_t cuuid;
  cuuid = stringtoCuuid(sreq->cuuid());

  /** for stager request */
  struct stage_query_req request;
  struct stage_options opts;

  /** we query by request, easier for stager to fill */
  request.type = BY_REQID;
  request.param = (void*)sreq->cuuid().c_str();
  opts.stage_host = (char*)ptr_server->getStagerName().c_str();
  opts.stage_port = 0;
  opts.stage_version = 0;
  /// set the service class information from repackrequest
  getServiceClass(&opts, sreq);
  
  rc = errno = serrno = *nbresps = 0;
  /// Send request to stager 
  rc = stage_filequery(&request,
                      1,
                      responses,
                      nbresps,
                      &(opts));

  if ( rc == -1 || (*responses)[0].errorCode == 22 ){
    castor::exception::Exception ex(serrno);
    ex.getMessage() << (*responses[0]).errorMessage << "(" << sstrerror(serrno) << ")";
    throw ex;
  }
}




//------------------------------------------------------------------------------
// updateTape
//------------------------------------------------------------------------------
void RepackMonitor::updateTape(RepackSubRequest *sreq)
                          throw (castor::exception::Internal)
{ 
  int nbresps = 0;
  /** status counters */
  int canbemig_status,stagein_status,waitingmig_status, stageout_status, invalid_status;
  canbemig_status = stagein_status = waitingmig_status = stageout_status = invalid_status = 0;

  struct stage_filequery_resp *responses = NULL;
  Cuuid_t cuuid;  
  cuuid = stringtoCuuid(sreq->cuuid());

  try {
    /** get the stats by quering the stager */
    getStats(sreq, &responses, &nbresps);
  }catch (castor::exception::Exception ex){
    castor::dlf::Param params[] =
    {castor::dlf::Param("Error Message", ex.getMessage().str() ),
    castor::dlf::Param("Responses", nbresps )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 41, 2, params);
    //if ( nbresps) free_filequery_resp(responses, nbresps);
    return;
  }

    /// see, in which status the files are 
  for ( int i=0; i<nbresps; i++ ) {
    if ( responses[i].errorCode > 0 ){
      castor::dlf::Param params[] =
      {castor::dlf::Param("Error Message", responses[i].errorMessage)};
        castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 41, 1, params);
    }
    switch ( responses[i].status ){
      case FILE_CANBEMIGR  : canbemig_status++;  break;
      case FILE_STAGEIN    : stagein_status++;   break;
      case FILE_WAITINGMIGR: waitingmig_status++;break;
      case FILE_STAGEOUT:    stageout_status++;break;
      case FILE_INVALID_STATUS: invalid_status++; break;
    }
    //free_stager_response(&responses[i]);
  }
  //free (responses);

  free_filequery_resp(responses, nbresps);

  if ( invalid_status ) {
    stage_trace(3,"Found %d files in invalid status", invalid_status);
    castor::dlf::Param params[] =
      {castor::dlf::Param("Number", invalid_status)};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_WARNING, 42, 1, params);
  }

  /// we only update the subrequest, if something has changed */
  if ( (waitingmig_status + canbemig_status) !=  sreq->filesMigrating() 
        || stagein_status != sreq->filesStaging() ||  invalid_status != sreq->filesFailed()
  )
  {
    
    sreq->setFilesMigrating( waitingmig_status + canbemig_status );
    sreq->setFilesStaging( stagein_status );
    sreq->setFilesFailed( invalid_status );
    
    stage_trace(3,"Updating RepackSubRequest: Mig: %d\tStaging: %d\t Invalid %d\n",
        sreq->filesMigrating(), sreq->filesStaging(),sreq->filesFailed() );  


    /// if we find migration candidates, we just change the status from staging, 
    ///    or if all files are staged 
    
    if ( waitingmig_status + canbemig_status 
        && sreq->status() != SUBREQUEST_MIGRATING )
    {
      sreq->setStatus(SUBREQUEST_MIGRATING);
      castor::dlf::Param params[] =
      {castor::dlf::Param("VID", sreq->vid()),
      castor::dlf::Param("STATUS", sreq->status())};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 40, 2, params);
    }
    
    else if ( !(waitingmig_status + canbemig_status + stagein_status + stageout_status + invalid_status) )
    {
      sreq->setStatus(SUBREQUEST_READYFORCLEANUP);
      castor::dlf::Param params[] =
      {castor::dlf::Param("VID", sreq->vid()),
      castor::dlf::Param("STATUS", sreq->status())};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 40, 2, params);
    }
    
    /** update the RepackSubRequest with the latest stats */
    m_dbhelper->updateSubRequest(sreq,false,cuuid);
  }
}


//------------------------------------------------------------------------------
// getFinishedTapes
//------------------------------------------------------------------------------
  RepackSubRequest* RepackMonitor::getFinishedTapes() throw (castor::exception::Internal)
  {
    

  }

  } //END NAMESPACE REPACK
}//END NAMESPACE CASTOR

