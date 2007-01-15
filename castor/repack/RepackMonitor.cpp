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
#include "castor/rh/FileQryResponse.hpp"
#include "castor/stager/QueryParameter.hpp"
#include "castor/stager/StageFileQueryRequest.hpp"


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
  RepackMonitor::~RepackMonitor() throw() {
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
void RepackMonitor::getStats(RepackSubRequest* sreq,
                            std::vector<castor::rh::FileQryResponse*>* fr)
                                      throw (castor::exception::Exception)
{
  
  // Uses a BaseClient to handle the request
  castor::client::BaseClient client(stage_getClientTimeout());
  std::vector<castor::rh::Response*>respvec;
  castor::client::VectorResponseHandler rh(&respvec);
  castor::stager::StageFileQueryRequest req;
  struct stage_options opts;
  memset(&opts,0,sizeof(stage_options));
  client.setOption(NULL);

  /// set the service class information from repackrequest
  getStageOpts(&opts, sreq);
  client.setOption(&opts);

  castor::stager::RequestHelper reqh(&req);
  reqh.setOptions(&opts);
 
  // Prepare the Request
  castor::stager::QueryParameter *par = new castor::stager::QueryParameter();
  par->setQueryType( (castor::stager::RequestQueryType) BY_REQID );
  par->setValue(sreq->cuuid());
  par->setQuery(&req);
  stage_trace(3,"Querying the Stager for reqID %s ",sreq->cuuid().c_str());
  req.addParameters(par);

  // send the FileRequest, throws Exception ! 
  client.sendRequest(&req, &rh);
    
  for (int i=0; i<(int)respvec.size(); i++) {
     castor::rh::FileQryResponse* tmp = dynamic_cast<castor::rh::FileQryResponse*>(respvec[i]);
     fr->push_back(tmp);
  }

  /* in case of one answer, it might be that the request is unknown 
   * then we throw an exception with EINVAL
   */
  if ( fr->size() == 1 ){
    if ( fr->at(0)->errorCode() == EINVAL ) {
      castor::exception::InvalidArgument inval;
      delete fr->at(0);
      fr->clear();
      throw inval;
    }
  }

}


//------------------------------------------------------------------------------
// updateTape
//------------------------------------------------------------------------------
void RepackMonitor::updateTape(RepackSubRequest *sreq)
                          throw (castor::exception::Internal)
{ 
  /** status counters */
  int canbemig_status,stagein_status,waitingmig_status, stageout_status, invalid_status;
  canbemig_status = stagein_status = waitingmig_status = stageout_status = invalid_status = 0;

  Cuuid_t cuuid;  
  cuuid = stringtoCuuid(sreq->cuuid());
  std::vector<castor::rh::FileQryResponse*> fr;
  std::vector<castor::rh::FileQryResponse*>::iterator response;

  try {

    /** get the stats by quering the stager */
    getStats(sreq, &fr);

  }catch (castor::exception::InvalidArgument inval){
    castor::dlf::Param params[] =
    {castor::dlf::Param("New status", SUBREQUEST_READYFORCLEANUP)};
     castor::dlf::dlf_writep(cuuid, DLF_LVL_WARNING, 48, 1, params);
    sreq->setStatus(SUBREQUEST_READYFORCLEANUP);
    m_dbhelper->updateSubRequest(sreq,false,cuuid);
    return;
  }catch (castor::exception::Exception ex){
    castor::dlf::Param params[] =
    {castor::dlf::Param("Error Message", ex.getMessage().str() )};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 41, 2, params);
    while ( fr.size() ) delete fr.at(0);
    return;
  }

  /** we ignore that we didn't find anything for the Request ID */
  if ( fr.size() == 0  ){
    stage_trace(1,"No Results found for CUUID. Will try again later." );
    castor::dlf::dlf_writep(cuuid, DLF_LVL_WARNING, 47, 0, NULL);
    return;
  }

  /* in case of one answer, it might be that the request is unknown */
  if ( fr.size() == 1 ){
    if ( fr.at(0)->errorCode() == EINVAL ) {
      castor::dlf::Param params[] =
      {castor::dlf::Param("New status", SUBREQUEST_READYFORCLEANUP)};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_WARNING, 48, 1, params);
      sreq->setStatus(SUBREQUEST_READYFORCLEANUP);
      m_dbhelper->updateSubRequest(sreq,false,cuuid);
      delete fr.at(0);
    }
  }

  /// see, in which status the files are 
  response = fr.begin();
  while ( response != fr.end() ) {
    if ( (*response)->errorCode() > 0 ){
      castor::dlf::Param params[] =
      {castor::dlf::Param("Error Message", (*response)->errorMessage()) };
      castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 41, 1, params);
    }
    switch ( (*response)->status() ){
      case FILE_CANBEMIGR     :
      case FILE_BEINGMIGR     : canbemig_status++;  break;
      case FILE_STAGEIN       : stagein_status++;   break;
      case FILE_WAITINGMIGR   : waitingmig_status++;break;
      case FILE_STAGEOUT      : stageout_status++;  break;
      case FILE_INVALID_STATUS: invalid_status++;   break;
    }
    response++;
  }

  if ( invalid_status ) {
    stage_trace(3,"Found  %d files in invalid status.", invalid_status);
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
    
    stage_trace(3,"Updating RepackSubRequest with %d responses: Mig: %d\tStaging: %d\t Invalid %d\n",
    fr.size(), sreq->filesMigrating(), sreq->filesStaging(),sreq->filesFailed() );  


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

  /* Cleanup : delete the responses */
  response = fr.begin();
  while ( response != fr.end() ) {
    delete (*response);
    response++;
  }

}


  } //END NAMESPACE REPACK
}//END NAMESPACE CASTOR

