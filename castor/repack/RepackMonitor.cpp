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
#include "castor/Services.hpp"
#include "RepackSubRequestStatusCode.hpp"
#include "RepackUtility.hpp"

namespace castor {
  namespace repack {



//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
  RepackMonitor::RepackMonitor(RepackServer *svr){
    m_dbSvc = NULL;
    
    ptr_server = svr;
  }


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
  RepackMonitor::~RepackMonitor() throw() {
    if (m_dbSvc != NULL) delete m_dbSvc;
    m_dbSvc=NULL;
  }


//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
  void RepackMonitor::run(void *param) throw() {
    
    std::vector<RepackSubRequest*> tapelist;
    std::vector<RepackSubRequest*>::iterator tape;
    try {
      if (m_dbSvc == 0){
	// service to access the database
	castor::IService* orasvc = castor::BaseObject::services()->service("OraRepackSvc", castor::SVC_ORAREPACKSVC);
  
	m_dbSvc = dynamic_cast<castor::repack::IRepackSvc*>(orasvc);
	if (m_dbSvc == 0) {
	  // FATAL ERROR
	  castor::dlf::Param params0[] =
	    {castor::dlf::Param("Standard Message", "RepackMonitor fatal error"),};
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 2, 1, params0);
	  return;
	}
      }

      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 34, 0, 0);
      tapelist = m_dbSvc->getSubRequestsByStatus(RSUBREQUEST_ONGOING,false);
      tape=tapelist.begin();

      while (tape != tapelist.end()){ 
	if (*tape != NULL){
	  castor::dlf::Param params[] =
	    {castor::dlf::Param("VID", (*tape)->vid()),
	     castor::dlf::Param("ID", (*tape)->id()),
	     castor::dlf::Param("STATUS", (*tape)->status())};
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 14, 3, params);

	// get information for this tape querying the stager
	  updateTape(*tape);
	  freeRepackObj(*(tape));

	}

	tape++;
      }

    } catch (...) {
      if (!tapelist.empty()){
	tape=tapelist.begin();
	while (tape != tapelist.end()){
	  freeRepackObj(*tape);
	  tape++;
	}
      }
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

  castor::client::BaseClient client(stage_getClientTimeout(),stage_getClientTimeout());
  std::vector<castor::rh::Response*>respvec;
  castor::client::VectorResponseHandler rh(&respvec);

  castor::stager::StageFileQueryRequest req;
  struct stage_options opts;
  memset(&opts,0,sizeof(stage_options));
  
  /// set the service class information from repackrequest
  getStageOpts(&opts, sreq);
  client.setOptions(&opts);

  // Prepare the Request
  castor::stager::QueryParameter *par = new castor::stager::QueryParameter();
  par->setQueryType( (castor::stager::RequestQueryType) BY_REQID );
  par->setValue(sreq->cuuid());
  par->setQuery(&req);
  stage_trace(3,"Querying the Stager for reqID %s ",sreq->cuuid().c_str());
  req.addParameters(par);

  // send the FileRequest, throws Exception (time out)! 
 
  client.sendRequest(&req, &rh);

  for (int i=0; i<(int)respvec.size(); i++) {
     castor::rh::FileQryResponse* tmp = dynamic_cast<castor::rh::FileQryResponse*>(respvec[i]);
     fr->push_back(tmp);
  }

  /* in case of one answer, it might be that the request is unknown 
   * then we throw an exception with EINVAL */
   
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
                          throw ()
{ 
  /** status counters */

  u_signed64 canbemig_status,stagein_status,waitingmig_status, stageout_status, staged_status, invalid_status;
  canbemig_status = stagein_status = waitingmig_status = stageout_status = staged_status = invalid_status = 0;

  Cuuid_t cuuid;  
  cuuid = stringtoCuuid(sreq->cuuid());
  std::vector<castor::rh::FileQryResponse*> fr;
  std::vector<castor::rh::FileQryResponse*>::iterator response;


  /** get the stats by quering the stager */
  try {
   getStats(sreq, &fr);
  } catch (castor::exception::InvalidArgument inval) {
    // CLEANED BY THE STAGER
    sreq->setStatus(RSUBREQUEST_TOBECLEANED);
    m_dbSvc->updateSubRequest(sreq);
    return;
  } catch (castor::exception::Exception e) {
    // the stager might time out
    sreq->setStatus(RSUBREQUEST_ONGOING);
    m_dbSvc->updateSubRequest(sreq);
    return;
  }

  
  /// see, in which status the files are 

  response = fr.begin();

  while ( response != fr.end() ) {
    if ( (*response)->errorCode() > 0 ){
      castor::dlf::Param params[] =
      {castor::dlf::Param("Error Message", (*response)->errorMessage()) };
      castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 36, 1, params);
    }

    switch ( (*response)->status() ){
      case FILE_CANBEMIGR     :
      case FILE_BEINGMIGR     : 
	canbemig_status++;  
	break;
      case FILE_STAGEIN       : 
	stagein_status++;   
	break;
      case FILE_WAITINGMIGR   : 
	waitingmig_status++;
	break;
      case FILE_STAGEOUT      :
	stageout_status++;  
	break;
      case FILE_STAGED        :   
	// no sense because the gc can remove the diskcopy
	break;
      case FILE_INVALID_STATUS:
      case FILE_PUTFAILED     :
	invalid_status++;
	break; 
                           //putfailed should never appeare 
    }
    delete(*response);
    response++;
  }



  if ( (waitingmig_status + canbemig_status) !=  sreq->filesMigrating() 
        || stagein_status != sreq->filesStaging() ||  invalid_status != sreq->filesFailed() 
  )
  {
    
    // UPDATE NEEDED
    
    if ( !(waitingmig_status + canbemig_status + stagein_status + stageout_status)){

      // REPACK is finished

      invalid_status+=sreq->filesFailedSubmit();
      sreq->setStatus(RSUBREQUEST_TOBECLEANED);
      m_dbSvc->updateSubRequest(sreq);

      if (!invalid_status){

	// everything went fine

	castor::dlf::Param params[] =
	  {castor::dlf::Param("VID", sreq->vid()),
	    castor::dlf::Param("STATUS", sreq->status())};
	 castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 37, 2, params);

      } else{
	// we faced some errors

	  castor::dlf::Param params[] =
	   {castor::dlf::Param("VID", sreq->vid()),
	    castor::dlf::Param("STATUS", sreq->status())};
	  castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 39, 2, params);
	  return;
	}	
      
    } else {
    
    // Still ON_GOING
  
      // call to the nameserver

      FileListHelper filelisthelper(ptr_server->getNsName());
      
      u_signed64 filesOnTape=filelisthelper.countFilesOnTape(sreq->vid());

      castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 40, 0, 0);

      sreq->setFilesMigrating( waitingmig_status + canbemig_status );
      sreq->setFilesStaging( stagein_status );
      sreq->setFilesFailed( invalid_status );
      
      // compute staged files

      staged_status=sreq->files() - filesOnTape;
      
      sreq->setFilesStaged( staged_status );
      sreq->setStatus(RSUBREQUEST_ONGOING);
    
      m_dbSvc->updateSubRequest(sreq);

      stage_trace(3,"Updating RepackSubRequest with %d responses: Mig: %d\tStaging: %d\t Invalid: %d\t FailedSubmition: %d\t Staged: %d\n",
    fr.size(), sreq->filesMigrating(), sreq->filesStaging(),sreq->filesFailed(),sreq->filesFailedSubmit(),sreq->filesStaged());  

    }
  }
  fr.clear();

}


  } //END NAMESPACE REPACK
}//END NAMESPACE CASTOR

