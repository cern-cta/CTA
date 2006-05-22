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
    
    std::vector<RepackSubRequest*>* tapelist = m_dbhelper->getAllSubRequests();
    
    for (int i=0; i< tapelist->size(); i++){
      if ( tapelist->at(i)->status() != SUBREQUEST_READYFORSTAGING 
          && tapelist->at(i)->status() != SUBREQUEST_READYFORCLEANUP
	  && tapelist->at(i)->status() != SUBREQUEST_DONE  )
        getStats(tapelist->at(i));
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
  void RepackMonitor::getStats(RepackSubRequest* sreq) throw (castor::exception::Internal){
   
    int rc, nbresps;
    Cuuid_t cuuid;
    cuuid = stringtoCuuid(sreq->cuuid());
    /** status counters */
    int canbemig_status,stagein_status,waitingmig_status, other_status;
    canbemig_status = stagein_status = waitingmig_status = other_status = 0;
    
    /** for stager request */
    struct stage_query_req request;
    struct stage_filequery_resp *responses;
    struct stage_options opts;
  
    /** we query by request, easier for stager to fill */
    request.type = BY_REQID;
    request.param = (void*)sreq->cuuid().c_str();
    opts.stage_host = (char*)ptr_server->getStagerName().c_str();
    opts.service_class = (char*)ptr_server->getServiceClass().c_str();

    rc = errno = serrno = nbresps = 0;
    /** Send request to stager */
    rc = stage_filequery(&request,
                        1,
                        &responses,
                        &nbresps,
                        &(opts));
    if ( rc != 0 ){
      castor::dlf::Param params[] =
      {castor::dlf::Param("Error Message", sstrerror(serrno) ),
      castor::dlf::Param("Responses", nbresps )};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 41, 1, params);
      return;
    }
    
    /** see, in which status the files are */
    for ( int i=0; i<nbresps; i++ ) {
      switch ( responses[i].status ){
        case FILE_CANBEMIGR  : canbemig_status++;  break;
        case FILE_STAGEIN    : stagein_status++;   break;
        case FILE_WAITINGMIGR: waitingmig_status++;break;
        default : other_status++;
      }
      free_stager_response(&responses[i]);
    }
    free (responses);
    if ( other_status = 10000 )
    /** it can happen, that our filelist we got from the old tape
        is not synchronous with the responses from the stager.
        This can happen whenever a file is deleted before we check that 
        it is being staged (fast garbage collector). So, in this case
        we have to ask the NS, if these missing files from the response
        were already written to tape.*/

    sreq->setFilesMigrating( waitingmig_status + canbemig_status );
    sreq->setFilesStaging( stagein_status );

    /** we just change the status from staging, if we find migration candidates
        or if all files are staged 
    */
    if ( waitingmig_status + canbemig_status 
         && sreq->status() != SUBREQUEST_MIGRATING )
    {
      sreq->setStatus(SUBREQUEST_MIGRATING);
      castor::dlf::Param params[] =
      {castor::dlf::Param("VID", sreq->vid()),
       castor::dlf::Param("VID", sreq->status())};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 40, 1, params);
    }
    
    else if ( !(waitingmig_status + canbemig_status + stagein_status ) ) {
      sreq->setStatus(SUBREQUEST_READYFORCLEANUP);
      castor::dlf::Param params[] =
      {castor::dlf::Param("VID", sreq->vid()),
       castor::dlf::Param("VID", sreq->status())};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 40, 1, params);
    }
    
    /** update the RepackSubRequest with the latest stats */
    m_dbhelper->updateSubRequest(sreq,false,cuuid);
  }



//------------------------------------------------------------------------------
// getFinishedTapes
//------------------------------------------------------------------------------
  RepackSubRequest* RepackMonitor::getFinishedTapes() throw (castor::exception::Internal)
  {
    

  }

  } //END NAMESPACE REPACK
}//END NAMESPACE CASTOR

