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
#include "FileListHelper.hpp"
#include "stager_client_api.h"
#include "castor/stager/QueryParameter.hpp"
#include "castor/stager/StageFileQueryRequest.hpp"
#include "RepackSubRequestStatusCode.hpp"

#include "castor/client/BaseClient.hpp"
#include "castor/client/VectorResponseHandler.hpp"
#include "castor/repack/RepackUtility.hpp"
#include "castor/exception/InvalidArgument.hpp"
#include "IRepackSvc.hpp"

#include <sys/types.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <u64subr.h>
#include <time.h>

#include "castor/Services.hpp"
#include "castor/IService.hpp"

namespace castor {
  namespace repack {



//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
  RepackMonitor::RepackMonitor(RepackServer *svr){    
    ptr_server = svr;
  }


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
  RepackMonitor::~RepackMonitor() throw() {
  }


//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
  void RepackMonitor::run(void*) throw() {
    
    std::vector<RepackSubRequest*> tapelist;
    std::vector<RepackSubRequest*>::iterator tape;

    // connect to the db
    // service to access the database
    castor::IService* dbSvc = castor::BaseObject::services()->service("OraRepackSvc", castor::SVC_ORAREPACKSVC);
    castor::repack::IRepackSvc* oraSvc = dynamic_cast<castor::repack::IRepackSvc*>(dbSvc);
    

    if (0 == oraSvc) {    
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 1 , 0, NULL);
      return;
    }


    try {
     
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 34, 0, 0);
      tapelist = oraSvc->getSubRequestsByStatus(RSUBREQUEST_ONGOING,false);
      tape=tapelist.begin();

      while (tape != tapelist.end()){ 
	if (*tape != NULL){
	  castor::dlf::Param params[] =
	    {castor::dlf::Param("VID", (*tape)->vid()),
	     castor::dlf::Param("ID", (*tape)->id()),
	     castor::dlf::Param("STATUS", RepackSubRequestStatusCodeStrings[(*tape)->status()])};
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 35, 3, params);

	// get information for this tape querying the stager
	try {
	  updateTape(*tape,oraSvc);
	} catch (castor::exception::Exception e){
	  	  
	  castor::dlf::Param params[] =
	    {
	      castor::dlf::Param("Precise Message", e.getMessage().str()),
	      castor::dlf::Param("VID", (*tape)->vid())
	    };
	  castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 66, 2, params);
	}

	freeRepackObj(*(tape));

	}

	tape++;
      }

    } catch (castor::exception::Exception e) {
      // db error asking the tapes to Monitor

      castor::dlf::Param params[] = {castor::dlf::Param("ErrorCode", e.code()),
				     castor::dlf::Param("Precise Message", e.getMessage().str())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 65, 2,params);
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

void RepackMonitor::updateTape(RepackSubRequest *sreq, castor::repack::IRepackSvc* oraSvc )
                          throw (castor::exception::Exception)
{ 
  /** status counters */

  int canbemig_status,stagein_status,waitingmig_status, stageout_status, staged_status, invalid_status;
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
    oraSvc->updateSubRequest(sreq);
    return;
  } catch (castor::exception::Exception e) {
    // the stager might time out
    sreq->setStatus(RSUBREQUEST_ONGOING);
    oraSvc->updateSubRequest(sreq);
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

  // call to the nameserver

  FileListHelper filehelper(ptr_server->getNsName());
  
  u_signed64 filesOnTape= filehelper.countFilesOnTape(sreq->vid());
  


  if ( filesOnTape==0 || (waitingmig_status + canbemig_status) !=  sreq->filesMigrating() 
        || stagein_status != sreq->filesStaging() ||  invalid_status != sreq->filesFailed() 
  )
  {
    
    // UPDATE NEEDED
    
    if ( !(waitingmig_status + canbemig_status + stagein_status + stageout_status) || filesOnTape==0 ){

      u_signed64 timeSpent=sreq->submitTime();
      if (timeSpent) 
	timeSpent=time(NULL)-timeSpent;

      castor::dlf::Param params[] =
	{castor::dlf::Param("VID", sreq->vid()),
	 castor::dlf::Param("STATUS",RepackSubRequestStatusCodeStrings[sreq->status()]),
	 castor::dlf::Param("time", timeSpent)
	};
      

      if (filesOnTape==0 && (waitingmig_status + canbemig_status + stagein_status + stageout_status)){
	//inconsistency left around

	castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 69, 3,params);

      }

      // REPACK is finished

      invalid_status+=sreq->filesFailedSubmit();
      sreq->setStatus(RSUBREQUEST_TOBECLEANED);
      oraSvc->updateSubRequest(sreq);

      if (!invalid_status){

	// everything went fine

	 castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 37, 3, params);

      } else{
	// we faced some errors
	  castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 39, 3, params);
	
	}	
      
    } else {
    
    // Still ON_GOING

      sreq->setFilesMigrating( waitingmig_status + canbemig_status );
      sreq->setFilesStaging( stagein_status );
      sreq->setFilesFailed( invalid_status );
      
      // compute staged files

      staged_status=sreq->files() - filesOnTape;
      
      if (staged_status<0){

	castor::dlf::Param params[] =
	  {castor::dlf::Param("VID", sreq->vid())};
	castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 68, 1,params);

	sreq->setFiles(filesOnTape);
	sreq->setFilesFailedSubmit(sreq->filesFailedSubmit() - staged_status);
	staged_status=0;

      }

      u_signed64 data=0;
      if (sreq->files())
	data=sreq->xsize()/sreq->files();
      data=data*staged_status;
      
      char buf[21];
      u64tostru(data, buf, 0);

      castor::dlf::Param params[] =
	{castor::dlf::Param("VID", sreq->vid()),
	 castor::dlf::Param("toRecall",stagein_status ),
	 castor::dlf::Param("toMigrate",waitingmig_status + canbemig_status),
	 castor::dlf::Param("failed",invalid_status+sreq->filesFailedSubmit()),
	 castor::dlf::Param("done",staged_status),
	 castor::dlf::Param("filesStillOnTape", filesOnTape),
	 castor::dlf::Param("transferedDataVolume", buf)
	};


      castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 40, 7, params);


      
      sreq->setFilesStaged( staged_status );
      sreq->setStatus(RSUBREQUEST_ONGOING);
    
      oraSvc->updateSubRequest(sreq);
  
    }
  }
  fr.clear();

}


  } //END NAMESPACE REPACK
}//END NAMESPACE CASTOR

