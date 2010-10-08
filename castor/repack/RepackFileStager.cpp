/******************************************************************************
 *                      RepackFileStager.cpp
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
 * @(#)$RCSfile: RepackFileStager.cpp,v $ $Revision: 1.52 $ $Release$ $Date: 2009/06/22 09:26:14 $ $Author: gtaur $
 *
 * @author Giulia Taurelli
 *****************************************************************************/
 
#include "RepackFileStager.hpp"
#include "RepackSubRequestStatusCode.hpp"
#include "RepackUtility.hpp"


#include "FileListHelper.hpp"

#include "castor/stager/StageRepackRequest.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/rh/Response.hpp"
#include "castor/rh/FileResponse.hpp"
#include "castor/client/VectorResponseHandler.hpp"
#include "castor/client/BaseClient.hpp"
#include "RepackRequest.hpp"
#include "IRepackSvc.hpp"

#include "castor/Services.hpp"
#include "castor/IService.hpp"

namespace castor {
	namespace repack {
		
	
		
//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RepackFileStager::RepackFileStager(RepackServer* srv) 
{
  ptr_server = srv;
  	
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
RepackFileStager::~RepackFileStager() throw() {
}


//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void RepackFileStager::run(void*) throw() {
  std::vector<RepackSubRequest*> sreqs;
  std::vector<RepackSubRequest*>::iterator sreq;
  try {

    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 18, 0, 0);
    
    // connect to the db
    // service to access the database
    castor::IService* dbSvc = castor::BaseObject::services()->service("OraRepackSvc", castor::SVC_ORAREPACKSVC);
    castor::repack::IRepackSvc* oraSvc = dynamic_cast<castor::repack::IRepackSvc*>(dbSvc);
  

    if (0 == oraSvc) {    
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 1 , 0, NULL);
      return;
    }

          
    // start

    sreqs = oraSvc->getSubRequestsByStatus(RSUBREQUEST_TOBESTAGED,true); // segments needed
    sreq=sreqs.begin();
    while (sreq != sreqs.end()){
      castor::dlf::Param params[] =
	{castor::dlf::Param("VID", (*sreq)->vid()),
	 castor::dlf::Param("ID", (*sreq)->id()),
	 castor::dlf::Param("STATUS", RepackSubRequestStatusCodeStrings[(*sreq)->status()])};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 21, 3, params);
      try {

	startRepack(*sreq,oraSvc);
	
      } catch (castor::exception::Exception& e){
	castor::dlf::Param params[] =
	  {
	    castor::dlf::Param("Precise Message", e.getMessage().str()),
	    castor::dlf::Param("VID", (*sreq)->vid())
	  };
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 58, 2, params);

      }
      freeRepackObj(*sreq);
      sreq++;
    }
     
  }catch ( castor::exception::Exception e) {
    // db error asking the tapes to stage

    castor::dlf::Param params[] = {castor::dlf::Param("ErrorCode", e.code()),
				   castor::dlf::Param("Precise Message", e.getMessage().str())};
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 57, 2,params);
  }    

}


//------------------------------------------------------------------------------
// stop
//------------------------------------------------------------------------------
void RepackFileStager::stop() throw() {
}



//------------------------------------------------------------------------------
// startRepack
//------------------------------------------------------------------------------
void RepackFileStager::startRepack(RepackSubRequest* sreq,castor::repack::IRepackSvc* oraSvc) throw (castor::exception::Exception){
  _Cuuid_t cuuid = stringtoCuuid(sreq->cuuid());
  std::vector<RepackSegment*> failedSegments;
  std::vector<RepackSegment*>::iterator failedSegment;
  try {
    failedSegments =stageFiles(sreq);
  }catch (castor::exception::Exception& ex){
    // failure in stagin the file
    castor::dlf::Param params[] =
    {castor::dlf::Param("Standard Message", sstrerror(ex.code())),
     castor::dlf::Param("Precise Message", ex.getMessage().str()),
     castor::dlf::Param("VID", sreq->vid())
    };
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 29, 3, params);
    // just one segment without fileid => all failed    

    RepackSegment* seg= new RepackSegment();
    seg->setErrorCode(ex.code());
    seg->setErrorMessage(ex.getMessage().str());
    seg->setFileid(0);
    failedSegments.push_back(seg);
  }

  // let's update the database 
  if (failedSegments.empty())
    oraSvc->updateSubRequest(sreq);
  else 
    oraSvc->updateSubRequestSegments(sreq,failedSegments);
  

}



//------------------------------------------------------------------------------
// stageFiles
//------------------------------------------------------------------------------
std::vector<RepackSegment*> RepackFileStager::stageFiles(RepackSubRequest* sreq) 
                                            throw (castor::exception::Exception)
{

  std::string reqId = "";
  _Cuuid_t cuuid = stringtoCuuid(sreq->cuuid());
  castor::stager::StageRepackRequest req;
  std::vector<castor::stager::SubRequest*>::iterator stager_subreq;
  
  /** check if the passed RepackSubRequest has segment members */

  if ( !sreq->repacksegment().size() ){
    // no file to submit
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 22, 0, 0);
    sreq->setStatus(RSUBREQUEST_TOBECLEANED);  
    return sreq->repacksegment();
  }

  // ---------------------------------------------------------------
  // This part has to be removed, if the stager also accepts only fileid
  // as parameter. For now we have to get the paths first.
  
  FileListHelper filehelper(ptr_server->getNsName());
  
  std::vector<std::string> filelist = filehelper.getFilePathnames(sreq);

 
  std::vector<std::string>::iterator filename = filelist.begin();

  /// here we build the Request for the stager 

  while (filename != filelist.end()) {
    castor::stager::SubRequest *subreq = new castor::stager::SubRequest();
    subreq->setFileName((*filename));
    subreq->setProtocol(ptr_server->getProtocol()); // we have to specify a protocol, otherwise our call is not scheduled!
    subreq->setRequest(&req);
    req.addSubRequests(subreq);
    filename++;
  }

  
  filelist.clear();

  req.setRepackVid(sreq->vid());
  /// We need to set the stage options. 
  struct stage_options opts;

  /// the service class information is checked and 
  getStageOpts(&opts, sreq);  /// would through an exception, if repackrequest is not available

  /// Msg: Staging files
  castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 23, 0, 0);

  /// Here, we send the stager request and Update the db with the result
  std::vector<RepackSegment*>failedSegments;
  try {
    failedSegments=sendStagerRepackRequest(sreq, &req, &reqId, &opts);
  } catch (castor::exception::Exception& e){
    /// delete the allocated Stager SubRequests
    stager_subreq = req.subRequests().begin();
    while ( stager_subreq != req.subRequests().end() )
      delete (*stager_subreq);
    throw e;
  }
  stager_subreq = req.subRequests().begin();
  while ( stager_subreq != req.subRequests().end() )
    delete (*stager_subreq);

  return failedSegments;
}


//-----------------------------------------------------------------------------
// sendStagerRepackRequest
//------------------------------------------------------------------------------
std::vector<RepackSegment*> RepackFileStager::sendStagerRepackRequest(  RepackSubRequest* rsreq,
                                                castor::stager::StageRepackRequest* req,
                                                std::string *reqId,
                                                struct stage_options* opts
                                              ) throw (castor::exception::Exception)
{
  unsigned int failed = 0; /** counter for failed files during submit */	
  Cuuid_t cuuid = stringtoCuuid(rsreq->cuuid());
  std::vector<RepackSegment*> failedSegments;

  /** Uses a BaseClient to handle the request */
  castor::client::BaseClient client(stage_getClientTimeout(),stage_getClientTimeout());

  client.setOptions(opts);

  client.setAuthorizationId(rsreq->repackrequest()->userId(), rsreq->repackrequest()->groupId());
  
  /** 1. Send the Request */

  std::vector<castor::rh::Response*>respvec;
  castor::client::VectorResponseHandler rh(&respvec);

  try {

    *reqId = client.createClientAndSend(req);

  } catch (castor::exception::Exception& e) {
    // failure to reach the stager
    //clean up
    std::vector<castor::rh::Response*>::iterator respToDelete=respvec.begin();
    while (respToDelete!=respvec.end()){
      if (*respToDelete) delete *respToDelete;
      respToDelete++; 
    }
    respvec.clear();
    throw e;
    
  }

  /** 2. Get the  answer */

  rsreq->setCuuid(*reqId);
  rsreq->setSubmitTime(time(NULL));
  rsreq->setStatus(RSUBREQUEST_ONGOING);
  rsreq->setFilesStaging(req->subRequests().size()); 



  castor::dlf::Param param[] = {castor::dlf::Param("New reqID", *reqId)};
  castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 31, 1, param);

  try {
    client.pollAnswersFromStager(req,&rh);
  } catch (castor::exception::Exception& ex){
    // time out that should be ignored .. however it has been submited

    castor::dlf::Param params[] =
    {castor::dlf::Param("Standard Message", sstrerror(ex.code())),
     castor::dlf::Param("Precise Message", ex.getMessage().str()),
     castor::dlf::Param("VID", req->repackVid())
    };
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 32, 3, params);

    //clean up
    std::vector<castor::rh::Response*>::iterator respToDelete=respvec.begin();
    while (respToDelete!= respvec.end()){
      if (*respToDelete) delete *respToDelete;
      respToDelete++; 
    }
    respvec.clear();

    return failedSegments; // don't know about how many files failed
  }
  
   /** 3. Surf the stager answer  */

  /** like the normale api call, we have to check for errors in the answer*/

  for (unsigned int i=0; i<respvec.size(); i++) {

    // Casting the response into a FileResponse !
    castor::rh::FileResponse* fr = dynamic_cast<castor::rh::FileResponse*>(respvec[i]);
    if (0 == fr) {
      // let's try later
      castor::dlf::Param params[] =
	{castor::dlf::Param("Precise Message", "Error in dynamic cast, stager response was not a file response. \n"),
	 castor::dlf::Param("VID", req->repackVid())};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 32, 2, params);

      //clean
      
      for (unsigned int j=i; j<respvec.size();j++)
	if (respvec[j]) delete respvec[i];

      return failedSegments;  // don't know about how many files failed
    }

    if ( fr->errorCode() || fr->status() != 6 ){
      struct Cns_fileid fileid;
      memset(&fileid, '\0', sizeof(Cns_fileid));
      fileid.fileid = fr->fileId();
      castor::dlf::Param param[] = 
      {castor::dlf::Param("Filename",fr->castorFileName()),
       castor::dlf::Param("Message", fr->errorMessage()) 
      };
      castor::dlf::dlf_writep(stringtoCuuid(rsreq->cuuid()), DLF_LVL_ERROR, 33, 1, param, &fileid);
      RepackSegment* failedSeg= new RepackSegment();
      failedSeg->setFileid(fr->fileId());
      failedSeg->setErrorCode(fr->errorCode());
      failedSeg->setErrorMessage(fr->errorMessage());
      failedSegments.push_back(failedSeg);
      failed++;
    }
    delete fr;
    fr=NULL;
  }
  
  // global repacksubrequest values

  /// we want to know how many files were successfully submitted
  
  rsreq->setFilesFailedSubmit(failed);

  /// in case all files failed, we set the status to failed.

  if ( failed == req->subRequests().size() ){
    rsreq->setStatus(RSUBREQUEST_TOBECLEANED);
    rsreq->setFilesStaging(0);
  } else {
    rsreq->setFilesStaging(rsreq->filesStaging()-failed);
  }
  respvec.clear();
  return failedSegments;
 
}


		
} //END NAMESPACE REPACK
}//END NAMESPACE CASTOR
