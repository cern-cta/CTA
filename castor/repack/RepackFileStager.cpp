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
 * @(#)$RCSfile: RepackFileStager.cpp,v $ $Revision: 1.48 $ $Release$ $Date: 2009/01/14 17:31:17 $ $Author: sponcec3 $
 *
 *
 *
 * @author Felix Ehm
 *****************************************************************************/
 
#include "RepackFileStager.hpp"
#include "castor/rh/FileQryResponse.hpp"
#include "castor/stager/StageRmRequest.hpp"
#include "castor/repack/RepackSubRequestStatusCode.hpp"
#include "castor/Services.hpp"
#include "castor/repack/RepackUtility.hpp"
#include "castor/client/BaseClient.hpp"

namespace castor {
	namespace repack {
		
	
		
//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RepackFileStager::RepackFileStager(RepackServer* srv) 
{
  ptr_server = srv;
  m_filehelper = new FileListHelper(ptr_server->getNsName());
  m_dbSvc = NULL;	
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
RepackFileStager::~RepackFileStager() throw() {
  if (m_dbSvc != NULL) delete m_dbSvc;
  m_dbSvc=NULL;
  delete m_filehelper;
}


//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void RepackFileStager::run(void *param) throw() {
  std::vector<RepackSubRequest*> sreqs;
  std::vector<RepackSubRequest*>::iterator sreq;
  try {
    if (m_dbSvc == NULL ) {
      // service to access the database
      castor::IService* orasvc = castor::BaseObject::services()->service("OraRepackSvc", castor::SVC_ORAREPACKSVC);
  
      m_dbSvc = dynamic_cast<castor::repack::IRepackSvc*>(orasvc);
      if (m_dbSvc == 0) {
	// FATAL ERROR
	castor::dlf::Param params0[] =
	  {castor::dlf::Param("Standard Message", "RepackFileStager fatal error"),};
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 2, 1, params0);
	return;
      }
    
    }
    castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 18, 0, 0);
        
    // remove
          
    sreqs = m_dbSvc->getSubRequestsByStatus(RSUBREQUEST_TOBEREMOVED,true); // segments needed
    sreq=sreqs.begin();
    
    while (sreq != sreqs.end()){
      castor::dlf::Param params[] =
	 {castor::dlf::Param("VID", (*sreq)->vid()),
	   castor::dlf::Param("ID", (*sreq)->id()),
	   castor::dlf::Param("STATUS", (*sreq)->status())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 19, 3, params);
      abortRepack(*sreq);
      freeRepackObj(*sreq);
      sreq++;
    }
  
 	 
    // restart
        
    sreqs = m_dbSvc->getSubRequestsByStatus(RSUBREQUEST_TOBERESTARTED,false); // segments not needed
    sreq=sreqs.begin();
    while (sreq != sreqs.end()){
      castor::dlf::Param params[] =
	{castor::dlf::Param("VID", (*sreq)->vid()),
	  castor::dlf::Param("ID", (*sreq)->id()),
	 castor::dlf::Param("STATUS", (*sreq)->status())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 20, 3, params);

      m_dbSvc->restartSubRequest((*sreq)->id());
      freeRepackObj(*sreq);
      sreq++;
    }
    
    // start

    sreqs = m_dbSvc->getSubRequestsByStatus(RSUBREQUEST_TOBESTAGED,true); // segments needed
    sreq=sreqs.begin();
    while (sreq != sreqs.end()){
      castor::dlf::Param params[] =
	{castor::dlf::Param("VID", (*sreq)->vid()),
	 castor::dlf::Param("ID", (*sreq)->id()),
	 castor::dlf::Param("STATUS", (*sreq)->status())};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 21, 3, params);

      startRepack(*sreq);
      freeRepackObj(*sreq);
      sreq++;
    }
     
  }catch (...) {
    if (!sreqs.empty()){
      sreq=sreqs.begin();
      while (sreq != sreqs.end()){
	freeRepackObj(*sreq);
	sreq++;
      }
    }
  }    

}


//------------------------------------------------------------------------------
// stop
//------------------------------------------------------------------------------
void RepackFileStager::stop() throw() {
}



//------------------------------------------------------------------------------
// stageFiles
//------------------------------------------------------------------------------
void RepackFileStager::stageFiles(RepackSubRequest* sreq) 
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
    sreq->setStatus(RSUBREQUEST_DONE);
    m_dbSvc->updateSubRequest(sreq);  
    return;
  }

  stage_trace(3,"Getting Pathnames");
  // ---------------------------------------------------------------
  // This part has to be removed, if the stager also accepts only fileid
  // as parameter. For now we have to get the paths first.
  std::vector<std::string>* filelist = m_filehelper->getFilePathnames(sreq);
  std::vector<std::string>::iterator filename = filelist->begin();

  /// here we build the Request for the stager 

  while (filename != filelist->end()) {
    castor::stager::SubRequest *subreq = new castor::stager::SubRequest();
    subreq->setFileName((*filename));
    subreq->setProtocol(ptr_server->getProtocol()); // we have to specify a protocol, otherwise our call is not scheduled!
    subreq->setRequest(&req);
    req.addSubRequests(subreq);
    filename++;
  }

  
  filelist->clear();
  delete filelist;

  req.setRepackVid(sreq->vid());
  /// We need to set the stage options. 
  struct stage_options opts;

  /// the service class information is checked and 
  getStageOpts(&opts, sreq);  /// would through an exception, if repackrequest is not available

  /// Msg: Staging files
  castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 23, 0, 0);


  stage_trace(3,"Sending Request");

  /// Here, we send the stager request and Update the db with the result

  sendStagerRepackRequest(sreq, &req, &reqId, &opts);

  /// delete the allocated Stager SubRequests
  stager_subreq = req.subRequests().begin();
  while ( stager_subreq != req.subRequests().end() )
    delete (*stager_subreq);

}

//------------------------------------------------------------------------------
// abortRepack
//------------------------------------------------------------------------------

void  RepackFileStager::abortRepack(RepackSubRequest* sreq) throw (){
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
    m_dbSvc->updateSubRequest(sreq);
    return;
  }

  sreq->setStatus(RSUBREQUEST_TOBECLEANED);
  m_dbSvc->updateSubRequest(sreq);
  stage_trace(2,"File are sent to stager, repack request updated.");
  castor::dlf::dlf_writep(stringtoCuuid(sreq->cuuid()), DLF_LVL_DEBUG, 25, 0, 0);
 
}



//------------------------------------------------------------------------------
// startRepack
//------------------------------------------------------------------------------
void RepackFileStager::startRepack(RepackSubRequest* sreq) throw (){
  _Cuuid_t cuuid = stringtoCuuid(sreq->cuuid());
  try {
    stageFiles(sreq);
  }catch (castor::exception::Exception ex){
    castor::dlf::Param params[] =
    {castor::dlf::Param("Standard Message", sstrerror(ex.code())),
     castor::dlf::Param("Precise Message", ex.getMessage().str()),
     castor::dlf::Param("VID", sreq->vid())
    };
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 29, 3, params);
    
    std::vector<RepackSegment*>::iterator segFailed=sreq->repacksegment().begin();
    while (segFailed != sreq->repacksegment().end() ){
      (*segFailed)->setErrorCode(-1);
      (*segFailed)->setErrorMessage(ex.getMessage().str());
      segFailed++;
    }
    m_dbSvc->updateSubRequestSegments(sreq,sreq->repacksegment());
    sreq->setStatus(RSUBREQUEST_TOBECLEANED);
    m_dbSvc->updateSubRequest(sreq);
    return;
  }

  m_dbSvc->updateSubRequest(sreq);
  stage_trace(2,"File are sent to stager, repack request updated.");
  castor::dlf::dlf_writep(stringtoCuuid(sreq->cuuid()), DLF_LVL_DEBUG, 25, 0, 0);

}

//-----------------------------------------------------------------------------
// sendStagerRepackRequest
//------------------------------------------------------------------------------
void RepackFileStager::sendStagerRepackRequest(  RepackSubRequest* rsreq,
                                                castor::stager::StageRepackRequest* req,
                                                std::string *reqId,
                                                struct stage_options* opts
                                              ) throw ()
{
  unsigned int failed = 0; /** counter for failed files during submit */	
  Cuuid_t cuuid = stringtoCuuid(rsreq->cuuid());

  /** Uses a BaseClient to handle the request */
  castor::client::BaseClient client(stage_getClientTimeout(),stage_getClientTimeout());

  client.setOptions(opts);

  client.setAuthorizationId(rsreq->repackrequest()->userId(), rsreq->repackrequest()->groupId());
  
  /** 1. Send the Request */

  std::vector<castor::rh::Response*>respvec;
  castor::client::VectorResponseHandler rh(&respvec);

  try {

    *reqId = client.createClientAndSend(req);

  } catch (castor::exception::Exception e) {
    // failure to reach the stager
    std::vector<RepackSegment*>::iterator segFailed= rsreq->repacksegment().begin();
    while (segFailed != rsreq->repacksegment().end()){
      (*segFailed)->setErrorCode(-1);
      (*segFailed)->setErrorMessage(e.getMessage().str());
      segFailed++;
    }
    m_dbSvc->updateSubRequestSegments(rsreq,rsreq->repacksegment()); //failured reported

    rsreq->setStatus(RSUBREQUEST_TOBECLEANED);
    m_dbSvc->updateSubRequest(rsreq);

    //clean up
    std::vector<castor::rh::Response*>::iterator respToDelete=respvec.begin();
    while (respToDelete!=respvec.end()){
      if (*respToDelete) delete *respToDelete;
      respToDelete++; 
    }
    respvec.clear();

    return;
  }

  /** 2. Get the  answer */

  rsreq->setCuuid(*reqId);
  rsreq->setSubmitTime(time(NULL));
  rsreq->setStatus(RSUBREQUEST_ONGOING);
  rsreq->setFilesStaging(req->subRequests().size()); 

  // log

  stage_trace(3," Stagerrequest (now new RepackSubRequest CUUID ): %s",(char*)reqId->c_str());
  castor::dlf::Param param[] = {castor::dlf::Param("New reqID", *reqId)};
  castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 31, 1, param);

  try{
    client.pollAnswersFromStager(req,&rh);
  }catch (castor::exception::Exception ex){
    // time out that should be ignored .. however it has been submited
    
    m_dbSvc->updateSubRequest(rsreq); // to save the new cuuid

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

    return;
  }

   /** 3. Surf the stager answer  */

  /** like the normale api call, we have to check for errors in the answer*/

  std::vector<RepackSegment*> failedSegments;
  std::vector<RepackSegment*>::iterator seg;

  for (unsigned int i=0; i<respvec.size(); i++) {

    // Casting the response into a FileResponse !
    castor::rh::FileResponse* fr = dynamic_cast<castor::rh::FileResponse*>(respvec[i]);
    if (0 == fr) {
      // let's try later
      m_dbSvc->updateSubRequest(rsreq); // to save the new cuuid
      castor::dlf::Param params[] =
	{castor::dlf::Param("Precise Message", "Error in dynamic cast, stager response was not a file response. \n"),
	 castor::dlf::Param("VID", req->repackVid())};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 32, 2, params);

      //clean
      
      for (unsigned int j=i; j<respvec.size();j++)
	if (respvec[j]) delete respvec[i];

      return;
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

  m_dbSvc->updateSubRequestSegments(rsreq,failedSegments);

  // clean up 
  std::vector<RepackSegment*>::iterator toClean=failedSegments.begin();
  while ( toClean!=failedSegments.end()){
    freeRepackObj(*toClean);
    toClean++;
  }
  
  failedSegments.clear();
  respvec.clear();
 
}

//-----------------------------------------------------------------------------------------------------------------
// sendRepackRemoveRequest
//-----------------------------------------------------------------------------------------------------------------


void  RepackFileStager::sendRepackRemoveRequest( RepackSubRequest*sreq)throw(castor::exception::Exception)
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
   * the port has to be set by the BaseClient. Nevertheless the 
   * values has to be 0.
   */
  opts.stage_port = 0; 

  castor::client::BaseClient client(stage_getClientTimeout(),stage_getClientTimeout());

  /* set the service class information from repackrequest */
  
  getStageOpts(&opts, sreq);
  client.setOptions(&opts);

  std::vector<castor::rh::Response *>respvec;
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
