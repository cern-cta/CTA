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
 * @(#)$RCSfile: RepackFileStager.cpp,v $ $Revision: 1.24 $ $Release$ $Date: 2006/11/02 17:01:52 $ $Author: felixehm $
 *
 *
 *
 * @author Felix Ehm
 *****************************************************************************/
 
#include "RepackFileStager.hpp"
 
namespace castor {
	namespace repack {
		
	
		
//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RepackFileStager::RepackFileStager(RepackServer* srv) 
{
  ptr_server = srv;
  m_filehelper = new FileListHelper(ptr_server->getNsName());
  m_dbhelper = new DatabaseHelper();	
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
RepackFileStager::~RepackFileStager() throw() {
  delete m_dbhelper;
  delete m_filehelper;
}


//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void RepackFileStager::run(void *param) throw() {

		/// Lets see, if good old pal DB has a Job for us 
    RepackSubRequest* sreq = NULL;
    Cuuid_t cuuid = nullCuuid;

    try {
      sreq = m_dbhelper->checkSubRequestStatus(SUBREQUEST_TOBESTAGED);
      /// no one ready for staging, so look for restartable ones
      if ( sreq == NULL )
        sreq = m_dbhelper->checkSubRequestStatus(SUBREQUEST_RESTART);
    }catch (castor::exception::Exception e){
      castor::dlf::Param params[] = 
      {castor::dlf::Param("Error", e.getMessage().str() )};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 5, 1, params);
      return;
    }

    if ( sreq != NULL ){
      //m_dbhelper->lock(sreq);
      cuuid = stringtoCuuid(sreq->cuuid());
      castor::dlf::Param params[] =
      {castor::dlf::Param("VID", sreq->vid()),
        castor::dlf::Param("ID", sreq->id()),
        castor::dlf::Param("STATUS", sreq->status())};
      castor::dlf::dlf_writep( cuuid, DLF_LVL_SYSTEM, 22, 3, params);
      try {
          /// main handling
          if ( sreq->status() == SUBREQUEST_TOBESTAGED ){ 
            startRepack(sreq);
          }
          else
            restartRepack(sreq);

        m_dbhelper->unlock();

        /// the Request has now status SUBREQUEST_STAGING
        /// try to stage the files, and forget the errors
      }
      catch (castor::exception::Exception ex){
        freeRepackObj(sreq->requestID());
      }
      freeRepackObj(sreq->requestID()); /// always delete from the top
      sreq = NULL;
    }
    m_dbhelper->unlock();
}


//------------------------------------------------------------------------------
// stop
//------------------------------------------------------------------------------
void RepackFileStager::stop() throw() {
}



//------------------------------------------------------------------------------
// stage_files
//------------------------------------------------------------------------------
void RepackFileStager::stage_files(RepackSubRequest* sreq) 
                                            throw (castor::exception::Exception)
{

  int failed = 0;
  std::string reqId = "";
  _Cuuid_t cuuid = stringtoCuuid(sreq->cuuid());
  castor::stager::StageRepackRequest req;
  
  /** check if the passed RepackSubRequest has segment members */
  if ( !sreq->segment().size() ){
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 36, 0, NULL);
    sreq->setStatus(SUBREQUEST_DONE);
    m_dbhelper->updateSubRequest(sreq,false,cuuid);   // doesn't throw
    return;
  }

  // ---------------------------------------------------------------
  // This part has to be removed, if the stager also accepts only fileid
  // as parameter. For now we have to get the paths first.
  std::vector<std::string>* filelist = m_filehelper->getFilePathnames(sreq);
  std::vector<std::string>::iterator filename = filelist->begin();
  // ---------------------------------------------------------------
  //std::vector<u_signed64>* filelist = m_filehelper->getFileList(sreq,cuuid);
  //std::vector<u_signed64>::iterator fileid;


  /// here we build the Request for the stager 
  while (filename != filelist->end()) {
    castor::stager::SubRequest *subreq = new castor::stager::SubRequest();
    subreq->setFileName((*filename));
    subreq->setProtocol(ptr_server->getProtocol());	// we have to specify a protocol, otherwise our call is not scheduled!
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
  castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 26, 0, NULL);


  /// here, we send the stager request
  try {
    failed = sendStagerRepackRequest(sreq, &req, &reqId, &opts);
  }catch (castor::exception::Exception ex){
    
    for (int i=0; i<req.subRequests().size(); i++)  
      delete req.subRequests().at(i);
    req.subRequests().clear();

    castor::dlf::Param params[] =
    {castor::dlf::Param("Standard Message", sstrerror(ex.code())),
     castor::dlf::Param("Precise Message", ex.getMessage().str()),
     castor::dlf::Param("VID", sreq->vid())
    };
    castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 21, 3, params);
    return;
  }

  /// we want to know how many files were successfully submitted
  sreq->setFilesFailed(failed);


  /// delete the allocated Stager SubRequests
  for (int i=0; i<req.subRequests().size(); i++) delete req.subRequests().at(i);

  /// Setting the SubRequest's Cuuid to the stager one for monitoring
  stage_trace(3," Stagerrequest (now new RepackSubRequest CUUID ): %s",(char*)reqId.c_str());
  castor::dlf::Param param[] = {castor::dlf::Param("New reqID", reqId)};
  castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 33, 1, param);
  cuuid = stringtoCuuid(reqId);
  sreq->setCuuid(reqId);
  sreq->setStatus(SUBREQUEST_STAGING);
 
}


//------------------------------------------------------------------------------
// restartRepack
//------------------------------------------------------------------------------		
void RepackFileStager::restartRepack(RepackSubRequest* sreq){

    _Cuuid_t cuuid = stringtoCuuid(sreq->cuuid());
    struct stage_filequery_resp *responses;
    int nbresps= 0;

    RepackMonitor monitor(ptr_server);
    FileListHelper filelisthelper(ptr_server->getNsName());


    /** fake the original sreq */
    RepackSubRequest* faked = new RepackSubRequest();
    faked->setCuuid(sreq->cuuid());
    faked->setVid(sreq->vid());
    faked->setStatus(sreq->status());
    /** we need the service class info in the original RepackRequest for staging */
    faked->setRequestID(sreq->requestID()); 

    
    /** we only want to restart a repack for the files, which made problems 
        (status in invalid) and are still on the original tape */
    try {
      monitor.getStats(sreq, &responses, &nbresps);
    }catch (castor::exception::Exception ex){
      castor::dlf::Param params[] =
      {castor::dlf::Param("Module", "RepackFileStager" ),
       castor::dlf::Param("Error Message", ex.getMessage().str() ),
       castor::dlf::Param("Responses", nbresps )};
      castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 41, 3, params);
    }
    
    /** if we got an answer for this cuuid, its means that the StageRepackRequest is
        not over. We are not allowed to send a new Request, but to try to finish the
        files in invalid.
    */
    for ( int i=0; i<nbresps; i++ ) {
      if ( responses[i].status == FILE_INVALID_STATUS )
      {
        RepackSegment* seg = new RepackSegment();
        seg->setFileid(responses[i].fileid);
        faked->addSegment(seg);
      }
    }
    free_filequery_resp(responses, nbresps);

    /** next check: if there are no files in failed, staging or migrating, get the
        remaining files on tape, they have to be submitted again */
    if (   !faked->segment().size()    
        && !sreq->filesMigrating()
        && !sreq->filesStaging() ) 
    {
      castor::dlf::dlf_writep(cuuid, DLF_LVL_ALERT, 43, 0, NULL);
      filelisthelper.getFileListSegs(faked);
    }
    else {
      stage_trace(1,"There are still files to be staging/migrating, restart abort!");
      castor::dlf::dlf_writep(cuuid, DLF_LVL_WARNING, 44, 0, NULL);
      sreq->setStatus(SUBREQUEST_STAGING);
      m_dbhelper->updateSubRequest(sreq,false,cuuid);
    }
    
    /** Both checks obove returned no candidates-> fine, we're done */
    if ( !faked->segment().size() )
      sreq->setStatus(SUBREQUEST_DONE);
    else {
      sreq->setFilesMigrating(0);
      stage_files(faked);
      sreq->setCuuid(faked->cuuid());
      sreq->setStatus(SUBREQUEST_STAGING);
      sreq->setFilesFailed(faked->filesFailed());
      sreq->setFilesStaging(faked->filesStaging());
      /** do not remove or update the segment information */ 
      m_dbhelper->updateSubRequest(sreq,false,cuuid);
      castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG, 25, 0, NULL); 
   }

    faked->setRequestID(NULL);  /** set to NULL;just to be sure, it pointed to a foreign one */ 

    freeRepackObj(faked);  /** we know that there is no RepackRequest for 
                              the faked one, so we can delete it directly */
}


//------------------------------------------------------------------------------
// startRepack
//------------------------------------------------------------------------------
void RepackFileStager::startRepack(RepackSubRequest* sreq){
  _Cuuid_t cuuid = stringtoCuuid(sreq->cuuid());
  stage_files(sreq);

  /// doesn't throw exceptions and we want the RepackSubrequest to be updated with 
  /// the segment information

  m_dbhelper->updateSubRequest(sreq,false, cuuid);
  stage_trace(2,"File are sent to stager, repack request updated.");
  castor::dlf::dlf_writep(stringtoCuuid(sreq->cuuid()), DLF_LVL_DEBUG, 25, 0, NULL);

}

//------------------------------------------------------------------------------
// stage_files
//------------------------------------------------------------------------------
int RepackFileStager::sendStagerRepackRequest(  RepackSubRequest* rsreq,
                                                castor::stager::StageRepackRequest* req,
                                                std::string *reqId,
                                                struct stage_options* opts
                                              ) throw (castor::exception::Exception)
{
  int failed = 0; /** counter for failed files during submit */	
  Cuuid_t cuuid = stringtoCuuid(rsreq->cuuid());

	/** Uses a BaseClient to handle the request */
  castor::client::BaseClient client(stage_getClientTimeout());
  client.setOption(NULL);	/** to initialize the RH,stager, etc. */

  castor::stager::RequestHelper reqh(req);
  reqh.setOptions(opts);

  client.setAuthorizationId(rsreq->requestID()->userid(), rsreq->requestID()->groupid());
  
  /** 1. Send the Request */
  std::vector<castor::rh::Response*>respvec;
  castor::client::VectorResponseHandler rh(&respvec);
  *reqId = client.createClientAndSend(req);
  

  /** 2. we need to store the recieved cuuid from the request replier and save it
      in case the pollAnswersFromStager fails */
  rsreq->setCuuid(*reqId);
  rsreq->setStatus(SUBREQUEST_STAGING);
  rsreq->setFilesStaging(req->subRequests().size());
  m_dbhelper->updateSubRequest(rsreq,false,cuuid); 
  

  /** 3. now try to poll for the answers. If there is an timeout it will throw
      an exception */
  client.pollAnswersFromStager(req, &rh);

	/** like the normale api call, we have to check for errors in the answer*/
	for (int i=0; i<respvec.size(); i++) {

      // Casting the response into a FileResponse !
      castor::rh::FileResponse* fr = dynamic_cast<castor::rh::FileResponse*>(respvec[i]);
      if (0 == fr) {
        castor::exception::Internal e;
        e.getMessage() << "Error in dynamic cast, response was NOT a file response.\n"
                       << "Object type is "
                       << castor::ObjectsIdStrings[respvec[i]->type()];
        throw e;
      }
		if ( fr->errorCode() || fr->status() != 6 ){
      struct Cns_fileid fileid;
      fileid.fileid = fr->fileId();
      castor::dlf::Param param[] = 
      {castor::dlf::Param("Filename",fr->castorFileName()),
       castor::dlf::Param("Message", fr->errorMessage()) 
      };
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 38, 1, param, &fileid);
			std::cerr 
					<< fr->castorFileName() << " " << fr->fileId() << " "
					<<"(size "<< fr->fileSize() << ", "
					<<"status "<< fr->status() << ") "
					<<":"<< fr->errorMessage() << "(" << fr->errorCode() << ")"
					<< std::endl;
      failed++;
		}
		
		delete fr;
	}
	respvec.clear();
  return failed;
  
}


		
	} //END NAMESPACE REPACK
}//END NAMESPACE CASTOR
