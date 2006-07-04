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
 * @(#)$RCSfile: RepackFileStager.cpp,v $ $Revision: 1.9 $ $Release$ $Date: 2006/07/04 13:41:28 $ $Author: felixehm $
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
RepackFileStager::RepackFileStager(RepackServer* srv) throw ()
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
	
		/// Lets see, if good old pal DB has a Job for us */
    RepackSubRequest* sreq = NULL;
    try {
      sreq = m_dbhelper->checkSubRequestStatus(SUBREQUEST_READYFORSTAGING);
    }catch (castor::exception::Exception e){
      castor::dlf::Param params[] = 
      {castor::dlf::Param("Error", e.getMessage().str() )};
      castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 5, 1, params);
      return;
    }
		
		if ( sreq != NULL ){
    //m_dbhelper->lock(sreq);
    castor::dlf::Param params[] =
    {castor::dlf::Param("VID", sreq->vid()),
      castor::dlf::Param("ID", sreq->id()),
      castor::dlf::Param("STATUS", sreq->status())};
    castor::dlf::dlf_writep(stringtoCuuid(sreq->cuuid()), DLF_LVL_SYSTEM, 22, 3, params);

			/// main handling
			try {
				stage_files(sreq);
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
void RepackFileStager::stage_files(RepackSubRequest* sreq) {

	int rc=0;				/// the return code for the stager_prepareToGet call.
	int i,j;
	std::string reqId = "";
	_Cuuid_t cuuid = stringtoCuuid(sreq->cuuid());
	castor::stager::StagePrepareToGetRequest req;
	
	castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 29, 0, NULL);
	/// get the Segs for this tape !
	if ( m_filehelper->getFileListSegs(sreq,cuuid) )
		return;

  // ---------------------------------------------------------------
	// This part has to be removed, if the stager also accepts only fileid
	// as parameter. For now we have to get the paths first.
	std::vector<std::string>* filelist = m_filehelper->getFilePathnames(sreq,cuuid);
	std::vector<std::string>::iterator filename = filelist->begin();
	// ---------------------------------------------------------------
	//std::vector<u_signed64>* filelist = m_filehelper->getFileList(sreq,cuuid);
	//std::vector<u_signed64>::iterator fileid;

  	
  /** check, if we got something back */
  if ( filelist->size() == 0 ){
    castor::dlf::Param params[] =
    {castor::dlf::Param("VID", sreq->vid())};
    castor::dlf::dlf_writep(cuuid, DLF_LVL_WARNING, 36, 1, params);
    delete filelist;
    sreq->setStatus(SUBREQUEST_DONE);
    m_dbhelper->updateSubRequest(sreq,false, cuuid);
    return;
  }


  /* here we build the Request for the stager */
	while (filename != filelist->end()) {
		castor::stager::SubRequest *subreq = new castor::stager::SubRequest();
		subreq->setFileName((*filename));
		subreq->setRepackVid(sreq->vid());	// this marks the Request as a 'special' one.
		subreq->setProtocol(ptr_server->getProtocol());	// we have to specify a protocol, otherwise our call is not scheduled!
		subreq->setRequest(&req);
		req.addSubRequests(subreq);
		filename++;
	}

  /// We need to set the stage options. 
  struct stage_options opts;
  opts.stage_host = (char*)ptr_server->getStagerName().c_str(); 
  
  /** is a special service class given (for writing to tape(pools)) */
  if ( sreq->requestID()->serviceclass().length() )
    opts.service_class = (char*)sreq->requestID()->serviceclass().c_str();
  /** otherwise we take the one specified in the castor config file */
  else     
    opts.service_class = (char*)ptr_server->getServiceClass().c_str();


	castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 26, 0, NULL);

	try {
      sendStagerRepackRequest(&req, &reqId, &opts);

	}catch (castor::exception::Exception ex){
    for (int i=0; i<req.subRequests().size(); i++)
      delete req.subRequests().at(i);
    req.subRequests().clear();
		
    castor::dlf::Param params[] =
		{castor::dlf::Param("Standard Message", sstrerror(ex.code())),
		 castor::dlf::Param("Precise Message", ex.getMessage().str())};
		castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 21, 2, params);
    return;
	}
	
	/** Setting the SubRequest's Cuuid to the stager one for better follow */
	stage_trace(3," Stagerrequest (now new RepackSubRequest CUUID ): %s",(char*)reqId.c_str());
	castor::dlf::Param param[] = {castor::dlf::Param("New reqID", reqId)};
	castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 33, 1, param);
	cuuid = stringtoCuuid(reqId);
	sreq->setCuuid(reqId);

	sreq->setFilesStaging(filelist->size());
  sreq->setFiles(filelist->size());
  sreq->setStatus(SUBREQUEST_STAGING);
  
  filelist->clear();
  delete filelist;
	
  /// doesn't throw exceptions
  m_dbhelper->updateSubRequest(sreq,true, cuuid);

  stage_trace(2,"File are sent to stager, repack request updated.");
  castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG, 25, 0, NULL);

	// the Request has now status SUBREQUEST_STAGING
}
		



//------------------------------------------------------------------------------
// stage_files
//------------------------------------------------------------------------------
void RepackFileStager::sendStagerRepackRequest(
                                                castor::stager::StagePrepareToGetRequest* req,
                                                std::string *reqId,
                                                struct stage_options* opts
                                              ) throw (castor::exception::Internal)
{
	

	/** Uses a BaseClient to handle the request */
	castor::client::BaseClient client(stage_getClientTimeout());
	client.setOption(NULL);	/** to initialize the RH,stager, etc. */
	
	castor::stager::RequestHelper reqh(req);
	reqh.setOptions(opts);

	/** Send the Request */
	std::vector<castor::rh::Response*>respvec;
	castor::client::VectorResponseHandler rh(&respvec);
	
	
	*reqId = client.sendRequest(req, &rh);

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
			/** TODO: DLF logging for errors on files*/
			std::cerr 
					<< fr->castorFileName() << " " << fr->fileId() << " "
					<<"(size "<< fr->fileSize() << ", "
					<<"status "<< fr->status() << ") "
					<<":"<< fr->errorMessage() << "(" << fr->errorCode() << ")"
					<< std::endl;
		}
		
		delete fr;
	}
	respvec.clear();
}

		
	} //END NAMESPACE REPACK
}//END NAMESPACE CASTOR
