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
 * @(#)$RCSfile: RepackFileStager.cpp,v $ $Revision: 1.1 $ $Release$ $Date: 2006/03/03 17:14:03 $ $Author: felixehm $
 *
 *
 *
 * @author Felix Ehm
 *****************************************************************************/
 
#include "RepackFileStager.hpp"


 
namespace castor {
	namespace repack {
		
	const char* REPACK_POLL = "REPACK_POLL";
	const char* REPACK_SVC_CLASS = "repack";
	const char* CNS_CAT = "CNS";
	const char* CNS_HOST = "HOST";
		
//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RepackFileStager::RepackFileStager() throw ()
{
	char *tmp = (char*) malloc(CA_MAXHOSTNAMELEN*sizeof(char));
	
	if ( !(tmp = getconfent((char*)CNS_CAT, (char*)CNS_HOST,0)) ){
		castor::exception::Internal ex;
		ex.getMessage() << "Unable to initialise FileListHelper with nameserver "
		<< "entry in castor config file";
		throw ex;	
	}
	m_ns = new std::string(tmp);

	m_dbhelper = new DatabaseHelper();	
	m_filehelper = new FileListHelper((*m_ns));
	m_run = true;
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
RepackFileStager::~RepackFileStager() throw() {
	delete m_dbhelper;
	delete m_filehelper;
	delete m_ns;
}



//------------------------------------------------------------------------------
// Helper for shit Cuuid structs
//------------------------------------------------------------------------------
std::string Cuuidtostring(_Cuuid_t *cuuid){
		char buf[CUUID_STRING_LEN+1];
	    Cuuid2string(buf, CUUID_STRING_LEN+1, cuuid);
	    std::string tmp (buf,CUUID_STRING_LEN);
	    return tmp;
}
_Cuuid_t stringtoCuuid(std::string strcuuid){
	_Cuuid_t cuuid;
	string2Cuuid(&cuuid, (char*)strcuuid.c_str());
	return cuuid;
}
//------------------------------------------------------------------------------




//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void RepackFileStager::run(void *param) throw() {
	
		/* Lets see, if good old pal DB has a Job for us */
		RepackSubRequest* sreq = m_dbhelper->requestToDo();
		
		if ( sreq != NULL ){
	    	
			stage_trace(3,"New Job! %s , %s ",sreq->vid().c_str(), sreq->cuuid().c_str());
			castor::dlf::Param params[] =
			{castor::dlf::Param("VID", sreq->vid()),
			 castor::dlf::Param("ID", sreq->id()),
			 castor::dlf::Param("STATUS", sreq->status())};
			castor::dlf::dlf_writep(stringtoCuuid(sreq->cuuid()), DLF_LVL_SYSTEM, 22, 3, params);

			/* main decision for further handling */
			try {
				stage_files(sreq);
			/* stage the files, and we forget errors, the logs are written anyway */
			}
			catch (castor::exception::Internal e){
				delete sreq;
			}
			catch (castor::exception::Exception ex){
				delete sreq;
			}
			delete sreq;
		}
}


//------------------------------------------------------------------------------
// stop
//------------------------------------------------------------------------------
void RepackFileStager::stop() throw() {
	m_run = false;
}


//------------------------------------------------------------------------------
// sortReadOrder
//------------------------------------------------------------------------------
//TODO: check, if file is completly on this tape should be done by NS Database! - much quicker !
void RepackFileStager::sortReadOrder(std::vector<u_signed64>* fileidlist) throw()
{

}




//------------------------------------------------------------------------------
// stage_files
//------------------------------------------------------------------------------
void RepackFileStager::stage_files(RepackSubRequest* sreq) throw() {

	int rc=0;				// the return code for the stager_prepareToGet call.
	int i,j;
	_Cuuid_t cuuid = stringtoCuuid(sreq->cuuid());
	

	std::vector<u_signed64>* filelist;	// the returned filelist with ids

	/* stage_prepareToGet stuff */
	int nbresps = 0;
	char* reqId = NULL;
	struct stage_prepareToGet_fileresp* response = NULL;
	struct stage_options opts;
	opts.stage_host = NULL;	
	opts.service_class = (char*)REPACK_SVC_CLASS;
	/*-------------------------*/

	stage_trace(3,"Updating SubRequest with its segs");
	castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 29, 0, NULL);
	/* get the Segs for this tape !*/

	if ( m_filehelper->getFileListSegs(sreq,cuuid) )
		return;

	/* 
	 * TODO:We have to check for space on the DiscCache !
	u_signed64 free = stager_free();
	if ( sreq->xsize() > free ){
		stage_trace(3,"RepackFileStager::stage_files(..): Not enough space left for this job! Skipping..");
		castor::dlf::Param params[] =
		{castor::dlf::Param("Requested space", sreq->xsize()),
		 castor::dlf::Param("Available space", free)}
		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 21, 2, params);
		return;
	}
	*/

	filelist = m_filehelper->getFileList(sreq,cuuid);	/* and now all parent_fileids */
	struct stage_prepareToGet_filereq requests [filelist->size()] ;
	stage_trace(3,"Now, get the paths.");

//////////// THIS PART IS TO BE REMOVED INTO THE REPACK STAGER SERVICE \\\\\\\\\
	/* now call the stager */
	for (i=0; i < filelist->size();i++) {
		char path[CA_MAXPATHLEN+1];
		Cns_getpath((char*)m_ns->c_str(),filelist->at(i),path);
		requests[i].filename = (char*)malloc(sizeof(path)*sizeof(char));
		strcpy(requests[i].filename,path);
		requests[i].protocol = "rfio";	// TODO: now changing the file status in the DB
	}

	stage_trace(3,"Now staging");
	castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 26, 0, NULL);
	rc = stage_prepareToGet(NULL, requests, filelist->size(), &response, &nbresps, &reqId, &opts);
	
	/* Response Errors */
	for (i=0; i<nbresps; i++){
		if ( response[i].errorCode ) {
		std::cerr 
			<< response[i].filename << " " 
			<<"(size "<< response[i].filesize << ", "
			<<"status "<< response[i].status << ") "
			<<":"<< response[i].errorMessage << "(" << response[i].errorCode << ")"
			<< std::endl;
		}
	}

	/* delete all allocated filenames */
	for (i=0; i<filelist->size(); i++){
		free (requests[i].filename);
	}
/////////////////////////////                    \\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\


	/* delete the response */
	if ( response ) delete [] response;
     
    /* Check the return code */
	if ( rc != 0 ) {
		castor::exception::Internal ex;
		ex.getMessage() << "RepackFileStager::stage_files(..):" 
						<< sstrerror(serrno) << std::endl;
		castor::dlf::Param params[] =
		{castor::dlf::Param("Standard Message", sstrerror(ex.code())),
		 castor::dlf::Param("Precise Message", ex.getMessage().str())};
		castor::dlf::dlf_writep(cuuid, DLF_LVL_ERROR, 21, 2, params);
		throw ex;
	}
	
	/* Setting the SubRequest's Cuuid to the stager one for better follow */
	stage_trace(3," Stagerrequest (now new RepackSubRequest CUUID ): %s",reqId);
	castor::dlf::Param param[] = {castor::dlf::Param("New ReqID", reqId)};
	castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 32, 1, param);
	cuuid = stringtoCuuid(reqId);
	sreq->setCuuid(reqId);	

	stage_trace(3,"Updating Request to STAGING and add its segs");
	castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 25, 0, NULL);
	sreq->setStatus(SUBREQUEST_STAGING);
	m_dbhelper->updateSubRequest(sreq,cuuid);
	
	filelist->clear();
	delete filelist;
      
	// the Request has now status SUBREQUEST_STAGING
}
		


//------------------------------------------------------------------------------
// createSvcClass, for repacking to different tapepool than origin
//------------------------------------------------------------------------------	
/*
void RepackFileStager::createSvcClass(RepackSubRequest *rsub)
{
	// 1. enter new SvcClass into DB 
	
	std::ofstream myfile;
	struct vmgr_tape_info tape_info;

	if (vmgr_querytape (vid, 0, &tape_info, NULL) < 0) {
		// The check was done before !
		return;
	}
	
	myfile.open ("migratorPolicy.pl");
	myfile << "";
	myfile.close();
}
		
*/	
		
	} //END NAMESPACE REPACK
}//END NAMESPACE CASTOR
