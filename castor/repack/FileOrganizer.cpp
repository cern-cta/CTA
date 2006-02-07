/******************************************************************************
 *                      RepackServerReqSvcThread.cpp
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
 * @(#)$RCSfile: FileOrganizer.cpp,v $ $Revision: 1.5 $ $Release$ $Date: 2006/02/07 20:02:53 $ $Author: felixehm $
 *
 *
 *
 * @author Felix Ehm
 *****************************************************************************/
 
#include "FileOrganizer.hpp"


 
namespace castor {
	namespace repack {
		
	const char* REPACK_POLL = "REPACK_POLL";
		
		
//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
FileOrganizer::FileOrganizer() throw()
{
	m_dbhelper = new DatabaseHelper();
	m_run = true;
	
}


//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
FileOrganizer::~FileOrganizer() throw() {
	delete m_dbhelper;
}



//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------
void FileOrganizer::run(void *param) throw() {
	
	/* this is just to see, if there is a enviroment variable for polling time
	 */
	char* ptime;
	unsigned long polling_time;
	if ( (ptime=getenv(REPACK_POLL))!=0 ){
		char* error = ptime;
		polling_time = strtol(ptime, &error,0);
		if ( *error != 0 ){
			castor::exception::Internal ex;
			ex.getMessage()<< "FileOrganizer: run (..): Fatal Error! " 
					<< std::endl
					<< "The passed polling time in the enviroment varible is "
					<< "not vaild ! " << std::endl;
			throw ex;
		}
	}else
		polling_time = 10;
	
	/* now, here we go */
	while ( m_run )
	{
		/* Lets see, if good old pal DB has a Job for us */
		RepackSubRequest* sreq = m_dbhelper->requestToDo();
		
		if ( sreq != NULL ){
			stage_trace(3,"New Job! %s",sreq->vid().c_str());
			castor::dlf::Param params[] =
			{castor::dlf::Param("VID", sreq->vid()),
			 castor::dlf::Param("ID", sreq->id()),
			 castor::dlf::Param("STATUS", sreq->status())};
			castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 22, 3, params);
			/* stage the files */
			stage_files(sreq);
			/* we are responsible for this object, so we have to delete it */
			delete sreq;
		}
		else
			std::cerr << "Running, but no Job!" << std::endl;
		
		sleep(polling_time);
	}
}


//------------------------------------------------------------------------------
// stop
//------------------------------------------------------------------------------
void FileOrganizer::stop() throw() {
	m_run = false;
}


//------------------------------------------------------------------------------
// sortReadOrder
//------------------------------------------------------------------------------
//TODO: check, if file is completly on this tape should be done by NS Database! - much quicker !
void FileOrganizer::sortReadOrder(std::vector<u_signed64>* fileidlist) throw()
{

}




//------------------------------------------------------------------------------
// stage_files
//------------------------------------------------------------------------------
void FileOrganizer::stage_files(RepackSubRequest* sreq) throw() {

	int rc=0;					// the return code for the stager_prepareToGet call.
	int i,j;
	char path[CA_MAXPATHLEN+1];
	FileListHelper flh;

	std::vector<u_signed64>* filenamelist;	// the returned filelist with ids

	/* stage_prepareToGet stuff */
	int nbresps;
    char* reqId = 0;
	struct stage_prepareToGet_fileresp* response;
	/*-------------------------*/

	stage_trace(3,"Updating SubRequest with its segs");
	
	flh.getFileListSegs(sreq);				/* get the Segs for this tape !*/

	/* 
	 * TODO:We have to check for space on the DiscCache !
	u_signed64 free = stager_free();
	if ( sreq->xsize() > free ){
		stage_trace(3,"FileOrganizer::stage_files(..): Not enough space left for this job! Skipping..");
		castor::dlf::Param params[] =
		{castor::dlf::Param("Requested space", sreq->xsize()),
		 castor::dlf::Param("Available space", free)}
		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 21, 2, params);
		return;
	}
	*/

	filenamelist = flh.getFileList(sreq);	/* and now all parent_fileids */
	struct stage_prepareToGet_filereq requests [filenamelist->size()] ;

	stage_trace(3,"Now, get the paths.");

	/* now call the stager */
	for (i=0; i < filenamelist->size();i++) {
		Cns_getpath("castorns.cern.ch",filenamelist->at(i),path);	//TODO: remove hardcoded ns name
		//stage_trace(3,"%s", path);
		requests[i].filename = path;
		requests[i].protocol = "repack";		// for the stager to recognize
											// that this is a special call
	}
      
        
	// Before calling the stager, set the status of all the subrequests to in progress
	stage_trace(3,"Updating Request to STAGING and add its segs");
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 25, 0, NULL);
	sreq->setStatus(SUBREQUEST_STAGING);
	m_dbhelper->updateSubRequest(sreq);
	
	stage_trace(3,"Now staging");
	castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 26, 0, NULL);
	//rc = stage_prepareToGet(NULL, requests, filenamelist->size(), &response, &nbresps, &reqId, NULL);

      
	if ( rc != 0 ) {
		castor::exception::Internal ex;
		ex.getMessage() << "FileOrganizer::stage_files(..):" 
						<< sstrerror(serrno) << std::endl;
		castor::dlf::Param params[] =
		{castor::dlf::Param("Standard Message", sstrerror(ex.code())),
		 castor::dlf::Param("Precise Message", ex.getMessage().str())};
		castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 21, 2, params);
		throw ex;
	}
    filenamelist->clear();
	delete filenamelist;
    //TODO: if staging this has to be uncommented !    delete response;
      
	// the Request has now status SUBREQUEST_STAGING
}
		
		
		
		
		
		
	} //END NAMESPACE REPACK
}//END NAMESPACE CASTOR
