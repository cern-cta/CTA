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
 * @(#)$RCSfile: FileOrganizer.cpp,v $ $Revision: 1.3 $ $Release$ $Date: 2006/02/02 18:27:12 $ $Author: felixehm $
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
	
	while ( m_run )
	{
		/* Lets see, if good old pal DB has a Job for us :*/
		RepackSubRequest* sreq = m_dbhelper->requestToDo();
		if ( sreq != NULL ){
			castor::dlf::Param params[] =
			{castor::dlf::Param("VID", sreq->vid()),
			 castor::dlf::Param("VID", sreq->id()),
			 castor::dlf::Param("VID", sreq->status())};
			castor::dlf::dlf_writep(nullCuuid, DLF_LVL_SYSTEM, 18, 3, params);
			/* stage the files */
			stage_files(sreq);
			/* we are responsible for this object, so we have to delete it */
			delete sreq;
		}
		else
			std::cerr << "Running, but no Request!" << std::endl;
		
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

	int rc;					// the return code for the stager_prepareToGet call.
	int i,j;
	char path[CA_MAXPATHLEN+1];
	FileListHelper flh;

	/* stage_prepareToGet stuff */
	int nbresps;
    char* reqId = 0;
	struct stage_prepareToGet_fileresp* response;
	/*-------------------------*/

	/* get the filenames as strings */
	std::vector<u_signed64>* filenamelist;
	filenamelist = flh.getFileList(sreq);
	struct stage_prepareToGet_filereq requests [filenamelist->size()] ;

	/* now call the stager */
	for (i = 0; i < filenamelist->size(); i++) {
		Cns_getpath("castorns.cern.ch",filenamelist->at(i),path);	//TODO: remove hardcoded ns name
        requests[i].filename = path;
        requests[i].protocol = "repack";		// for the stager to recognize
        										// that this is a special call

      }
      
        // Before calling the stager, set the status of all the subrequests to in progress
		sreq->setStatus(SUBREQUEST_STAGING);
		m_dbhelper->updateRep(sreq);
		
      //rc = stage_prepareToGet(NULL, requests, filenamelist->size(), &response, &nbresps, &reqId, NULL);
      
      if ( rc != 0 ) {
      	castor::exception::Internal ex;
		ex.getMessage() << "FileOrganizer::stage_files(..):" 
						<< sstrerror(serrno) << std::endl;
		castor::dlf::Param params[] =
		{castor::dlf::Param("Standard Message", sstrerror(ex.code())),
    	 castor::dlf::Param("Precise Message", ex.getMessage().str())};
        castor::dlf::dlf_writep(nullCuuid, DLF_LVL_ERROR, 15, 2, params);
	    throw ex;
	    return;
      }
	  // the Request has now status SUBREQUEST_STAGING
}
		
		
		
		
		
		
	} //END NAMESPACE REPACK
}//END NAMESPACE CASTOR
