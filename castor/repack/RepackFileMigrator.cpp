#include "RepackFileMigrator.hpp"


namespace castor{
	namespace repack {
		
//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RepackFileMigrator::RepackFileMigrator(){
	m_dbhelper = new DatabaseHelper();
	m_stop = false;
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
RepackFileMigrator::~RepackFileMigrator(){
	delete m_dbhelper;
}
		
//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------	
void RepackFileMigrator::run(void* param) throw(){
	
	int rc, nbresps;
	nbresps = rc = errno = serrno = 0;
	Cuuid_t cuuid;
	struct stage_query_req request;
	struct stage_filequery_resp *response;
	
	RepackSubRequest* sreq = m_dbhelper->checkSubRequestStatus(SUBREQUEST_READYFORMIG);
	
	if ( sreq != NULL ) {
		stage_trace(3,"New Job! %s , %s ",sreq->vid().c_str(), sreq->cuuid().c_str());
	
		request.type = BY_REQID_GETNEXT;
		request.param = (void*)sreq->cuuid().c_str();
	
		stage_trace(3,"Getting NextFiles for Request: %s ",(char*)sreq->cuuid().c_str());
		rc = stage_filequery(&request,1,&response,&nbresps,NULL);
	
		if ( rc != 0 ){
			/* we don't throw exceptions here - just a message */
			stage_trace(3,sstrerror(serrno));
		}
		
		/* handle and delete the recieved objects */
		if (nbresps){
			handle(response,nbresps,sreq);
			free(response);
		}
		delete sreq;
	}

}
		
	
//------------------------------------------------------------------------------
// stop
//------------------------------------------------------------------------------
void RepackFileMigrator::stop() throw(){
	m_stop = true;
}

//------------------------------------------------------------------------------
// handle 
//------------------------------------------------------------------------------
void RepackFileMigrator::handle(struct stage_filequery_resp* files_to_mig, int count_files, RepackSubRequest* sreq){
	int i, nbresps; i = nbresps = 0;
	char **requestId;
	struct stage_filereq request [count_files];
	struct stage_fileresp *responses;
	struct stage_options;	/* not used, but in case good to know that it is there !*/


	castor::dlf::Param params[] =
		{castor::dlf::Param("VID", sreq->vid()),
		 castor::dlf::Param("Number of files", count_files)};
	castor::dlf::dlf_writep(stringtoCuuid(sreq->cuuid()), DLF_LVL_DEBUG, 99, 2, params);

	/* Create new struct requests for putdone */
	for (i=0;i < count_files; i++)
	{
		std::cout << files_to_mig[i].fileid << "(" << files_to_mig[i].castorfilename << ") " << files_to_mig[i].status << std::endl;
		request[i].filename = strdup(files_to_mig[i].castorfilename);
	}	
	
	/* now send the putDone Request */
	if ( stage_putDone ((char*)sreq->cuuid().c_str(), request, count_files, &responses, &nbresps, requestId, NULL) < 0 ){
		stage_trace(3,sstrerror(serrno));
	}
		/* analyse the response and deallocate it after use.*/
	while (--nbresps > -1 ){
		if ( responses[nbresps].errorCode > 0 ){
			castor::dlf::Param params[] =
				{castor::dlf::Param("VID", sreq->vid()),
				castor::dlf::Param("ErrorCode", responses[nbresps].errorCode),
				castor::dlf::Param("ErrorMsg", responses[nbresps].errorMessage),
				castor::dlf::Param("Method", "RepackFileMigrator::handle"),
				castor::dlf::Param("Line", 102)};
			castor::dlf::dlf_writep(stringtoCuuid(sreq->cuuid()), DLF_LVL_WARNING, 32, 5, params);
			free (responses[nbresps].errorMessage);
		}
		free (responses[nbresps].filename);
	}
	free (requestId);
	
	/**
		The staged files are now marked as CANBEMIGRATED
	*/
}


		
	} //END NAMESPACE REPACK
} //END NAMESPACE CASTOR

