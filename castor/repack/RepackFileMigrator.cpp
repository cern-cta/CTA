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
	nbresps = rc = 0;
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
			handle(response,nbresps);
			delete [] response;
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
void RepackFileMigrator::handle(struct stage_filequery_resp* response, int nbresps){
	int i=0;
	for (i=0;i < nbresps; i++)
	{
		std::cout << response[i].castorfilename << " " << response[i].status << std::endl;
	}

}


		
	} //END NAMESPACE REPACK
} //END NAMESPACE CASTOR

