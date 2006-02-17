#include "RepackCleaner.hpp"


namespace castor{
	namespace repack {
		
//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RepackCleaner::RepackCleaner(){
	m_dbhelper = new DatabaseHelper();
	m_stop = false;
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
RepackCleaner::~RepackCleaner(){
	
}
		
//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------	
void RepackCleaner::run(void* param) throw(){
	
	RepackSubRequest* tape = NULL;
	
	/* Check currently */
	while ( !m_stop ) {
		
		try {
			tape = m_dbhelper->checkSubRequestStatus(SUBREQUEST_STAGING);

			if ( tape != NULL )
			{
				if ( getStageStatus(tape) == castor::stager::SUBREQUEST_FINISHED )
					tape->setStatus(SUBREQUEST_READYFORMIG);
				delete tape;
				tape = NULL;
			}
		}catch (castor::exception::Internal ex){
			std::cerr << ex.getMessage();
			break;
		}
		sleep(10);
	}
}
		
	
//------------------------------------------------------------------------------
// stop
//------------------------------------------------------------------------------
void RepackCleaner::stop() throw(){
	m_stop = true;
}
		
//------------------------------------------------------------------------------
// getStageStatus
//------------------------------------------------------------------------------
int RepackCleaner::getStageStatus(RepackSubRequest* sreq) throw()
{
	struct stage_query_req request;
	struct stage_requestquery_resp* response;
	int nbresps = 0;
	int rc = 0;
			

	_Cuuid_t* cuuid;
	Cuuid2string((char*)sreq->cuuid().c_str(),sreq->cuuid().length(), cuuid);

	request.type = BY_REQID;
	request.param = (void*)sreq->cuuid().c_str();
			
	rc = stage_requestquery(&request,1,&response,&nbresps,NULL);

	if ( rc != 0 ){
		castor::exception::Internal ex;
		ex.getMessage() << sstrerror(serrno);
		throw ex;
	}

	if ( nbresps == 1){
		castor::dlf::Param params[] =
				{castor::dlf::Param("STAGERSTATUS", response[0].status)};
				castor::dlf::dlf_writep((*cuuid), DLF_LVL_DEBUG, 99, 1, params);
		free(response);
		return response[0].status;
	}
	else
		return -1;

}



		
	} //END NAMESPACE REPACK
} //END NAMESPACE CASTOR

