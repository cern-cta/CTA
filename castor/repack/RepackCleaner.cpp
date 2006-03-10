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
	delete m_dbhelper;
}
		
//------------------------------------------------------------------------------
// run
//------------------------------------------------------------------------------	
void RepackCleaner::run(void* param) throw(){
	
	RepackSubRequest* tape = NULL;
	Cuuid_t cuuid;
	
	try {

		tape = m_dbhelper->checkSubRequestStatus(SUBREQUEST_MIGRATING);

		if ( tape != NULL )	{
			if ( getStageStatus(tape) == castor::stager::SUBREQUEST_FINISHED ){
				string2Cuuid(&cuuid,(char*)tape->cuuid().c_str());
				tape->setStatus(SUBREQUEST_READYFORMIG);
				castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG, 40, 1, NULL);
				m_dbhelper->updateSubRequest(tape,cuuid);
			}
			cuuid = nullCuuid;
			delete tape;
			tape = NULL;
		}

	}catch (castor::exception::Internal ex){
		std::cerr << ex.getMessage();
		//break;
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
	

}



		
	} //END NAMESPACE REPACK
} //END NAMESPACE CASTOR

