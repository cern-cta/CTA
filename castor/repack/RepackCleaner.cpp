#include "RepackCleaner.hpp"


namespace castor{
	namespace repack {
		
//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
RepackCleaner::RepackCleaner(RepackServer* svr){
  m_dbhelper = new DatabaseHelper();
  ptr_server = svr;
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
    tape = m_dbhelper->checkSubRequestStatus(SUBREQUEST_READYFORCLEANUP);

    if ( tape != NULL )	{
    
      cuuid = stringtoCuuid(tape->cuuid());
      if ( cleanupTape(tape) ){
        castor::dlf::Param params[] =
        {castor::dlf::Param("VID", tape->vid())};
        castor::dlf::dlf_writep(cuuid, DLF_LVL_SYSTEM, 35, 1, params);
        tape->setStatus(SUBREQUEST_DONE);
	m_dbhelper->updateSubRequest(tape,false,cuuid);
      }
      cuuid = nullCuuid;
      delete tape;
      tape = NULL;
    }

  }catch (castor::exception::Internal ex){
    /** we know that a message was written to dlf, so we forget the exeption and continue
     */
  }
}
		
	
//------------------------------------------------------------------------------
// stop
//------------------------------------------------------------------------------
void RepackCleaner::stop() throw(){
	
}
		
//------------------------------------------------------------------------------
// getStageStatus
//------------------------------------------------------------------------------
int RepackCleaner::cleanupTape(RepackSubRequest* sreq) throw(castor::exception::Internal)
{ 
  FileListHelper flp;
  Cuuid_t cuuid = stringtoCuuid(sreq->cuuid());
  std::vector<std::string>* filelist = flp.getFilePathnames(sreq,cuuid);
  int rc, nbresps;
  rc = nbresps = 0;
  
  /** for stager request */
  struct stage_filereq requests[filelist->size()];
  struct stage_fileresp *responses;
  struct stage_options opts;

  /** we query by request, easier for stager to fill */
  opts.stage_host = (char*)ptr_server->getStagerName().c_str();
  opts.service_class = (char*)ptr_server->getServiceClass().c_str();
  
  if ( !filelist->size() ) {
    /* this means that there are no files for a repack tape in SUBREQUEST_READYFORCLEANUP
     * => ERROR, this must not happen!
     * but we leave it to the Monitor Service to solve that problem
     */
	  castor::exception::Internal ex;
	  ex.getMessage() << "No files found for cleanup phase for " << sreq->vid() 
		          << std::endl;
	  throw ex;
  }
  
  for ( int i=0; i< filelist->size(); i++){
    requests[i].filename =  (char*)filelist->at(i).c_str();
  }
  
  rc = stage_rm( requests,filelist->size(),&responses, &nbresps, NULL ,&opts); 
  if ( rc == 0 ) {
  //m_dbhelper->remove(sreq);
  
    for ( int i=0; i< nbresps; i++){
      free( responses[i].filename );
      if (  responses[i].errorCode ) free ( responses[i].errorMessage);
    }
    free (responses);
  }
  return true;
}



		
	} //END NAMESPACE REPACK
} //END NAMESPACE CASTOR

