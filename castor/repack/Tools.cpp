#include "Tools.hpp"

#include <vector>



namespace castor {
	namespace repack {


  // forward declaration
  void freeRepackSubRequest(castor::repack::RepackSubRequest* obj);
//------------------------------------------------------------------------------
// Helper for shit Cuuid C structs
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
  //-----------------------------------------------------------------------------
  

//------------------------------------------------------------------------------
// Helper for cleaning the stager file responses
//------------------------------------------------------------------------------  
  void free_stager_response(struct  stage_filequery_resp* resp){
    free( resp->filename);
    free( resp->castorfilename);
    free( resp->diskserver);
    free( resp->poolname);
    if ( resp->errorCode) free( resp->errorMessage);
  }

//------------------------------------------------------------------------------
// Helper for cleaning the repack objects
//------------------------------------------------------------------------------  
  void freeRepackObj(castor::IObject* obj)
  {
    if ( obj != NULL ) {
      if ( obj->type() == OBJ_RepackRequest)
      {
        RepackRequest* tmp = dynamic_cast<RepackRequest*>(obj);
        for (unsigned int i=0;i<tmp->subRequest().size(); i++){
          freeRepackSubRequest( tmp->subRequest().at(i) );
        }
        tmp->subRequest().clear();
        delete tmp;
      }
      else if ( obj->type() == OBJ_RepackSubRequest) {
        RepackSubRequest* tmp = dynamic_cast<RepackSubRequest*>(obj);
        freeRepackSubRequest( tmp );
      }
      else
        return;
    }
  }

//------------------------------------------------------------------------------
// Helper for cleaning a RepackSubRequest
//------------------------------------------------------------------------------  
  void freeRepackSubRequest(castor::repack::RepackSubRequest* obj)
  {
    if ( obj != NULL ) {
      for (unsigned int i=0;i<obj->segment().size(); i++){
        delete obj->segment().at(i);
      }
      obj->segment().clear();
      delete obj;
      obj = NULL;
    }
  }
 
//------------------------------------------------------------------------------
// Helper for setting the Stager Options 
//------------------------------------------------------------------------------ 
  void getStageOpts(struct stage_options* opts, RepackSubRequest* sreq) 
                                              throw (castor::exception::Internal)
  {
    /// first check the output stage_options
    if ( opts == NULL ){
      castor::exception::Internal ex;
      ex.getMessage() << "Passed stager option struct is NULL!" << std::endl;
      throw ex;
    }
    
    /// retrieve the information from RepackSubRequest
    if ( sreq->requestID() != NULL ) {
      opts->service_class = (char*)sreq->requestID()->serviceclass().c_str();
      opts->stage_host = (char*)sreq->requestID()->stager().c_str();
      opts->stage_port = opts->stage_port = DEFAULT_STAGER_PORT;
      opts->stage_version = 0;
    }
    else {
      castor::exception::Internal ex;
      ex.getMessage() << "Can't get service class from request " << std::endl
                      << "(corresponding RepackRequest not available)";
      throw ex;
    }

  }

 }
}

