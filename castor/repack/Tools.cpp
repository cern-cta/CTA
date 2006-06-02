


#include "Tools.hpp"

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
        for (int i=0;i<tmp->subRequest().size(); i++){
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
      for (int i=0;i<obj->segment().size(); i++){
        delete obj->segment().at(i);
      }
      obj->segment().clear();
      delete obj;
      obj = NULL;
    }
  }


	}
}
