


#include "Tools.hpp"

namespace castor {
	namespace repack {
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
    //free (resp);
  }


	}
}
