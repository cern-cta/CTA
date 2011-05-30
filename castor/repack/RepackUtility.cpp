
/******************************************************************************
 *                       RepackUtility.hpp
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
 *
 *
 *
 * @author Giulia Taurelli
 *****************************************************************************/



#include "RepackUtility.hpp"
#include "RepackResponse.hpp"
#include "RepackAck.hpp"
#include "RepackSegment.hpp"
#include "RepackSubRequest.hpp"
#include "RepackRequest.hpp"
#include "castor/exception/Internal.hpp"

#include "castor/Constants.hpp"

#define DEFAULT_STAGER_PORT 9002

#include <vector>
#include <stdlib.h>



namespace castor {
	namespace repack {


  // forward declaration
  void freeRepackSubRequest(castor::repack::RepackSubRequest* obj);
  void freeRepackResponse(castor::repack::RepackResponse* obj);

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
  void freeRepackObj(castor::IObject* obj) {

    if ( obj != NULL ) {
      if (obj->type() == OBJ_RepackRequest  ){
	RepackRequest* tmp = dynamic_cast<RepackRequest*>(obj);
	if (tmp==NULL) return;
	std::vector<RepackSubRequest*>::iterator sub =tmp->repacksubrequest().begin();
	while (sub != tmp->repacksubrequest().end()){
	  freeRepackSubRequest(*sub);
	  sub++;
	}
	tmp->repacksubrequest().clear();       
        delete obj;
	obj=NULL;       
	return;
      }

      if ( obj->type() ==  OBJ_RepackSubRequest) {
	RepackSubRequest* tmp = dynamic_cast<RepackSubRequest*>(obj);
	if (tmp==NULL) return;
        freeRepackSubRequest(tmp);
	obj=NULL;
	return;
      }
	
      if (obj->type() == OBJ_RepackSegment){
	RepackSegment* tmp = dynamic_cast<RepackSegment*>(obj);
	if (tmp==NULL) return;
	tmp->setRepacksubrequest(NULL);
	delete tmp;
	obj=NULL;
	return;
      }

      if (obj->type() ==  OBJ_RepackAck){
	RepackAck* tmp = dynamic_cast<RepackAck*>(obj);
	if (tmp==NULL) return;
	std::vector<RepackResponse*>::iterator sub = tmp->repackresponse().begin();
	while (sub != tmp->repackresponse().end()){
	  freeRepackObj(*sub);
	  sub++;
	}
	tmp->repackresponse().clear();
	
	delete obj;
	obj=NULL;
	return;
      }

      if (obj->type() == OBJ_RepackResponse){
	RepackResponse* tmp = dynamic_cast<RepackResponse*>(obj); 
        if (tmp==NULL) return;
	freeRepackSubRequest(tmp->repacksubrequest());
	delete tmp;
	tmp=NULL;
	return;
      }

    }

}



//------------------------------------------------------------------------------
// Helper for cleaning a RepackSubRequest
//------------------------------------------------------------------------------  
  void freeRepackSubRequest(castor::repack::RepackSubRequest* obj)
  {
    if ( obj != NULL ) {
      for (unsigned int i=0;i<obj->repacksegment().size(); i++){
        freeRepackObj(obj->repacksegment().at(i));
      }
      obj->repacksegment().clear();
      obj->setRepackrequest(0);
      delete obj;
      obj = NULL;
    }
  }


//------------------------------------------------------------------------------
// Helper for setting the Stager Options 
//------------------------------------------------------------------------------ 
  void getStageOpts(struct stage_options* opts, RepackSubRequest* sreq) 
                                              throw (castor::exception::Exception)
  {
    /// first check the output stage_options
    if ( opts == NULL ){
      castor::exception::Internal ex;
      ex.getMessage() << "Passed stager option struct is NULL!" << std::endl;
      throw ex;
    }
    
    /// retrieve the information from RepackSubRequest
    if ( sreq->repackrequest() != NULL ) {
      opts->service_class = (char*)sreq->repackrequest()->svcclass().c_str();
      opts->stage_host = (char*)sreq->repackrequest()->stager().c_str();
      opts->stage_port = DEFAULT_STAGER_PORT;
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

