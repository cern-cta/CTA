/*************************************************************************************/
/* StagerGetHandler: Constructor and implementation of the get subrequest's handler */
/***********************************************************************************/


#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"

#include "castor/stager/dbService/StagerRequestHandler.hpp"
#include "castor/stager/dbService/StagerJobRequestHandler.hpp"
#include "castor/stager/dbService/StagerGetHandler.hpp"

#include "stager_uuid.h"
#include "stager_constants.h"

#include "Cns_api.h"
#include "expert_api.h"

#include "Cpwd.h"
#include "Cgrp.h"
#include "osdep.h"

#include "castor/IClientFactory.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"
#include "castor/exception/Exception.hpp"

#include "serrno.h"
#include <errno.h>

#include <iostream>
#include <string>


namespace castor{
  namespace stager{
    namespace dbService{
      
      StagerGetHandler::StagerGetHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper) throw (castor::exception::Exception)
      {

	this->stgRequestHelper = stgRequestHelper;
	this->stgCnsHelper = stgCnsHelper;
	this->typeRequest = OBJ_StageGetRequest;

	this->maxReplicaNb = this->stgRequestHelper->svcClass->maxReplicaNb();	
	this->replicationPolicy = this->stgRequestHelper->svcClass->replicationPolicy();
	
	/* get the request's size required on disk */
	/* depending if the file exist, we ll need to update this variable */
	this->xsize = this->stgRequestHelper->subrequest->xsize();
	
	if( xsize > 0 ){
	  if(xsize < (stgCnsHelper->cnsFilestat.filesize)){
	    /* warning, user is requesting less bytes than the real size */
	    /* just print a message. we don't update xsize!!! */
	  }
	  	  
	}else{
	  this->xsize = stgCnsHelper->cnsFilestat.filesize;
	}


	this->default_protocol = "rfio";
	
	this->currentSubrequestStatus = stgRequestHelper->subrequest->status();

      }
      



      /****************************************************************************************/
      /* handler for the get subrequest method */
      /****************************************************************************************/
      void StagerGetHandler::handle() throw(castor::exception::Exception)
      {
	try{
	  
	  /**************************************************************************/
	  /* common part for all the handlers: get objects, link, check/create file*/
	  preHandle();
	  /**********/

	  jobOriented();
	  
	  /* for Get, Update                         */
	  /*     switch(getDiskCopyForJob):         */                                     
	  /*        case 0,1: (staged) jobManager  */
	  /*        case 2: (waitRecall) createTapeCopyForRecall */
	  /* to be overwriten in Repack, PrepareToGetHandler, PrepareToUpdateHandler  */
	  switchDiskCopiesForJob(); 

	 

	}catch(castor::exception::Exception e){

	  /* since if an error happens we are gonna reply to the client(and internally, update subreq on DB)*/
	  /* we don t execute: dbService->updateRep ..*/

	  castor::exception::Exception ex(e.code());
	  ex.getMessage()<<"(StagerGetHandler) Error"<<e.getMessage().str()<<std::endl;
	  throw ex;
	}  
      }

      StagerGetHandler::~StagerGetHandler()throw(){
	
      }

    }//end namespace dbService
  }//end namespace stager
}//end namespace castor
