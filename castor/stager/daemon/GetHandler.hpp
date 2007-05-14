
/*************************************************************************************/
/* StagerGetHandler: Constructor and implementation of the get subrequest's handler */
/***********************************************************************************/

#ifndef STAGER_GET_HANDLER_HPP
#define STAGER_GET_HANDLER_HPP 1




#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"

#include "castor/stager/dbService/StagerRequestHandler.hpp"
#include "castor/stager/dbService/StagerJobRequestHandler.hpp"


#include "stager_uuid.h"
#include "stager_constants.h"
#include "serno.h"
#include "Cns_api.h"
#include "expert_api.h"
#include "rm_api.h"
#include "Cpwd.h"
#include "Cgrp.h"
#include "castor/IClientFactory.h"
#include "castor/stager/SubRequestStatusCodes.hpp"

#include <iostream>
#include <string>



namespace castor{
  namespace stager{
    namespace dbService{

      class StagerRequestHelper;
      class StagerCnsHelper;

      class StagerGetHandler :public StagerJobRequestHandler{

	
      public:
	/* constructor */
	StagerGetHandler::StagerGetHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper, std::string message) throw();
	/* destructor */
	StagerGetHandler::~StagerGetHandler() throw();
	
	/* Get request handler */
	void StagerGetHandler::handle(void *param) throw();
	
      }// end StagerGetHandler class


    }//end namespace dbService
  }//end namespace stager
}//end namespace castor



#endif
