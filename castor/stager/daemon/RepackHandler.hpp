/**********************************************************************************************/
/* StagerRepackHandler: Constructor and implementation of the repack subrequest's handler */
/* It inherits from the StagerJobRequestHandler and it needs to reply to the client         */
/*******************************************************************************************/

#ifndef STAGER_REPACK_HANDLER_HPP
#define STAGER_REPACK_HANDLER_HPP 1


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

      class StagerRepackRequest : public StagerJobRequestHandler{

     
      public:
	/* constructor */
	StagerRepackHandler::StagerRepackHandler(StagerRequestHelper* stgRequestHelper,StagerCnsHelper* stgCnsHelper, std::string message) throw();
	/* destructor */
	StagerRepackHandler::~StagerRepackHandler() throw();

	/* repack subrequest' s handler*/
	void StagerRepackHandler::handle(void *param) throw();


      }// end StagerRepackHandler class


    }//end namespace dbService
  }//end namespace stager
}//end namespace castor


#endif
