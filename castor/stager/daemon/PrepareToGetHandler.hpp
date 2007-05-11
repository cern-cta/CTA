/**********************************************************************************************/
/* StagerPrepareToGetHandler: Constructor and implementation of the get subrequest's handler */
/* It inherits from the StagerJobRequestHandler and it needs to reply to the client         */
/*******************************************************************************************/


#ifndef STAGER_PREPARE_TO_GET_HANDLER_HPP
#define STAGER_PREPARE_TO_GET_HANDLER_HPP 1



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
      
      class StagerPrepareToGetRequest : public StagerJobRequestHandler{

	
      public:

	/* constructor */
	StagerPrepareToGetHandler::StagerPrepareToGetHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper, std::string message) throw();
	/* destructor */
	StagerPrepareToGetHandler::~StagerPrepareToGetHandler() throw();

	/********************************************************************************************************/
	/* we are overwritting this function inherited from the StagerJobRequestHandler  because of the case 0 */
	/*  it isn't included on the switchScheduling, it's processed on the handle main flow                 */
	/* after asking the stagerService is it is toBeScheduled                     */
	/* - do a normal tape recall                                                */
	/* - check if it is to replicate:                                          */
	/*         +processReplica if it is needed:                               */
	/*                    +make the hostlist if it is needed                 */
	/************************************************************************/
	void StagerPrepareToGetHandler::switchScheduling(int caseToSchedule);

	/* PrepareToGet request handler */
        void StagerPrepareToGetHandler::handle(void *param) throw();

      }//end StagerPrepareToGetHandler class


    }//end namespace dbService
  }//end namespace stager
}//end namespace castor


#endif
