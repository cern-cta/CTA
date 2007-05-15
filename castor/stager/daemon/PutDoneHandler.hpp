/*************************************************************************************************/
/* StagerPutDoneHandler: Constructor and implementation of the PutDone request's handle */
/* basically it is a calll to the jobService->prepareForMigration()       */
/***********************************************************************************************/


#ifndef STAGER_PUT_DONE_HANDLER_HPP
#define STAGER_PUT_DONE_HANDLER_HPP 1


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
#include "castor/stager/DiskCopyForRecall.hpp"

#include <iostream>
#include <string>




namespace castor{
  namespace stager{
    namespace dbService{

      class StagerRequestHelper;
      class StagerCnsHelper;

      class StagerPutDoneHandler : public StagerRequestHandler{


      private:
	castor::stager::IJobSvc* jobService;
	std::list<castor::stager::DiskCopyForRecall*> *sources;
	
	
      public:
	/* constructor */
	StagerPutDoneHandler::StagerPutDoneHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper, std::string message) throw();
	/* destructor */
	StagerPutDoneHandler::~StagerPutDoneHandler() throw();

	/* rm subrequest handler */
	void handle(void *param) throw();
     

      }//end StagerPutDoneHandler class
    }//end dbService 
  }//end stager
}//end castor



#endif
