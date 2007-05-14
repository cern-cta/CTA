/****************************************************************************************************/
/* StagerPrepareToPutHandler: Constructor and implementation of the PrepareToPut request's handler */
/**************************************************************************************************/

#ifndef STAGER_PREPARE_TO_PUT_HANDLER_HPP
#define STAGER_PREPARE_TO_PUT_HANDLER_HPP 1




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
      class castor::stager::DiskCopyForRecall;


      class StagerPrepareToPutHandler : public StagerJobRequestHandler{

      private:
	castor::stager::DiskCopyForRecall* diskCopyForRecall;

      public:
	/* constructor */
	StagerPrepareToPutHandler::StagerPrepareToPutHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper, std::string message) throw();

	/* destructor */
	StagerPrepareToPutHandler::~StagerPrepareToPutHandler() throw();

	/* PrepareToPut request handler */
	void StagerPrepareToPutHandler::handle(void *param) throw();
	
      }//end StagerPrepareToPutHandler class

    }//end dbService namespace
  }//end stager namespace
}//end castor namespace


#endif
