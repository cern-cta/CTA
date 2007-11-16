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
#include "Cns_api.h"
#include "expert_api.h"

#include "Cpwd.h"
#include "Cgrp.h"
#include "u64subr.h"

#include "castor/IClientFactory.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"

#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/ObjectSet.hpp"
#include "castor/IObject.hpp"

#include "serrno.h"
#include <errno.h>

#include <iostream>
#include <string>



namespace castor{
  namespace stager{
    namespace dbService{

      class StagerRequestHelper;
      class StagerCnsHelper;


      class StagerPrepareToPutHandler : public virtual StagerJobRequestHandler{

    
      public:
	/* constructor */
	StagerPrepareToPutHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper = 0) throw(castor::exception::Exception);

	/* destructor */
	~StagerPrepareToPutHandler() throw();

	void handlerSettings() throw(castor::exception::Exception);

	/* PrepareToPut request handler */
	void handle() throw(castor::exception::Exception);


	
      }; //end StagerPrepareToPutHandler class

    }//end dbService namespace
  }//end stager namespace
}//end castor namespace


#endif
