/******************************************************************************************************/
/* StagerPrepareToPutHandler: Constructor and implementation of the PrepareToUpdaterequest's handler */
/****************************************************************************************************/

#ifndef STAGER_PREPARE_TO_UPDATE_HANDLER_HPP
#define STAGER_PREPARE_TO_UPDATE_HANDLER_HPP 1




#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"

#include "castor/stager/dbService/StagerRequestHandler.hpp"
#include "castor/stager/dbService/StagerJobRequestHandler.hpp"

#include "stager_uuid.h"
#include "stager_constants.h"
#include "serrno.h"
#include "Cns_api.h"
#include "expert_api.h"
#include "rm_api.h"
#include "Cpwd.h"
#include "Cgrp.h"
#include "u64subr.h"
#include "castor/IClientFactory.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"

#include "castor/IObject.hpp"
#include "castor/ObjectSet.hpp"
#include "castor/exception/Exception.hpp"

#include <iostream>
#include <string>

namespace castor{
  namespace stager{
    namespace dbService{

      class StagerRequestHelper;
      class StagerCnsHelper;


      class StagerPrepareToUpdateHandler : public virtual StagerJobRequestHandler{

      private:
	/* flag to schedule or to recreateCastorFile depending if the file exists... */
	bool toRecreateCastorFile;



      public:
	/* constructor */
	StagerPrepareToUpdateHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper, bool toRecreateCastorFile) throw(castor::exception::Exception);

	/* destructor */
	~StagerPrepareToUpdateHandler() throw();

	/* PrepareToUpdate request handler */
	void handle() throw(castor::exception::Exception);
	



      }; //end StagerPrepareToUpdateHandler class

    }//end dbService namespace
  }//end stager namespace
}//end castor namespace


#endif
