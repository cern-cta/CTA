/*************************************************************************************************/
/* PutDoneHandler: Constructor and implementation of the PutDone request's handle */
/* basically it is a calll to the jobService->prepareForMigration()       */
/***********************************************************************************************/


#ifndef STAGER_PUT_DONE_HANDLER_HPP
#define STAGER_PUT_DONE_HANDLER_HPP 1


#include "castor/stager/daemon/RequestHelper.hpp"
#include "castor/stager/daemon/CnsHelper.hpp"
#include "castor/stager/daemon/ReplyHelper.hpp"

#include "castor/stager/daemon/RequestHandler.hpp"
#include "castor/stager/daemon/JobRequestHandler.hpp"

#include "stager_uuid.h"
#include "stager_constants.h"
#include "expert_api.h"

#include "Cpwd.h"
#include "Cgrp.h"
#include "u64subr.h"


#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"

#include "castor/stager/DiskCopyForRecall.hpp"
#include "../IJobSvc.hpp"

#include "castor/IObject.hpp"
#include "castor/ObjectSet.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"

#include "serrno.h"
#include <errno.h>
#include <iostream>
#include <string>




namespace castor{
  namespace stager{
    namespace daemon{

      class RequestHelper;
      class CnsHelper;

      class PutDoneHandler : public virtual JobRequestHandler{
	
      public:
	/* constructor */
	PutDoneHandler(RequestHelper* stgRequestHelper) throw(castor::exception::Exception);
	/* destructor */
	~PutDoneHandler() throw() {};

	/* putDone request handler */
	void handle() throw(castor::exception::Exception);
     
	
      	
	
      }; //end PutDoneHandler class

    }//end daemon 
  }//end stager
}//end castor



#endif
