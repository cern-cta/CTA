/**********************************************************************************************/
/* PrepareToGetHandler: Constructor and implementation of the get subrequest's handler */
/* It inherits from the JobRequestHandler and it needs to reply to the client         */
/*******************************************************************************************/


#ifndef STAGER_PREPARE_TO_GET_HANDLER_HPP
#define STAGER_PREPARE_TO_GET_HANDLER_HPP 1



#include "castor/stager/daemon/RequestHelper.hpp"
#include "castor/stager/daemon/CnsHelper.hpp"
#include "castor/stager/daemon/ReplyHelper.hpp"

#include "castor/stager/daemon/RequestHandler.hpp"
#include "castor/stager/daemon/JobRequestHandler.hpp"


#include "stager_uuid.h"
#include "stager_constants.h"

#include "Cns_api.h"
#include "expert_api.h"

#include "Cpwd.h"
#include "Cgrp.h"
#include "u64subr.h"

#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"

#include "castor/exception/Exception.hpp"

#include "castor/ObjectSet.hpp"
#include "castor/IObject.hpp"

#include "serrno.h"
#include <errno.h>

#include <iostream>
#include <string>



namespace castor{
  namespace stager{
    namespace daemon{

      class RequestHelper;
      class CnsHelper;
      
      class PrepareToGetHandler : public virtual JobRequestHandler{

	
      public:

	/* constructor */
	PrepareToGetHandler(RequestHelper* stgRequestHelper, CnsHelper* stgCnsHelper = 0) throw(castor::exception::Exception);
	/* destructor */
	~PrepareToGetHandler() throw();

	void handlerSettings() throw(castor::exception::Exception);

	/*******************************************/	
	/*     switch(getDiskCopyForJob):         */  
	/*        case 0: (staged) archiveSubrequest */                                   
	/*        case 1: (staged) waitD2DCopy  */
	/*        case 2: (waitRecall) createTapeCopyForRecall */
	virtual bool switchDiskCopiesForJob() throw (castor::exception::Exception);
	



	/* PrepareToGet request handler */
        void handle() throw(castor::exception::Exception);

      }; //end PrepareToGetHandler class


    }//end namespace daemon
  }//end namespace stager
}//end namespace castor


#endif
