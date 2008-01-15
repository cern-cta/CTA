/**********************************************************************************************/
/* StagerPrepareToGetHandler: Constructor and implementation of the get subrequest's handler */
/* It inherits from the StagerJobRequestHandler and it needs to reply to the client         */
/*******************************************************************************************/


#ifndef STAGER_PREPARE_TO_GET_HANDLER_HPP
#define STAGER_PREPARE_TO_GET_HANDLER_HPP 1



#include "castor/stager/daemon/StagerRequestHelper.hpp"
#include "castor/stager/daemon/StagerCnsHelper.hpp"
#include "castor/stager/daemon/StagerReplyHelper.hpp"

#include "castor/stager/daemon/StagerRequestHandler.hpp"
#include "castor/stager/daemon/StagerJobRequestHandler.hpp"


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

      class StagerRequestHelper;
      class StagerCnsHelper;
      
      class StagerPrepareToGetHandler : public virtual StagerJobRequestHandler{

	
      public:

	/* constructor */
	StagerPrepareToGetHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper = 0) throw(castor::exception::Exception);
	/* destructor */
	~StagerPrepareToGetHandler() throw();

	void handlerSettings() throw(castor::exception::Exception);

	/*******************************************/	
	/*     switch(getDiskCopyForJob):         */  
	/*        case 0: (staged) archiveSubrequest */                                   
	/*        case 1: (staged) waitD2DCopy  */
	/*        case 2: (waitRecall) createTapeCopyForRecall */
	virtual bool switchDiskCopiesForJob() throw (castor::exception::Exception);
	



	/* PrepareToGet request handler */
        void handle() throw(castor::exception::Exception);

      }; //end StagerPrepareToGetHandler class


    }//end namespace daemon
  }//end namespace stager
}//end namespace castor


#endif
