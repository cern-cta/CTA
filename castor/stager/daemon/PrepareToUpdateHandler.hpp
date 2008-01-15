/******************************************************************************************************/
/* StagerPrepareToPutHandler: Constructor and implementation of the PrepareToUpdaterequest's handler */
/****************************************************************************************************/

#ifndef STAGER_PREPARE_TO_UPDATE_HANDLER_HPP
#define STAGER_PREPARE_TO_UPDATE_HANDLER_HPP 1




#include "castor/stager/daemon/StagerRequestHelper.hpp"
#include "castor/stager/daemon/StagerCnsHelper.hpp"
#include "castor/stager/daemon/StagerReplyHelper.hpp"

#include "castor/stager/daemon/StagerUpdateHandler.hpp"

#include "stager_uuid.h"
#include "stager_constants.h"
#include "Cns_api.h"
#include "expert_api.h"

#include "Cpwd.h"
#include "Cgrp.h"
#include "u64subr.h"

#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"

#include "castor/IObject.hpp"
#include "castor/ObjectSet.hpp"
#include "castor/exception/Exception.hpp"

#include "serrno.h"
#include <errno.h>

#include <iostream>
#include <string>

namespace castor{
  namespace stager{
    namespace daemon{

      class StagerRequestHelper;
      class StagerCnsHelper;


      class StagerPrepareToUpdateHandler : public virtual StagerUpdateHandler {
	
      public:
        /* constructor */
        StagerPrepareToUpdateHandler(StagerRequestHelper* stgRequestHelper) throw(castor::exception::Exception);
        /* destructor */
        ~StagerPrepareToUpdateHandler() throw() {};
        
        /* PrepareToUpdate request handler */
        void handle() throw(castor::exception::Exception);
	
      };

    }//end daemon namespace
  }//end stager namespace
}//end castor namespace


#endif
