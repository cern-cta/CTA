/****************************************************************************************************/
/* PrepareToPutHandler: Constructor and implementation of the PrepareToPut request's handler */
/**************************************************************************************************/

#ifndef STAGER_PREPARE_TO_PUT_HANDLER_HPP
#define STAGER_PREPARE_TO_PUT_HANDLER_HPP 1




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
#include "castor/exception/Internal.hpp"
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


      class PrepareToPutHandler : public virtual JobRequestHandler{

    
      public:
	/* constructor */
	PrepareToPutHandler(RequestHelper* stgRequestHelper, CnsHelper* stgCnsHelper = 0) throw(castor::exception::Exception);

	/* destructor */
	~PrepareToPutHandler() throw();

	void handlerSettings() throw(castor::exception::Exception);

	/* PrepareToPut request handler */
	void handle() throw(castor::exception::Exception);


	
      }; //end PrepareToPutHandler class

    }//end daemon namespace
  }//end stager namespace
}//end castor namespace


#endif
