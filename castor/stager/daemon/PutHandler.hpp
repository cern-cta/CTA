/*************************************************************************************/
/* GetHandler: Constructor and implementation of the get subrequest's handler */
/***********************************************************************************/

#ifndef STAGER_PUT_HANDLER_HPP
#define STAGER_PUT_HANDLER_HPP 1




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

      class RequestHelper;
      class CnsHelper;

      class PutHandler : public virtual JobRequestHandler{

	
      public:
	/* constructor */
	PutHandler(RequestHelper* stgRequestHelper, CnsHelper* stgCnsHelper = 0) throw(castor::exception::Exception);
	/* destructor */
	~PutHandler() throw() {};
	
	void handlerSettings() throw(castor::exception::Exception);

	/* Put request handler */
	void handle() throw(castor::exception::Exception);
	

	
	
      }; // end PutHandler class


    }//end namespace daemon
  }//end namespace stager
}//end namespace castor



#endif
