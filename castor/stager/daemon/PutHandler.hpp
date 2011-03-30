/*************************************************************************************/
/* GetHandler: Constructor and implementation of the get subrequest's handler */
/***********************************************************************************/

#ifndef STAGER_PUT_HANDLER_HPP
#define STAGER_PUT_HANDLER_HPP 1

#include "serrno.h"
#include <errno.h>
#include <iostream>
#include <string>

#include "stager_constants.h"
#include "Cns_api.h"
#include "u64subr.h"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/IObject.hpp"
#include "castor/ObjectSet.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/stager/daemon/RequestHelper.hpp"
#include "castor/stager/daemon/ReplyHelper.hpp"
#include "castor/stager/daemon/OpenRequestHandler.hpp"


namespace castor{
  namespace stager{
    namespace daemon{

      class PutHandler : public OpenRequestHandler {
	
      public:
        /* constructor */
        PutHandler(RequestHelper* reqHelper) throw(castor::exception::Exception) :
          OpenRequestHandler(reqHelper) {};
        /* destructor */
        ~PutHandler() throw() {};
	
        /* Put request handler */
        virtual void handle() throw (castor::exception::Exception);
        
        /* specific handling for Put */
        void handlePut() throw (castor::exception::Exception);
	
      }; // end PutHandler class

    }//end namespace daemon
  }//end namespace stager
}//end namespace castor

#endif
