/**********************************************************************************************/
/* PrepareToGetHandler: Constructor and implementation of the get subrequest's handler */
/* It inherits from the OpenRequestHandler and it needs to reply to the client         */
/*******************************************************************************************/


#ifndef STAGER_PREPARE_TO_GET_HANDLER_HPP
#define STAGER_PREPARE_TO_GET_HANDLER_HPP 1

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
      
      class PrepareToGetHandler : public OpenRequestHandler {

      public:

        /* constructor */
        PrepareToGetHandler(RequestHelper* reqHelper) throw(castor::exception::Exception) :
          OpenRequestHandler(reqHelper) {};
        /* destructor */
        ~PrepareToGetHandler() throw() {};

        virtual bool switchDiskCopiesForJob() throw (castor::exception::Exception);
	
        /* PrepareToGet request handler */
        virtual void handle() throw(castor::exception::Exception);

        /* specific handling for PrepareToGet requests */
        void handleGet() throw (castor::exception::Exception);
        
      }; //end PrepareToGetHandler class


    }//end namespace daemon
  }//end namespace stager
}//end namespace castor


#endif
