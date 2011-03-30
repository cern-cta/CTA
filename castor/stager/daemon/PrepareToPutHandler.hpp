/****************************************************************************************************/
/* PrepareToPutHandler: Constructor and implementation of the PrepareToPut request's handler */
/**************************************************************************************************/

#ifndef STAGER_PREPARE_TO_PUT_HANDLER_HPP
#define STAGER_PREPARE_TO_PUT_HANDLER_HPP 1

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

      class RequestHelper;

      class PrepareToPutHandler : public OpenRequestHandler {

      public:
        /* constructor */
        PrepareToPutHandler(RequestHelper* reqHelper) throw(castor::exception::Exception) :
          OpenRequestHandler(reqHelper) {};

        /* destructor */
        ~PrepareToPutHandler() throw() {};

        /* PrepareToPut request handler */
        virtual void handle() throw(castor::exception::Exception);
        
        /* PrepareToPut specific handling */
        void handlePut() throw(castor::exception::Exception);
	
      }; //end PrepareToPutHandler class

    }//end daemon namespace
  }//end stager namespace
}//end castor namespace


#endif
