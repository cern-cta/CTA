/*************************************************************************************************/
/* PutDoneHandler: Constructor and implementation of the PutDone request's handle */
/* basically it is a calll to the jobService->prepareForMigration()       */
/***********************************************************************************************/


#ifndef STAGER_PUT_DONE_HANDLER_HPP
#define STAGER_PUT_DONE_HANDLER_HPP 1


#include "castor/IObject.hpp"
#include "castor/ObjectSet.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"

#include "serrno.h"
#include <errno.h>
#include <iostream>
#include <string>

#include "castor/stager/daemon/RequestHelper.hpp"
#include "castor/stager/daemon/RequestHandler.hpp"


namespace castor{
  namespace stager{
    namespace daemon{

      class RequestHelper;

      class PutDoneHandler : public RequestHandler {
	
      public:
        /* constructor */
        PutDoneHandler(RequestHelper* reqHelper) throw(castor::exception::Exception) :
          RequestHandler(reqHelper) {};
        /* destructor */
        ~PutDoneHandler() throw() {};

        /* putDone request handler */
        virtual bool handle() throw(castor::exception::Exception);
        
      }; //end PutDoneHandler class

    }//end daemon 
  }//end stager
}//end castor



#endif
