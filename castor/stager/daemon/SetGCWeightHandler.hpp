/*******************************************************************************************************/
/* handler for the SetFileGCWeight request, simply call to the stagerService->setFileGCWeight()       */
/* since it isn't job oriented, it inherits from the RequestHandler                            */
/* it always needs to reply to the client                                                           */
/***************************************************************************************************/


#ifndef STAGER_SET_GC_HANDLER_HPP
#define STAGER_SET_GC_HANDLER_HPP 1

#include "serrno.h"
#include <errno.h>
#include <iostream>
#include <string>

#include "castor/IObject.hpp"
#include "castor/ObjectSet.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/stager/SetFileGCWeight.hpp"

#include "castor/stager/daemon/RequestHandler.hpp"
#include "castor/stager/daemon/RequestHelper.hpp"


namespace castor{
  namespace stager{
    namespace daemon{

      class SetGCWeightHandler : public RequestHandler {

      public:
        /* constructor */
        SetGCWeightHandler(RequestHelper* reqHelper) throw(castor::exception::Exception) :
          RequestHandler(reqHelper) {};
        /* destructor */
        ~SetGCWeightHandler() throw() {};

        /* SetFileGCWeight handle implementation */
        virtual bool handle() throw(castor::exception::Exception);
        
      }; //end SetGCWeightHandler class

    }//end namespace daemon
  }//end namespace stager
}//end namespace castor

#endif 
