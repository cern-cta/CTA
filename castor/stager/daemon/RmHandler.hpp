/************************************************************************************/
/* handler for the Rm subrequest, simply call to the stagerService->stageRm()      */
/* since it isn't job oriented, it inherits from the RequestHandler         */
/*********************************************************************************/


#ifndef STAGER_RM_HANDLER_HPP
#define STAGER_RM_HANDLER_HPP 1

#include "castor/stager/daemon/RequestHandler.hpp"

#include "castor/stager/daemon/RequestHelper.hpp"
#include "castor/stager/daemon/CnsHelper.hpp"
#include "castor/stager/daemon/ReplyHelper.hpp"

#include "u64subr.h"

#include "castor/stager/IStagerSvc.hpp"
#include "castor/stager/SubRequest.hpp"

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
      
      class RmHandler : public RequestHandler {
        
      public:
        /* constructor */
        RmHandler(RequestHelper* stgRequestHelper) throw ();
        /* destructor */
        ~RmHandler() throw() {};
        
        virtual void preHandle() throw(castor::exception::Exception);
        
        /* rm subrequest handler */
        void handle() throw(castor::exception::Exception);
        
      }; // end RmHandler class
      
      
    }//end namespace daemon 
  }//end namespace stager
}//end namespace castor



#endif
