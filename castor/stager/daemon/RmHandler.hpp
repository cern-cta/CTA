/************************************************************************************/
/* handler for the Rm subrequest, simply call to the stagerService->stageRm()      */
/* since it isn't job oriented, it inherits from the StagerRequestHandler         */
/*********************************************************************************/


#ifndef STAGER_RM_HANDLER_HPP
#define STAGER_RM_HANDLER_HPP 1

#include "castor/stager/daemon/StagerRequestHandler.hpp"

#include "castor/stager/daemon/StagerRequestHelper.hpp"
#include "castor/stager/daemon/StagerCnsHelper.hpp"
#include "castor/stager/daemon/StagerReplyHelper.hpp"

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
      
      class StagerRequestHelper;
      class StagerCnsHelper;
      class castor::stager::SubRequest;
      
      class StagerRmHandler : public StagerRequestHandler {
        
      public:
        /* constructor */
        StagerRmHandler(StagerRequestHelper* stgRequestHelper) throw ();
        /* destructor */
        ~StagerRmHandler() throw() {};
        
        virtual void preHandle() throw(castor::exception::Exception);
        
        /* rm subrequest handler */
        void handle() throw(castor::exception::Exception);
        
      }; // end StagerRmHandler class
      
      
    }//end namespace daemon 
  }//end namespace stager
}//end namespace castor



#endif
