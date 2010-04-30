
/*************************************************************************************/
/* GetHandler: Constructor and implementation of the get subrequest's handler */
/***********************************************************************************/

#ifndef STAGER_GET_HANDLER_HPP
#define STAGER_GET_HANDLER_HPP 1




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
#include "castor/exception/Exception.hpp"


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
      
      class GetHandler : public virtual JobRequestHandler{
        
        
      public:

        /* constructor */
        GetHandler(RequestHelper* stgRequestHelper, CnsHelper* stgCnsHelper = 0) throw(castor::exception::Exception);
        /* destructor */
        ~GetHandler() throw();
        
        void handlerSettings() throw(castor::exception::Exception);
        
        virtual bool switchDiskCopiesForJob() throw (castor::exception::Exception);
        
        /* Get request handler */
        void handle() throw(castor::exception::Exception);
        
      private:
        
        /// list of available diskcopies for the request to be scheduled        
        std::list<castor::stager::DiskCopyForRecall *> sources;
                
      }; // end GetHandler class
      
      
    }//end namespace daemon
  }//end namespace stager
}//end namespace castor



#endif
