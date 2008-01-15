/****************************************************************************************************************************/
/* handler for the Update subrequest, since it is jobOriented, it uses the mostly part of the JobRequestHandler class*/
/* depending if the file exist, it can follow the huge flow (jobOriented, as Get) or a small one                          */
/*************************************************************************************************************************/


#ifndef STAGER_UPDATE_HANDLER_HPP
#define STAGER_UPDATE_HANDLER_HPP 1


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

#include "serrno.h"
#include <errno.h>

#include <iostream>
#include <string>


namespace castor{
  namespace stager{
    namespace daemon{
      
      class RequestHelper;
      class CnsHelper;
      
      class UpdateHandler : public virtual JobRequestHandler{
        
      protected:
        
        bool recreate;
        
      public:
      
        /* constructor */
        UpdateHandler(RequestHelper* stgRequestHelper) throw(castor::exception::Exception);
        /* destructor */
        ~UpdateHandler() throw();
        
        /* set the internal attribute "toRecreateCastorFile depending on fileExist" */
        /* which determines the real flow of the handler */
        virtual void preHandle() throw(castor::exception::Exception);
        
        /* handler for the Update request  */
        void handle() throw(castor::exception::Exception);
        
        
      }; //end UpdateHandler class
      
      
    }//end namespace daemon
  }//end namespace stager
}//end namespace castor



#endif
