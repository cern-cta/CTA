
/*************************************************************************************/
/* StagerGetHandler: Constructor and implementation of the get subrequest's handler */
/***********************************************************************************/

#ifndef STAGER_GET_HANDLER_HPP
#define STAGER_GET_HANDLER_HPP 1




#include "castor/stager/daemon/StagerRequestHelper.hpp"
#include "castor/stager/daemon/StagerCnsHelper.hpp"
#include "castor/stager/daemon/StagerReplyHelper.hpp"

#include "castor/stager/daemon/StagerRequestHandler.hpp"
#include "castor/stager/daemon/StagerJobRequestHandler.hpp"


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
      
      class StagerRequestHelper;
      class StagerCnsHelper;
      
      class StagerGetHandler : public virtual StagerJobRequestHandler{
        
        
      public:

        /* constructor */
        StagerGetHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper = 0) throw(castor::exception::Exception);
        /* destructor */
        ~StagerGetHandler() throw();
        
        void handlerSettings() throw(castor::exception::Exception);
        
        virtual bool switchDiskCopiesForJob() throw (castor::exception::Exception);
        
        /* Get request handler */
        void handle() throw(castor::exception::Exception);
        
      private:
        
        /**********************************************/
        /* return if it is to replicate considering: */
        /* - sources.size() */
        /* - maxReplicaNb */
        /* - replicationPolicy (call to the expert system) */
        void processReplica() throw(castor::exception::Exception);
        
        /***************************************************************************************************************************/
        /* if the replicationPolicy exists, ask the expert system to get maxReplicaNb for this file                                */
        /**************************************************************************************************************************/
        int checkReplicationPolicy() throw(castor::exception::Exception);

        /// list of available diskcopies for the request to be scheduled        
        std::list<castor::stager::DiskCopyForRecall *> sources;
                
      }; // end StagerGetHandler class
      
      
    }//end namespace daemon
  }//end namespace stager
}//end namespace castor



#endif
