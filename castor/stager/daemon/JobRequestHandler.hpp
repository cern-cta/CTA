/*******************************************************************************************************/
/* base class for the 7 job handlers(Get,PrepareToGet,Update,PrepareToUpdate,Put,PrepareToPut,Repack) */
/*****************************************************************************************************/
#ifndef STAGER_JOB_REQUEST_HANDLER_HPP
#define STAGER_JOB_REQUEST_HANDLER_HPP 1

#include "castor/stager/daemon/RequestHelper.hpp"
#include "castor/stager/daemon/CnsHelper.hpp"
#include "castor/stager/daemon/ReplyHelper.hpp"

#include "castor/stager/daemon/RequestHandler.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "stager_uuid.h"
#include "stager_constants.h"

#include "Cns_api.h"
#include "expert_api.h"

#include "serrno.h"
#include <errno.h>

#include "Cpwd.h"
#include "Cgrp.h"
#include "u64subr.h"




#include "castor/exception/Exception.hpp"
#include "castor/ObjectSet.hpp"

#include <iostream>
#include <string>


namespace castor{
  namespace stager{
    namespace daemon{
      
      
      class JobRequestHandler : public RequestHandler{
        
      public:
        JobRequestHandler() throw() : m_notifyJobManager(false) {};
        virtual ~JobRequestHandler() throw() {};
        
        /* main function which must be implemented on each handler */
        virtual void handle() throw(castor::exception::Exception) = 0;
        
        /****************************************************************************************/
        /* job oriented block  */
        /****************************************************************************************/
        void jobOriented() throw(castor::exception::Exception);
        
        /********************************************/	
        /* for Get, Update                          */
        /*     switch(getDiskCopyForJob):           */
        /*        case 0,1: (staged) jobmanager     */
        /*        case 2: (waitRecall) createTapeCopyForRecall */
        /* to be overwriten in Repack, PrepareToGetHandler, PrepareToUpdateHandler  */
        virtual bool switchDiskCopiesForJob() throw (castor::exception::Exception) { return false; }; 
        
        bool notifyJobManager() {
          return m_notifyJobManager;
        }

      protected:
        
        unsigned int maxReplicaNb;
        std::string default_protocol;
        
        unsigned int xsize;	
        
        bool m_notifyJobManager;
        
      }; //end class JobRequestHandler
      
      
      
    }//end namespace daemon
  }//end namespace stager
}//end namespace castor


#endif





