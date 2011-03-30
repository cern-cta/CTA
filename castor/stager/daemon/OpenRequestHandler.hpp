/*******************************************************************************************************/
/* base class for the 7 job handlers(Get,PrepareToGet,Update,PrepareToUpdate,Put,PrepareToPut,Repack) */
/*****************************************************************************************************/
#ifndef STAGER_JOB_REQUEST_HANDLER_HPP
#define STAGER_JOB_REQUEST_HANDLER_HPP 1

#include <iostream>
#include <string>

#include "castor/exception/Exception.hpp"
#include "castor/stager/daemon/RequestHelper.hpp"
#include "castor/stager/daemon/RequestHandler.hpp"


namespace castor{
  namespace stager{
    namespace daemon{
      
      
      class OpenRequestHandler : public RequestHandler {
        
      public:

        OpenRequestHandler(RequestHelper* reqHelper) throw() :
          RequestHandler(reqHelper), m_notifyJobManager(false) {};
        virtual ~OpenRequestHandler() throw() {};
        
        virtual void handle() throw(castor::exception::Exception);
        
        bool notifyJobManager() {
          return m_notifyJobManager;
        }

      protected:
        
        bool m_notifyJobManager;
        
      }; //end class OpenRequestHandler
      
    } //end namespace daemon
    
  } //end namespace stager
  
} //end namespace castor

#endif
