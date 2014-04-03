/*******************************************************************************************************/
/* base class for the 7 job handlers(Get,PrepareToGet,Update,PrepareToUpdate,Put,PrepareToPut,Repack) */
/*****************************************************************************************************/
#pragma once

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
          RequestHandler(reqHelper) {};

        virtual ~OpenRequestHandler() throw() {};
        
        virtual bool handle() throw(castor::exception::Exception);

      }; //end class OpenRequestHandler
      
    } //end namespace daemon
    
  } //end namespace stager
  
} //end namespace castor

