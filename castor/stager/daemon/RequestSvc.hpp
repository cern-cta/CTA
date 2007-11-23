
/*************************************/
/* main class for the ___RequestSvc */
/***********************************/

#ifndef REQUEST_SVC_HPP
#define REQUEST_SVC_HPP 1

#include "castor/server/SelectProcessThread.hpp"
#include "castor/Constants.hpp"

#include "castor/stager/dbService/StagerRequestHelper.hpp"

#include "serrno.h"
#include <errno.h>

#include "castor/exception/Exception.hpp"
#include "castor/IObject.hpp"

#include <string>
#include <iostream>

namespace castor {
  namespace stager{
    namespace dbService {
      
      class StagerRequestHelper;
      class StagerCnsHelper;
      
      class RequestSvc : public castor::server::SelectProcessThread{
        
      public: 

        RequestSvc(std::string name) throw() : SelectProcessThread(), m_name(name) {};
        virtual ~RequestSvc() throw() { };
        
        /***************************************************************************/
        /* abstract functions inherited from the SelectProcessThread to implement */
        /*************************************************************************/
        virtual castor::IObject* select() throw();
        
        void handleException(StagerRequestHelper* stgRequestHelper,StagerCnsHelper* stgCnsHelper, int errorCode, std::string errorMessage) throw();
        
      protected:
        std::string m_name;
        
        int typeRequest;

      };// end class RequestSvc
      
    }// end dbService
  } // end namespace stager
}//end namespace castor


#endif
