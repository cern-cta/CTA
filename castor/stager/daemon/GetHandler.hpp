
/*************************************************************************************/
/* GetHandler: Constructor and implementation of the get subrequest's handler */
/***********************************************************************************/

#ifndef STAGER_GET_HANDLER_HPP
#define STAGER_GET_HANDLER_HPP 1


#include "castor/IObject.hpp"
#include "castor/ObjectSet.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"

#include "serrno.h"
#include <errno.h>
#include <iostream>
#include <string>

#include "castor/stager/daemon/RequestHelper.hpp"
#include "castor/stager/daemon/OpenRequestHandler.hpp"


namespace castor{
  namespace stager{
    namespace daemon{
      
      class GetHandler : public OpenRequestHandler {
        
      public:

        /* constructor */
        GetHandler(RequestHelper* reqHelper) throw(castor::exception::Exception) :
          OpenRequestHandler(reqHelper) {};
        /* destructor */
        ~GetHandler() throw() {};
        
        virtual bool switchDiskCopiesForJob() throw (castor::exception::Exception);
        
        /* Get request handler */
        virtual void handle() throw (castor::exception::Exception);
        
        /* specific handling for Get requests */
        void handleGet() throw (castor::exception::Exception);
        
      private:
        
        /// list of available diskcopies for the request to be scheduled        
        std::list<castor::stager::DiskCopyForRecall *> sources;
                
      }; // end GetHandler class
      
      
    }//end namespace daemon
  }//end namespace stager
}//end namespace castor



#endif
