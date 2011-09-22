/**********************************************************************************************
 * RepackHandler: Constructor and implementation of the repack subrequest's handler  
 * Note that this implementation is a mix between the "ideal" implementation, where everything
 * is done in the DB and the old implementation, where everything is done in C++.
 * Here, subrequestToDo was kept, as well as the usage of handlers, but this handler only
 * does one call to the DB and answers the client. This should be the base for the "ideal"
 * implementation of the stager.
 *******************************************************************************************/

#ifndef STAGER_REPACK_HANDLER_HPP
#define STAGER_REPACK_HANDLER_HPP 1

#include "castor/stager/daemon/OpenRequestHandler.hpp"
#include "castor/exception/Exception.hpp"

namespace castor{
  namespace stager{
    namespace daemon{

      class RepackHandler : public virtual OpenRequestHandler {

      public:

        /* constructor
         * @reqHelper the request helper to be used. Note that is is actually
         * only used for getting the subRequest id.
         */
        RepackHandler(RequestHelper* reqHelper) throw(castor::exception::Exception) :
          OpenRequestHandler(reqHelper) {};
        
        /**
         * Main function for the specific request handler.
         * Default behavior is the common part across all user requests processing.
         * @throw exception in case of system/db errors.
         */
        virtual void handle() throw (castor::exception::Exception);

      }; // end RepackHandler class

    }//end namespace daemon
  }//end namespace stager
}//end namespace castor


#endif
