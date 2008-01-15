/**********************************************************************************************/
/* RepackHandler: Constructor and implementation of the repack subrequest's handler */
/* It inherits from the JobRequestHandler and it needs to reply to the client         */
/*******************************************************************************************/

#ifndef STAGER_REPACK_HANDLER_HPP
#define STAGER_REPACK_HANDLER_HPP 1


#include "castor/stager/daemon/RequestHelper.hpp"
#include "castor/stager/daemon/PrepareToGetHandler.hpp"
#include "castor/exception/Exception.hpp"

namespace castor{
  namespace stager{
    namespace daemon{

      class RequestHelper;
      class CnsHelper;

      class RepackHandler : public virtual PrepareToGetHandler{

      public:
        /* constructor */
        RepackHandler(RequestHelper* stgRequestHelper) throw(castor::exception::Exception);
        
        virtual bool switchDiskCopiesForJob() throw (castor::exception::Exception);

      }; // end RepackHandler class


    }//end namespace daemon
  }//end namespace stager
}//end namespace castor


#endif
