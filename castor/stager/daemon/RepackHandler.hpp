/**********************************************************************************************/
/* StagerRepackHandler: Constructor and implementation of the repack subrequest's handler */
/* It inherits from the StagerJobRequestHandler and it needs to reply to the client         */
/*******************************************************************************************/

#ifndef STAGER_REPACK_HANDLER_HPP
#define STAGER_REPACK_HANDLER_HPP 1


#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerPrepareToGetHandler.hpp"
#include "castor/exception/Exception.hpp"

namespace castor{
  namespace stager{
    namespace dbService{

      class StagerRequestHelper;
      class StagerCnsHelper;

      class StagerRepackHandler : public virtual StagerPrepareToGetHandler{

      public:
        /* constructor */
        StagerRepackHandler(StagerRequestHelper* stgRequestHelper) throw(castor::exception::Exception);
        
        virtual bool switchDiskCopiesForJob() throw (castor::exception::Exception);

      }; // end StagerRepackHandler class


    }//end namespace dbService
  }//end namespace stager
}//end namespace castor


#endif
