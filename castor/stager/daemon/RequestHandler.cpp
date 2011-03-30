/*******************************************************************************************************/
/* Base class for OpenRequestHandler and all the fileRequest handlers                            */
/* Basically: handle() as METHOD  and  (reqHelper,reqHelper,stgReplyHelper)  as ATTRIBUTES */
/****************************************************************************************************/


#include "castor/stager/daemon/RequestHandler.hpp"

namespace castor{
  namespace stager{
    namespace daemon{
      
      /* function to perform the common flow for all the handlers */
      void RequestHandler::handle() throw(castor::exception::Exception)
      {
        // set the username and groupname for logging
        reqHelper->setUsernameAndGroupname();

        // get the svcClass and eventually the forced fileClass
        reqHelper->resolveSvcClass();
      }
      
    }//end namespace daemon
  }//end namespace stager
}//end namespace castor
