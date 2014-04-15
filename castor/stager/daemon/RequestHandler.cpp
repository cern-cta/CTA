/*******************************************************************************************************/
/* Base class for OpenRequestHandler and all the fileRequest handlers                            */
/* Basically: handle() as METHOD  and  (reqHelper,reqHelper,stgReplyHelper)  as ATTRIBUTES */
/****************************************************************************************************/


#include "castor/stager/daemon/RequestHandler.hpp"

namespace castor{
  namespace stager{
    namespace daemon{
      
      /* function to perform the common flow for all the handlers */
      bool RequestHandler::handle() throw(castor::exception::Exception)
      {
        // get the svcClass and eventually the forced fileClass
        reqHelper->resolveSvcClass();

        return true;
      }
      
    }//end namespace daemon
  }//end namespace stager
}//end namespace castor
