/*******************************************************************************************************/
/* base class for the 7 job handlers(Get,PrepareToGet,Update,PrepareToUpdate,Put,PrepareToPut,Repack) */
/*****************************************************************************************************/

#include "serrno.h"
#include <errno.h>
#include <iostream>
#include <string>

#include "castor/exception/Exception.hpp"
#include "castor/stager/daemon/RequestHelper.hpp"
#include "castor/stager/daemon/OpenRequestHandler.hpp"


namespace castor{
  namespace stager{
    namespace daemon{
      
      void OpenRequestHandler::handle() throw(castor::exception::Exception)
      {
        /* inherited behavior */
        RequestHandler::handle();
        /* check permissions and open the file according to the request type file */
        reqHelper->openNameServerFile();
        /* get the castorFile entity and populate its links in the db */
        reqHelper->getCastorFile();
      }
      
    }//end namespace daemon
  }//end namespace stager
}//end namespce castor
