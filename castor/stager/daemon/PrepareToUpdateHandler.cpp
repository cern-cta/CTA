/***********************************************************************************************************/
/* PrepareToUpdatetHandler: Constructor and implementation of the PrepareToUpdate request handler   */
/* Since it is jobOriented, it uses the mostly part of the OpenRequestHandler class                 */
/* Depending if the file exist, it can follow the huge flow (scheduled, as Get) or a small one (as Put) */
/* We need to reply to the client (just in case of error )                                             */
/******************************************************************************************************/




#include "castor/stager/daemon/ReplyHelper.hpp"

#include "castor/stager/daemon/PrepareToUpdateHandler.hpp"
#include "castor/stager/daemon/PrepareToPutHandler.hpp"
#include "castor/stager/daemon/PrepareToGetHandler.hpp"

#include "stager_uuid.h"
#include "stager_constants.h"
#include "Cns_api.h"


#include "Cpwd.h"
#include "Cgrp.h"

#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"
#include "castor/stager/DiskCopyForRecall.hpp"

#include "castor/exception/Exception.hpp"

#include "castor/dlf/Dlf.hpp"
#include "castor/dlf/Message.hpp"
#include "castor/stager/daemon/DlfMessages.hpp"

#include "serrno.h"
#include <errno.h>

#include <iostream>
#include <string>


namespace castor{
  namespace stager{
    namespace daemon{

      void PrepareToUpdateHandler::handle() throw(castor::exception::Exception)
      {
        // Inherited behavior from RequestHandler, overriding the OpenRequestHandler one
        RequestHandler::handle();
        
        /* check permissions and open the file according to the request type file */
        bool recreate = reqHelper->openNameServerFile() || ((reqHelper->subrequest->flags() & O_TRUNC) == O_TRUNC);

        /* get the castorFile entity and populate its links in the db */
        reqHelper->getCastorFile();

        reqHelper->logToDlf(DLF_LVL_DEBUG, STAGER_PREPARETOUPDATE, &(reqHelper->cnsFileid));
        
        if(recreate) {
          // delegate to PrepareToPut (may throw exception, which are forwarded)
          PrepareToPutHandler* h = new PrepareToPutHandler(reqHelper);
          h->handlePut();
          delete h;
        }
        else {
          // delegate to PrepareToGet (may throw exception, which are forwarded)
          PrepareToGetHandler* h = new PrepareToGetHandler(reqHelper);
          h->handleGet();
          delete h;
        }      
      }
      
    }// end daemon namespace
  }// end stager namespace
}//end castor namespace 
