/************************************************************************************************************/
/* UpdateHandler: Contructor and implementation of the Update request handler                        */
/* Since it is jobOriented, it uses the mostly part of the OpenRequestHandler class                  */
/* Depending if the file exist, it can follow the huge flow (scheduled, as Get) or a small one (as Put)  */
/* We dont  need to reply to the client (just in case of error )                                        */
/*******************************************************************************************************/




#include "castor/stager/daemon/RequestHelper.hpp"
#include "castor/stager/daemon/ReplyHelper.hpp"
#include "castor/stager/daemon/OpenRequestHandler.hpp"
#include "castor/stager/daemon/UpdateHandler.hpp"
#include "castor/stager/daemon/PutHandler.hpp"
#include "castor/stager/daemon/GetHandler.hpp"

#include "stager_uuid.h"
#include "stager_constants.h"
#include "Cns_api.h"
#include "Cpwd.h"
#include "Cgrp.h"

#include "castor/stager/SubRequestStatusCodes.hpp"
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
      
      /************************************/
      /* handler for the update request  */
      /**********************************/
      void UpdateHandler::handle() throw(castor::exception::Exception)
      {
        // Inherited behavior from RequestHandler, overriding the OpenRequestHandler one
        RequestHandler::handle();
        
        /* check permissions and open the file according to the request type file */
        bool recreate = reqHelper->openNameServerFile() || ((reqHelper->subrequest->flags() & O_TRUNC) == O_TRUNC);

        /* get the castorFile entity and populate its links in the db */
        reqHelper->getCastorFile();

        reqHelper->logToDlf(DLF_LVL_DEBUG, STAGER_UPDATE, &(reqHelper->cnsFileid));
        
        if(recreate) {
          // delegate to Put (may throw exception, which are forwarded)
          PutHandler* h = new PutHandler(reqHelper);
          h->handlePut();
          delete h;
        }
        else {
          // delegate to Get (may throw exception, which are forwarded)
          GetHandler* h = new GetHandler(reqHelper);
          h->handleGet();
          delete h;
        }      
      }
      
    }//end namespace daemon
  }//end namespace stager
}//end namespace castor
