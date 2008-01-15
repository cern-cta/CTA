/***********************************************************************************************************/
/* PrepareToUpdatetHandler: Constructor and implementation of the PrepareToUpdate request handler   */
/* Since it is jobOriented, it uses the mostly part of the JobRequestHandler class                 */
/* Depending if the file exist, it can follow the huge flow (scheduled, as Get) or a small one (as Put) */
/* We need to reply to the client (just in case of error )                                             */
/******************************************************************************************************/



#include "castor/stager/daemon/CnsHelper.hpp"
#include "castor/stager/daemon/ReplyHelper.hpp"

#include "castor/stager/daemon/RequestHandler.hpp"
#include "castor/stager/daemon/JobRequestHandler.hpp"
#include "castor/stager/daemon/PrepareToUpdateHandler.hpp"
#include "castor/stager/daemon/PrepareToPutHandler.hpp"
#include "castor/stager/daemon/PrepareToGetHandler.hpp"

#include "stager_uuid.h"
#include "stager_constants.h"
#include "Cns_api.h"
#include "expert_api.h"

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
      
      PrepareToUpdateHandler::PrepareToUpdateHandler(RequestHelper* stgRequestHelper) throw(castor::exception::Exception) :
        UpdateHandler(stgRequestHelper)
      {
        this->typeRequest = OBJ_StagePrepareToUpdateRequest;
      }

      /*********************************************/
      /* handler for the PrepareToUpdate request  */
      /*******************************************/
      void PrepareToUpdateHandler::handle() throw(castor::exception::Exception)
      {
        stgRequestHelper->logToDlf(DLF_LVL_DEBUG, STAGER_PREPARETOUPDATE, &(stgCnsHelper->cnsFileid));
        
        RequestHandler* h = 0;
        if(recreate) {
          // delegate to PPut
          h = new PrepareToPutHandler(stgRequestHelper, stgCnsHelper);
        } else {
          // delegate to PGet
          h = new PrepareToGetHandler(stgRequestHelper, stgCnsHelper);
        }      
        h->handle();   // may throw exception, just forward it - logging is done in the callee
        delete h;
        stgCnsHelper = 0;   // the delegated handler has already deleted this object
      }
      
      
      
      
    }// end daemon namespace
  }// end stager namespace
}//end castor namespace 

