/***********************************************************************************************************/
/* StagerPrepareToUpdatetHandler: Constructor and implementation of the PrepareToUpdate request handler   */
/* Since it is jobOriented, it uses the mostly part of the StagerJobRequestHandler class                 */
/* Depending if the file exist, it can follow the huge flow (scheduled, as Get) or a small one (as Put) */
/* We need to reply to the client (just in case of error )                                             */
/******************************************************************************************************/



#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"

#include "castor/stager/dbService/StagerRequestHandler.hpp"
#include "castor/stager/dbService/StagerJobRequestHandler.hpp"
#include "castor/stager/dbService/StagerPrepareToUpdateHandler.hpp"
#include "castor/stager/dbService/StagerPrepareToPutHandler.hpp"
#include "castor/stager/dbService/StagerPrepareToGetHandler.hpp"

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
#include "castor/stager/dbService/StagerDlfMessages.hpp"

#include "serrno.h"
#include <errno.h>

#include <iostream>
#include <string>



namespace castor{
  namespace stager{
    namespace dbService{
      
      StagerPrepareToUpdateHandler::StagerPrepareToUpdateHandler(StagerRequestHelper* stgRequestHelper) throw(castor::exception::Exception) :
        StagerUpdateHandler(stgRequestHelper)
      {
        this->typeRequest = OBJ_StagePrepareToUpdateRequest;
      }

      /*********************************************/
      /* handler for the PrepareToUpdate request  */
      /*******************************************/
      void StagerPrepareToUpdateHandler::handle() throw(castor::exception::Exception)
      {
        stgRequestHelper->logToDlf(DLF_LVL_DEBUG, STAGER_PREPARETOUPDATE, &(stgCnsHelper->cnsFileid));
        
        StagerRequestHandler* h = 0;
        if(recreate) {
          // delegate to PPut
          h = new StagerPrepareToPutHandler(stgRequestHelper, stgCnsHelper);
        } else {
          // delegate to PGet
          h = new StagerPrepareToGetHandler(stgRequestHelper, stgCnsHelper);
        }      
        h->handle();   // may throw exception, just forward it - logging is done in the callee
        delete h;
        stgCnsHelper = 0;   // the delegated handler has already deleted this object
      }
      
      
      
      
    }// end dbService namespace
  }// end stager namespace
}//end castor namespace 

