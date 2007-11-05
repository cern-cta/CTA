/*********************************************************************************************************/
/* cpp version of the "stager_db_service.c" represented by a thread calling the right request's handler */
/*******************************************************************************************************/

#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"
#include "castor/stager/dbService/StagerRequestHandler.hpp"
#include "castor/stager/dbService/StagerJobRequestHandler.hpp"
#include "castor/stager/dbService/JobRequestSvc.hpp"

#include "castor/stager/dbService/StagerGetHandler.hpp"
#include "castor/stager/dbService/StagerPutHandler.hpp"
#include "castor/stager/dbService/StagerUpdateHandler.hpp"

#include "castor/stager/dbService/RequestSvc.hpp"
#include "castor/BaseObject.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"

#include "stager_constants.h"
#include "Cns_api.h"
#include "expert_api.h"
#include "serrno.h"

#include "dlf_api.h"
#include "castor/dlf/Dlf.hpp"
#include "castor/dlf/Param.hpp"

#include "osdep.h"
#include "Cnetdb.h"
#include "Cpwd.h"
#include "Cgrp.h"
#include "stager_uuid.h"
#include "Cuuid.h"
#include "u64subr.h"
#include "marshall.h"
#include "net.h"

#include "castor/IAddress.hpp"
#include "castor/IObject.hpp"
#include "castor/Services.hpp"
#include "castor/stager/IStagerSvc.hpp"
#include "castor/BaseObject.hpp"
#include "castor/server/BaseServer.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/Constants.hpp"

#include "castor/exception/Exception.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"


#include "serrno.h"
#include <errno.h>
#include <iostream>
#include <string>


namespace castor{
  namespace stager{
    namespace dbService{
      
      
      /****************/
      /* constructor */
      /**************/
      JobRequestSvc::JobRequestSvc(std::string jobManagerHost, int jobManagerPort) throw() :
      RequestSvc("JobReqSvc"), m_jobManagerHost(jobManagerHost), m_jobManagerPort(jobManagerPort)
      {
      }
      

      void JobRequestSvc::process(castor::IObject* subRequestToProcess) throw () {
        StagerCnsHelper* stgCnsHelper= NULL;
        StagerRequestHelper* stgRequestHelper= NULL;
        StagerJobRequestHandler* stgRequestHandler = NULL;
        
        try {
          stgCnsHelper = new StagerCnsHelper();
          int typeRequest=0;
          stgRequestHelper = new StagerRequestHelper(dynamic_cast<castor::stager::SubRequest*>(subRequestToProcess), typeRequest);

          switch(typeRequest){
            
            case OBJ_StageGetRequest:
              stgRequestHandler = new StagerGetHandler(stgRequestHelper, stgCnsHelper);
              break;
            
            case OBJ_StagePutRequest:
              stgRequestHandler = new StagerPutHandler(stgRequestHelper, stgCnsHelper);
              break;
            
            case OBJ_StageUpdateRequest:
              stgRequestHandler = new StagerUpdateHandler(stgRequestHelper, stgCnsHelper);
              break;
            
          }// end switch(typeRequest)
          
          /**********************************************/
          /* inside the handle(), call to preHandle() */
          
          stgRequestHandler->handle();
          
          if (stgRequestHandler->notifyJobManager()) {
            castor::server::BaseServer::sendNotification(m_jobManagerHost, m_jobManagerPort, 1);
          }
          
          delete stgRequestHandler;
          
          if(stgRequestHelper != NULL){
            if(stgRequestHelper->baseAddr) delete stgRequestHelper->baseAddr;
            delete stgRequestHelper;
          }
          
          if(stgCnsHelper) delete stgCnsHelper;
          
          
        }catch(castor::exception::Exception ex){ /* process the exception an replyToClient */
          
          handleException(stgRequestHelper,stgCnsHelper, ex.code(), ex.getMessage().str());
          
          /* we delete our objects */
          if(stgRequestHandler) delete stgRequestHandler;	  
          if(stgRequestHelper != NULL){
            if(stgRequestHelper->baseAddr) delete stgRequestHelper->baseAddr;
            delete stgRequestHelper;
          }	  
          if(stgCnsHelper) delete stgCnsHelper;
        }
        
      }/* end JobRequestSvc::process */
      
      
    }//end namespace dbService
  }//end namespace stager
}//end namespace castor
















