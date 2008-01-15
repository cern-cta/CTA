/*******************************************************************************************************/
/* handler for the SetFileGCWeight request, simply call to the stagerService->setFileGCWeight()       */
/* since it isn't job oriented, it inherits from the StagerRequestHandler                            */
/* it always needs to reply to the client                                                           */
/***************************************************************************************************/

#include "castor/stager/daemon/StagerSetGCHandler.hpp"

#include "castor/stager/daemon/StagerRequestHelper.hpp"
#include "castor/stager/daemon/StagerCnsHelper.hpp"
#include "castor/stager/daemon/StagerReplyHelper.hpp"

#include "stager_constants.h"
#include "castor/stager/SetFileGCWeight.hpp"

#include "castor/IObject.hpp"
#include "castor/ObjectSet.hpp"
#include "castor/stager/IStagerSvc.hpp"
#include "castor/stager/SubRequest.hpp"

#include "castor/stager/FileRequest.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"

#include "castor/exception/Exception.hpp"

#include "castor/dlf/Dlf.hpp"
#include "castor/dlf/Message.hpp"
#include "castor/stager/daemon/StagerDlfMessages.hpp"


#include "serrno.h"
#include <errno.h>
#include <iostream>
#include <string>



namespace castor{
  namespace stager{
    namespace daemon{
      
      StagerSetGCHandler::StagerSetGCHandler(StagerRequestHelper* stgRequestHelper) throw(castor::exception::Exception)
      {
        this->stgRequestHelper = stgRequestHelper;
        this->typeRequest = OBJ_SetFileGCWeight;
      }
      
      void StagerSetGCHandler::handle() throw(castor::exception::Exception)
      {
        
        StagerReplyHelper* stgReplyHelper=NULL;
        try{
          
          stgRequestHelper->logToDlf(DLF_LVL_SYSTEM, STAGER_SETGC, &(stgCnsHelper->cnsFileid));
          
          /* execute the main function for the setFileGCWeight request   */
          /* basically a call to the corresponding stagerService method */
          /* passing the SetFileGCWeight object                        */
          castor::stager::SetFileGCWeight* setGCWeightReq = dynamic_cast<castor::stager::SetFileGCWeight*>(stgRequestHelper->fileRequest);
          stgRequestHelper->stagerService->setFileGCWeight(stgCnsHelper->cnsFileid.fileid, stgCnsHelper->cnsFileid.server, setGCWeightReq->weight());
          
          stgRequestHelper->subrequest->setStatus(SUBREQUEST_ARCHIVED);
          
          stgReplyHelper = new StagerReplyHelper();
          stgReplyHelper->setAndSendIoResponse(stgRequestHelper,&(stgCnsHelper->cnsFileid), 0, "No error");
          stgReplyHelper->endReplyToClient(stgRequestHelper);
          delete stgReplyHelper;
          
          stgRequestHelper->stagerService->archiveSubReq(stgRequestHelper->subrequest->id());	 
        }catch(castor::exception::Exception e){
          
          if(stgReplyHelper != NULL) delete stgReplyHelper;
          castor::dlf::Param params[]={castor::dlf::Param("Error Code",sstrerror(e.code())),
            castor::dlf::Param("Error Message",e.getMessage().str())
          };
          
          castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_ERROR, STAGER_UPDATE, 2 ,params, &(stgCnsHelper->cnsFileid));
          throw(e); 
          
        }
        
      }
      
      
      
      StagerSetGCHandler::~StagerSetGCHandler() throw()
      {
      }
      
    }//end namespace daemon
  }//end namespace stager
}//end namespace castor
