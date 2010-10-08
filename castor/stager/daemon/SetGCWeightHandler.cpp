/*******************************************************************************************************/
/* handler for the SetFileGCWeight request, simply call to the stagerService->setFileGCWeight()       */
/* since it isn't job oriented, it inherits from the RequestHandler                            */
/* it always needs to reply to the client                                                           */
/***************************************************************************************************/

#include "castor/stager/daemon/SetGCWeightHandler.hpp"

#include "castor/stager/daemon/RequestHelper.hpp"
#include "castor/stager/daemon/CnsHelper.hpp"
#include "castor/stager/daemon/ReplyHelper.hpp"

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
#include "castor/stager/daemon/DlfMessages.hpp"


#include "serrno.h"
#include <errno.h>
#include <iostream>
#include <string>



namespace castor{
  namespace stager{
    namespace daemon{
      
      SetGCWeightHandler::SetGCWeightHandler(RequestHelper* stgRequestHelper) throw(castor::exception::Exception)
      {
        this->stgRequestHelper = stgRequestHelper;
        this->typeRequest = OBJ_SetFileGCWeight;
      }
      
      void SetGCWeightHandler::handle() throw(castor::exception::Exception)
      {
        ReplyHelper* stgReplyHelper=NULL;
        try {
          // extract actual request
          castor::stager::SetFileGCWeight* setGCWeightReq =
            dynamic_cast<castor::stager::SetFileGCWeight*>(stgRequestHelper->fileRequest);
          // log the request
          castor::dlf::Param params[] = {
            castor::dlf::Param(stgRequestHelper->subrequestUuid),
            castor::dlf::Param("Type",
                               ((unsigned)stgRequestHelper->fileRequest->type() < castor::ObjectsIdsNb ?
                                castor::ObjectsIdStrings[stgRequestHelper->fileRequest->type()] : "Unknown")),
            castor::dlf::Param("Filename", stgRequestHelper->subrequest->fileName()),
            castor::dlf::Param("Username", stgRequestHelper->username),
            castor::dlf::Param("Groupname", stgRequestHelper->groupname),
            castor::dlf::Param("SvcClass", stgRequestHelper->svcClass->name()),
	    castor::dlf::Param("Weight", setGCWeightReq->weight())
          };
          castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_SYSTEM, STAGER_SETGC,
                                  7, params, &(stgCnsHelper->cnsFileid));
          // execute it
          int rc = stgRequestHelper->stagerService->setFileGCWeight(stgCnsHelper->cnsFileid.fileid, stgCnsHelper->cnsFileid.server,
                                                                    stgRequestHelper->svcClass->id(), setGCWeightReq->weight());
          // this method fails only if no diskCopies were found on the given service class. In such a case we answer the client
          // without involving the Error service.
          stgRequestHelper->subrequest->setStatus(rc ? SUBREQUEST_FINISHED : SUBREQUEST_FAILED_FINISHED);
          
          stgReplyHelper = new ReplyHelper();
          stgReplyHelper->setAndSendIoResponse(stgRequestHelper, &(stgCnsHelper->cnsFileid),
                                               (rc ? 0 : ENOENT), (rc ? "" : "File not on this service class"));
          stgReplyHelper->endReplyToClient(stgRequestHelper);
          delete stgReplyHelper;
          stgReplyHelper = 0;
        }
        catch(castor::exception::Exception& e) {          
          if(stgReplyHelper != NULL) delete stgReplyHelper;
          castor::dlf::Param params[]={
            castor::dlf::Param("Error Code", sstrerror(e.code())),
            castor::dlf::Param("Error Message", e.getMessage().str())
          };
          
          castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_ERROR, STAGER_SETGC, 2 ,params, &(stgCnsHelper->cnsFileid));
          throw(e); 
          
        }
        
      }
      
      
      
      SetGCWeightHandler::~SetGCWeightHandler() throw()
      {
      }
      
    }//end namespace daemon
  }//end namespace stager
}//end namespace castor
