/*******************************************************************************************************/
/* handler for the SetFileGCWeight request, simply call to the stagerService->setFileGCWeight()       */
/* since it isn't job oriented, it inherits from the RequestHandler                            */
/* it always needs to reply to the client                                                           */
/***************************************************************************************************/

#include "serrno.h"
#include <errno.h>
#include <iostream>
#include <string>

#include "Cupv_api.h"
#include "stager_constants.h"
#include "castor/IObject.hpp"
#include "castor/ObjectSet.hpp"
#include "castor/System.hpp"
#include "castor/stager/IStagerSvc.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/SetFileGCWeight.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/dlf/Dlf.hpp"
#include "castor/dlf/Message.hpp"
#include "castor/stager/daemon/DlfMessages.hpp"

#include "castor/stager/daemon/SetGCWeightHandler.hpp"
#include "castor/stager/daemon/RequestHelper.hpp"
#include "castor/stager/daemon/ReplyHelper.hpp"


namespace castor{
  namespace stager{
    namespace daemon{
            
      void SetGCWeightHandler::handle() throw(castor::exception::Exception)
      {
        RequestHandler::handle();
        
        ReplyHelper* stgReplyHelper = new ReplyHelper();
        
        reqHelper->statNameServerFile(); 
        if(serrno == ENOENT) {
          // user error, file does not exist
          reqHelper->logToDlf(DLF_LVL_USER_ERROR, STAGER_USER_NONFILE, 0);
          castor::exception::Exception ex(serrno);
          throw ex;
        }

        castor::stager::SetFileGCWeight* setGCWeightReq =
          dynamic_cast<castor::stager::SetFileGCWeight*>(reqHelper->fileRequest);
        std::string localHost;
        uid_t euid = setGCWeightReq->euid();
        uid_t egid = setGCWeightReq->egid();
        // Get the name of the localhost to pass into the Cupv interface.
        try {
          localHost = castor::System::getHostName();
        } catch (castor::exception::Exception& e) {
          castor::exception::Exception ex(SEINTERNAL);
          ex.getMessage() << "Failed to determine the name of the localhost";
          throw ex;
        }
        // Check if the user has GRP_ADMIN or ADMIN privileges.
        int rc = Cupv_check(euid, egid, localHost.c_str(), localHost.c_str(),
          (reqHelper->cnsFilestat.gid == egid ? P_GRP_ADMIN : P_ADMIN));
        if ((rc < 0) && (serrno != EACCES)) {
          castor::exception::Exception ex(serrno);
          ex.getMessage() << "Failed Cupv_check call for "
                          << euid << ":" << egid;
          throw ex;
        }
        else if (serrno == EACCES) {
          reqHelper->logToDlf(DLF_LVL_USER_ERROR, STAGER_USER_PERMISSION, &(reqHelper->cnsFileid));
          castor::exception::Exception ex(serrno);
          throw ex;
        }

        try {
          // log the request
          castor::dlf::Param params[] = {
            castor::dlf::Param(reqHelper->subrequestUuid),
            castor::dlf::Param("Type",
                               ((unsigned)reqHelper->fileRequest->type() < castor::ObjectsIdsNb ?
                                castor::ObjectsIdStrings[reqHelper->fileRequest->type()] : "Unknown")),
            castor::dlf::Param("Filename", reqHelper->subrequest->fileName()),
            castor::dlf::Param("Username", reqHelper->username),
            castor::dlf::Param("Groupname", reqHelper->groupname),
            castor::dlf::Param("SvcClass", reqHelper->svcClass->name()),
            castor::dlf::Param("Weight", setGCWeightReq->weight())
          };
          castor::dlf::dlf_writep(reqHelper->requestUuid, DLF_LVL_SYSTEM, STAGER_SETGC,
                                  7, params, &(reqHelper->cnsFileid));
          // execute it
          int rc = reqHelper->stagerService->setFileGCWeight(reqHelper->cnsFileid.fileid, reqHelper->cnsFileid.server,
                                                                    reqHelper->svcClass->id(), setGCWeightReq->weight());
          // this method fails only if no diskCopies were found on the given service class. In such a case we answer the client
          // without involving the Error service.
          reqHelper->subrequest->setStatus(rc ? SUBREQUEST_FINISHED : SUBREQUEST_FAILED_FINISHED);
          
          stgReplyHelper->setAndSendIoResponse(reqHelper, &(reqHelper->cnsFileid),
                                               (rc ? 0 : ENOENT), (rc ? "" : "File not found on this service class"));
          stgReplyHelper->endReplyToClient(reqHelper);
          delete stgReplyHelper;
        }
        catch(castor::exception::Exception& e) {          
          if(stgReplyHelper != NULL) delete stgReplyHelper;
          castor::dlf::Param params[]={
            castor::dlf::Param("Error Code", sstrerror(e.code())),
            castor::dlf::Param("Error Message", e.getMessage().str())
          };
          
          castor::dlf::dlf_writep(reqHelper->requestUuid, DLF_LVL_ERROR, STAGER_SETGC, 2 ,params, &(reqHelper->cnsFileid));
          throw(e); 
        }
      }
      
    }//end namespace daemon
  }//end namespace stager
}//end namespace castor
