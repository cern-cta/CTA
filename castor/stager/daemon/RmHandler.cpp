
/************************************************************************************/
/* handler for the Rm subrequest, simply call to the stagerService->stageRm()      */
/* since it isn' t job oriented, it inherits from the RequestHandler        */
/* it always needs to reply to the client                                        */
/********************************************************************************/
#include "serrno.h"
#include <errno.h>
#include <iostream>
#include <string>

#include "castor/stager/daemon/RmHandler.hpp"
#include "castor/stager/daemon/OpenRequestHandler.hpp"
#include "castor/stager/daemon/RequestHelper.hpp"
#include "castor/stager/daemon/ReplyHelper.hpp"

#include "stager_constants.h"
#include "castor/stager/IStagerSvc.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/dlf/Dlf.hpp"
#include "castor/dlf/Message.hpp"
#include "castor/stager/daemon/DlfMessages.hpp"


namespace castor{
  namespace stager{
    namespace daemon{


      void RmHandler::handle() throw(castor::exception::Exception)
      {
        // We completely override the inherited behavior
        // set the username and groupname for logging
        reqHelper->setUsernameAndGroupname();
        
        // stat the file in the nameserver
        reqHelper->statNameServerFile();
        
        if(serrno != ENOENT) {
          // Check if the user (euid,egid) has the right permission for the rm, otherwise throw exception
          std::string dirName = reqHelper->subrequest->fileName();
          dirName = dirName.substr(0, dirName.rfind('/'));
          if(0 != Cns_accessUser(dirName.c_str(), W_OK,
             reqHelper->fileRequest->euid(), reqHelper->fileRequest->egid())) {
            reqHelper->logToDlf(DLF_LVL_USER_ERROR, STAGER_USER_PERMISSION, &(reqHelper->cnsFileid));
            castor::exception::Exception ex(serrno);
            throw ex;
          }
        }
        // else the file does not exist (any other error is caught in statNameServerFile()),
        // go on and try to cleanup stager db. Note that in this case we override the permission check
        // but that's fine as the cleanup would anyway need to be done.

        // check the service class and handle the '*' case;
        // the existence of the service class is guaranteed by the RH
        u_signed64 svcClassId = 0;
        if(reqHelper->fileRequest->svcClassName() != "*") {
          reqHelper->dbSvc->fillObj(reqHelper->baseAddr, reqHelper->fileRequest, castor::OBJ_SvcClass, false);
          svcClassId = reqHelper->fileRequest->svcClass()->id();
        }

        ReplyHelper* stgReplyHelper=NULL;
        try {
          if (reqHelper->cnsFileid.fileid > 0) {
            // try to perform the stageRm; internally, the method checks for non existing files
            if(reqHelper->stagerService->stageRm(reqHelper->subrequest, reqHelper->cnsFileid.fileid,
                                                 reqHelper->cnsFileid.server, svcClassId)) {
              reqHelper->subrequest->setStatus(SUBREQUEST_FINISHED);
              stgReplyHelper = new ReplyHelper();
              stgReplyHelper->setAndSendIoResponse(reqHelper,&(reqHelper->cnsFileid), 0, "No error");
              stgReplyHelper->endReplyToClient(reqHelper);
              reqHelper->logToDlf(DLF_LVL_SYSTEM, STAGER_RMPERFORMED, &(reqHelper->cnsFileid));
              delete stgReplyHelper;
              stgReplyHelper = NULL;
            } else {
              // user error, log it only in case the file existed
              // Otherwise, it is an internal double check that failed and this is not really relevant
              if (reqHelper->cnsFileid.fileid > 0) {
                reqHelper->logToDlf(DLF_LVL_USER_ERROR, STAGER_UNABLETOPERFORM, &(reqHelper->cnsFileid));
              }
            }
          } else {
            // the file does not exist, but we may have to cleanup the DB in case it got renamed
            reqHelper->stagerService->renamedFileCleanup(reqHelper->subrequest->fileName(),
                                                         reqHelper->subrequest->id());
          }
        }
        catch(castor::exception::Exception& e){
          if(stgReplyHelper != NULL) {
            delete stgReplyHelper;
            stgReplyHelper = NULL;
          }
          castor::dlf::Param params[] = {
            castor::dlf::Param("Error Code",sstrerror(e.code())),
            castor::dlf::Param("Error Message",e.getMessage().str())};
          castor::dlf::dlf_writep(reqHelper->requestUuid, DLF_LVL_ERROR, STAGER_RM, 2, params, &(reqHelper->cnsFileid));
          throw(e);
        }
      }

    }//end daemon
  }//end stager
}//end castor
