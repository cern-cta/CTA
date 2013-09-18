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

      bool PrepareToUpdateHandler::handle() throw(castor::exception::Exception) {
        // call parent's handler method, check permissions and open the file according to the requested flags
        bool recreate = OpenRequestHandler::handle() || ((reqHelper->subrequest->flags() & O_TRUNC) == O_TRUNC);
        /* get the castorFile entity and populate its links in the db */
        reqHelper->getCastorFile();
        reqHelper->logToDlf(DLF_LVL_DEBUG, STAGER_PREPARETOUPDATE, &(reqHelper->cnsFileid));
        if(recreate) {
          // call PL/SQL handlePrepareToPut method
          ReplyHelper* stgReplyHelper = NULL;
          try {
            bool replyNeeded = reqHelper->stagerService->handlePrepareToPut(reqHelper->castorFile->id(),
                                                                            reqHelper->subrequest->id(),
                                                                            reqHelper->cnsFileid,
                                                                            reqHelper->m_stagerOpenTimeInUsec);
            if (replyNeeded) {
              /* we are gonna replyToClient so we dont  updateRep on DB explicitly */
              reqHelper->subrequest->setStatus(SUBREQUEST_READY);
              stgReplyHelper = new ReplyHelper();
              stgReplyHelper->setAndSendIoResponse(reqHelper,&(reqHelper->cnsFileid), 0, "No Error");
              stgReplyHelper->endReplyToClient(reqHelper);
              delete stgReplyHelper;
            }
          } catch (oracle::occi::SQLException e) {
            if (stgReplyHelper != NULL) delete stgReplyHelper;
            throw(e);
          }          
        } else {
          ReplyHelper* stgReplyHelper = NULL;
          castor::stager::DiskCopyInfo *diskCopy = NULL;
          try {
            castor::stager::SubRequestStatusCodes srStatus =
              reqHelper->stagerService->handlePrepareToGet(reqHelper->castorFile->id(),
                                                           reqHelper->subrequest->id(),
                                                           reqHelper->cnsFileid,
                                                           reqHelper->cnsFilestat.filesize,
                                                           reqHelper->m_stagerOpenTimeInUsec);
            reqHelper->subrequest->setStatus(srStatus);
            if (srStatus == SUBREQUEST_WAITTAPERECALL) { 
              // reset the filesize to the nameserver one, as we don't have anything in the db
              reqHelper->subrequest->castorFile()->setFileSize(reqHelper->cnsFilestat.filesize);
            }
            // answer client if needed
            if (srStatus != SUBREQUEST_FAILED) {
              if (reqHelper->subrequest->answered() == 0) {
                stgReplyHelper = new ReplyHelper();
                if(reqHelper->subrequest->protocol() == "xroot") {
                  diskCopy = reqHelper->stagerService->
                    getBestDiskCopyToRead(reqHelper->subrequest->castorFile(),
                                          reqHelper->svcClass);
                }
                if (diskCopy) {
                  stgReplyHelper->setAndSendIoResponse(reqHelper,&(reqHelper->cnsFileid), 0, "", diskCopy);
                } else {              
                  stgReplyHelper->setAndSendIoResponse(reqHelper,&(reqHelper->cnsFileid), 0, "");
                }
                stgReplyHelper->endReplyToClient(reqHelper);
                delete stgReplyHelper;
                if (diskCopy) delete diskCopy;
              } else {
                // no reply needs to be sent to the client, archive the subrequest
                reqHelper->stagerService->archiveSubReq(reqHelper->subrequest->id(), SUBREQUEST_FINISHED);
              }
            }
          } catch(castor::exception::Exception& e) {
            if (stgReplyHelper != NULL) delete stgReplyHelper;
            if (diskCopy != NULL) delete diskCopy;
            throw(e);
          }        
        }
        return true;
      }
      
    }// end daemon namespace
  }// end stager namespace
}//end castor namespace 
