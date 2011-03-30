/**********************************************************************************************/
/* PrepareToGetHandler: Constructor and implementation of the get subrequest's handler */
/* It inherits from the OpenRequestHandler and it needs to reply to the client         */
/*******************************************************************************************/

#include "serrno.h"
#include <errno.h>
#include <iostream>
#include <string>

#include "Cns_api.h"
#include "castor/dlf/Dlf.hpp"
#include "castor/dlf/Message.hpp"
#include "castor/stager/DiskCopyForRecall.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/stager/Tape.hpp"

#include "castor/stager/daemon/DlfMessages.hpp"
#include "castor/stager/daemon/RequestHelper.hpp"
#include "castor/stager/daemon/ReplyHelper.hpp"
#include "castor/stager/daemon/PrepareToGetHandler.hpp"


namespace castor{
  namespace stager{
    namespace daemon{
      
      bool PrepareToGetHandler::switchDiskCopiesForJob() throw (castor::exception::Exception)
      {
        bool reply = false;
        int result = reqHelper->stagerService->processPrepareRequest(reqHelper->subrequest);
        switch(result) {
          case -2:                         // this happens for repacks that need to wait on other requests
          reqHelper->subrequest->setStatus(SUBREQUEST_WAITSUBREQ);
          reqHelper->logToDlf(DLF_LVL_SYSTEM, STAGER_WAITSUBREQ, &(reqHelper->cnsFileid));
          reply = true;
          break;
          
          case -1:
          reqHelper->logToDlf(DLF_LVL_USER_ERROR, STAGER_UNABLETOPERFORM, &(reqHelper->cnsFileid));
          break;
          
          case DISKCOPY_STAGED:             // nothing to do, just log it
          reqHelper->subrequest->setStatus(SUBREQUEST_FINISHED);
          reqHelper->logToDlf(DLF_LVL_SYSTEM, STAGER_ARCHIVE_SUBREQ, &(reqHelper->cnsFileid));
          reply = true;
          break;
          
          case DISKCOPY_WAITDISK2DISKCOPY:    // nothing to do (the db is already updated), log that a replication is about to happen
          reqHelper->subrequest->setStatus(SUBREQUEST_WAITSUBREQ);
          reqHelper->logToDlf(DLF_LVL_SYSTEM, STAGER_PREPARETOGET_DISK2DISKCOPY, &(reqHelper->cnsFileid));
          reply = true;
          break;
          
          case DISKCOPY_WAITTAPERECALL:   // trigger a recall
          {
            // reset the filesize to the nameserver one, as we don't have anything in the db
            reqHelper->subrequest->castorFile()->setFileSize(reqHelper->cnsFilestat.filesize);
            // first check the special case of 0 bytes files
            if (0 == reqHelper->cnsFilestat.filesize) {
              reqHelper->stagerService->createEmptyFile(reqHelper->subrequest, false);
              reqHelper->subrequest->setStatus(SUBREQUEST_FINISHED);
              reply = true;
              break;
            }
            // answer client only if success
            castor::stager::Tape *tape = 0;
            reply = reqHelper->stagerService->createRecallCandidate(reqHelper->subrequest, reqHelper->svcClass, tape);
            if (reply) {
              // "Triggering Tape Recall"
              castor::dlf::Param params[] = {
                castor::dlf::Param("Type", castor::ObjectsIdStrings[reqHelper->fileRequest->type()]),
                castor::dlf::Param("Filename", reqHelper->subrequest->fileName()),
                castor::dlf::Param("Username", reqHelper->username),
                castor::dlf::Param("Groupname", reqHelper->groupname),
                castor::dlf::Param("SvcClass", reqHelper->svcClass->name()),
                castor::dlf::Param("TPVID", tape->vid()),
                castor::dlf::Param("TapeStatus", castor::stager::TapeStatusCodesStrings[tape->status()]),
                castor::dlf::Param("FileSize", reqHelper->subrequest->castorFile()->fileSize()),
              castor::dlf::Param(reqHelper->subrequestUuid)};
              castor::dlf::dlf_writep(reqHelper->requestUuid, DLF_LVL_SYSTEM, STAGER_TAPE_RECALL, 9, params, &(reqHelper->cnsFileid));
            } else {
              // no tape copy found because of Tape0 file, log it
              // any other tape error will throw an exception and will be classified as LVL_ERROR 
              reqHelper->logToDlf(DLF_LVL_USER_ERROR, STAGER_UNABLETOPERFORM, &(reqHelper->cnsFileid));
            }
            if (tape != 0)
              delete tape;
          }
          break;
        }
        return reply;
      }
      
      void PrepareToGetHandler::handle() throw(castor::exception::Exception)
      {
        OpenRequestHandler::handle();
        
        handleGet();
      }
      
      
      void PrepareToGetHandler::handleGet() throw(castor::exception::Exception)
      {        
        ReplyHelper* stgReplyHelper = NULL;
        castor::stager::DiskCopyInfo *diskCopy = NULL;
        try{
          
          reqHelper->logToDlf(DLF_LVL_DEBUG, STAGER_PREPARETOGET, &(reqHelper->cnsFileid));
          
          if(switchDiskCopiesForJob()) {
            if(reqHelper->subrequest->answered() == 0) {
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
              stgReplyHelper = 0;
              if (diskCopy)
                delete diskCopy;
            }
            else {
              // no reply needs to be sent to the client, archive the subrequest
              reqHelper->stagerService->archiveSubReq(reqHelper->subrequest->id(), SUBREQUEST_FINISHED);
            }
          }
        }
        catch(castor::exception::Exception& e){
          
          /* since if an error happens we are gonna reply to the client(and internally, update subreq on DB)*/
          /* we don t execute: dbSvc->updateRep ..*/
          if(stgReplyHelper != NULL) delete stgReplyHelper;
          if(diskCopy != NULL) delete diskCopy;
          
          if(e.getMessage().str().empty()) {
            e.getMessage() << sstrerror(e.code());
          }
          castor::dlf::Param params[] = {
            castor::dlf::Param("Error Code", e.code()),
          castor::dlf::Param("Error Message", e.getMessage().str())};
          castor::dlf::dlf_writep(reqHelper->requestUuid, DLF_LVL_ERROR, STAGER_PREPARETOGET, 2, params, &(reqHelper->cnsFileid));
          throw(e);
        }        
        
      }
      
      
    }//end namespace dbservice
  }//end namespace stager
}//end namespace castor
