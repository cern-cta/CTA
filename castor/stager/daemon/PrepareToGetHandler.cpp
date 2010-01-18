/**********************************************************************************************/
/* PrepareToGetHandler: Constructor and implementation of the get subrequest's handler */
/* It inherits from the JobRequestHandler and it needs to reply to the client         */
/*******************************************************************************************/


#include "castor/stager/daemon/RequestHelper.hpp"
#include "castor/stager/daemon/CnsHelper.hpp"
#include "castor/stager/daemon/ReplyHelper.hpp"
#include "castor/stager/daemon/RequestHandler.hpp"
#include "castor/stager/daemon/JobRequestHandler.hpp"
#include "castor/stager/daemon/PrepareToGetHandler.hpp"

#include "stager_uuid.h"
#include "stager_constants.h"
#include "Cns_api.h"
#include "expert_api.h"
#include "Cpwd.h"
#include "Cgrp.h"

#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
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
      
      PrepareToGetHandler::PrepareToGetHandler(RequestHelper* stgRequestHelper, CnsHelper* stgCnsHelper) throw(castor::exception::Exception)
      {
        this->stgRequestHelper = stgRequestHelper;
        this->stgCnsHelper = stgCnsHelper;
        this->typeRequest = OBJ_StagePrepareToGetRequest;
      }
      
      /*******************************************************************/
      /* function to set the handler's attributes according to its type */
      /*****************************************************************/
      void PrepareToGetHandler::handlerSettings() throw(castor::exception::Exception)
      {	
        this->maxReplicaNb= this->stgRequestHelper->svcClass->maxReplicaNb();
      }
      
      
      bool PrepareToGetHandler::switchDiskCopiesForJob() throw (castor::exception::Exception)
      {
        bool reply = false;
        int result = stgRequestHelper->stagerService->processPrepareRequest(stgRequestHelper->subrequest);
        switch(result) {
          case -2:                         // this happens for repacks that need to wait on other requests
          stgRequestHelper->subrequest->setStatus(SUBREQUEST_WAITSUBREQ);
          stgRequestHelper->logToDlf(DLF_LVL_SYSTEM, STAGER_WAITSUBREQ, &(stgCnsHelper->cnsFileid));
          reply = true;
          break;
          
          case -1:
          stgRequestHelper->logToDlf(DLF_LVL_USER_ERROR, STAGER_UNABLETOPERFORM, &(stgCnsHelper->cnsFileid));
          break;
          
          case DISKCOPY_STAGED:             // nothing to do, just log it
          stgRequestHelper->subrequest->setStatus(SUBREQUEST_FINISHED);
          stgRequestHelper->logToDlf(DLF_LVL_SYSTEM, STAGER_ARCHIVE_SUBREQ, &(stgCnsHelper->cnsFileid));
          reply = true;
          break;
          
          case DISKCOPY_WAITDISK2DISKCOPY:    // nothing to do (the db is already updated), log that a replication is about to happen
          stgRequestHelper->subrequest->setStatus(SUBREQUEST_WAITSUBREQ);
          stgRequestHelper->logToDlf(DLF_LVL_SYSTEM, STAGER_PREPARETOGET_DISK2DISKCOPY, &(stgCnsHelper->cnsFileid));
          reply = true;
          break;
          
          case DISKCOPY_WAITTAPERECALL:   // trigger a recall
          {
            // reset the filesize to the nameserver one, as we don't have anything in the db
            stgRequestHelper->subrequest->castorFile()->setFileSize(stgCnsHelper->cnsFilestat.filesize);
            // first check the special case of 0 bytes files
            if (0 == stgCnsHelper->cnsFilestat.filesize) {
              stgRequestHelper->stagerService->createEmptyFile(stgRequestHelper->subrequest, false);
              stgRequestHelper->subrequest->setStatus(SUBREQUEST_FINISHED);
              reply = true;
              break;
            }
            // answer client only if success
            castor::stager::Tape *tape = 0;
            reply = stgRequestHelper->stagerService->createRecallCandidate(stgRequestHelper->subrequest, stgRequestHelper->svcClass, tape);
            if (reply) {
              // "Triggering Tape Recall"
              castor::dlf::Param params[] = {
                castor::dlf::Param("Type", castor::ObjectsIdStrings[stgRequestHelper->fileRequest->type()]),
                castor::dlf::Param("Filename", stgRequestHelper->subrequest->fileName()),
                castor::dlf::Param("Username", stgRequestHelper->username),
                castor::dlf::Param("Groupname", stgRequestHelper->groupname),
                castor::dlf::Param("SvcClass", stgRequestHelper->svcClass->name()),
                castor::dlf::Param("TPVID", tape->vid()),
                castor::dlf::Param("TapeStatus", castor::stager::TapeStatusCodesStrings[tape->status()]),
                castor::dlf::Param("FileSize", stgRequestHelper->subrequest->castorFile()->fileSize()),
              castor::dlf::Param(stgRequestHelper->subrequestUuid)};
              castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_SYSTEM, STAGER_TAPE_RECALL, 9, params, &(stgCnsHelper->cnsFileid));
            } else {
              // no tape copy found because of Tape0 file, log it
              // any other tape error will throw an exception and will be classified as LVL_ERROR 
              stgRequestHelper->logToDlf(DLF_LVL_USER_ERROR, STAGER_UNABLETOPERFORM, &(stgCnsHelper->cnsFileid));
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
        
        ReplyHelper* stgReplyHelper = NULL;
        castor::stager::DiskCopyInfo *diskCopy = NULL;
        try{
          handlerSettings();
          
          stgRequestHelper->logToDlf(DLF_LVL_DEBUG, STAGER_PREPARETOGET, &(stgCnsHelper->cnsFileid));
          jobOriented();	  
          
          if(switchDiskCopiesForJob()) {
            if(stgRequestHelper->subrequest->answered() == 0) {
              stgReplyHelper = new ReplyHelper();
              if(stgRequestHelper->subrequest->protocol() == "xroot") {
                diskCopy = stgRequestHelper->stagerService->
                  getBestDiskCopyToRead(stgRequestHelper->subrequest->castorFile(),
					                              stgRequestHelper->svcClass);
              }
              if (diskCopy) {
                stgReplyHelper->setAndSendIoResponse(stgRequestHelper,&(stgCnsHelper->cnsFileid), 0, "", diskCopy);
              } else {              
                stgReplyHelper->setAndSendIoResponse(stgRequestHelper,&(stgCnsHelper->cnsFileid), 0, "");
              }
              stgReplyHelper->endReplyToClient(stgRequestHelper);
            
              delete stgReplyHelper;
              stgReplyHelper = 0;
              if (diskCopy)
                delete diskCopy;
            }
            else {
              // no reply needs to be sent to the client, archive the subrequest
              stgRequestHelper->stagerService->archiveSubReq(stgRequestHelper->subrequest->id(), SUBREQUEST_FINISHED);
            }
          }
        }
        catch(castor::exception::Exception e){
          
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
          castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_ERROR, STAGER_PREPARETOGET, 2, params, &(stgCnsHelper->cnsFileid));
          throw(e);
        }        
        
      }
      
      
      /***********************************************************************/
      /*    destructor                                                      */
      PrepareToGetHandler::~PrepareToGetHandler() throw()
      {
        
      }
      
    }//end namespace dbservice
  }//end namespace stager
}//end namespace castor
