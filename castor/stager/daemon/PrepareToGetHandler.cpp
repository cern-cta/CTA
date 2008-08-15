/**********************************************************************************************/
/* PrepareToGetHandler: Constructor and implementation of the get subrequest's handler */
/* It inherits from the JobRequestHandler and it needs to reply to the client         */
/*******************************************************************************************/


#include "castor/stager/daemon/RequestHelper.hpp"
#include "castor/stager/daemon/CnsHelper.hpp"
#include "ReplyHelper.hpp"

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
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"
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
        this->replicationPolicy = this->stgRequestHelper->svcClass->replicationPolicy();
      }
      
      
      bool PrepareToGetHandler::switchDiskCopiesForJob() throw (castor::exception::Exception)
      {
        bool reply = false;
        int result = stgRequestHelper->stagerService->processPrepareRequest(stgRequestHelper->subrequest);
        switch(result) {
          case -2:
          stgRequestHelper->logToDlf(DLF_LVL_SYSTEM, STAGER_WAITSUBREQ, &(stgCnsHelper->cnsFileid));
          stgRequestHelper->subrequest->setStatus(SUBREQUEST_WAITSUBREQ);
          reply = true;
          break;
          
          case -1:
          stgRequestHelper->logToDlf(DLF_LVL_USER_ERROR, STAGER_UNABLETOPERFORM, &(stgCnsHelper->cnsFileid));
          break;
          
          case DISKCOPY_STAGED:             // nothing to do, just log it
          case DISKCOPY_WAITDISK2DISKCOPY:
          stgRequestHelper->subrequest->setStatus(SUBREQUEST_ARCHIVED);
          stgRequestHelper->subrequest->setGetNextStatus(GETNEXTSTATUS_FILESTAGED);
          if(result == DISKCOPY_STAGED)
            stgRequestHelper->logToDlf(DLF_LVL_SYSTEM, STAGER_ARCHIVE_SUBREQ, &(stgCnsHelper->cnsFileid));
          else
            stgRequestHelper->logToDlf(DLF_LVL_SYSTEM, STAGER_PREPARETOGET_DISK2DISKCOPY, &(stgCnsHelper->cnsFileid));
          
          /* we archive the subrequest */
          stgRequestHelper->stagerService->archiveSubReq(stgRequestHelper->subrequest->id());
          reply = true;
          break;
          
          case DISKCOPY_WAITTAPERECALL:   // trigger a recall
          {
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
                castor::dlf::Param("SvcClass", stgRequestHelper->svcClassName),
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
        
        ReplyHelper* stgReplyHelper=NULL;
        try{
          handlerSettings();
          
          stgRequestHelper->logToDlf(DLF_LVL_DEBUG, STAGER_PREPARETOGET, &(stgCnsHelper->cnsFileid));
          jobOriented();	  
          
          /* depending on the value returned by getDiskCopiesForJob */
          /* if needed, we update the subrequestStatus internally  */
          if(switchDiskCopiesForJob()) {
            if(stgRequestHelper->subrequest->answered() == 0) {
              stgReplyHelper = new ReplyHelper();
              if ( stgRequestHelper->subrequest->protocol() == "xroot"){
                std::string filepath = stgRequestHelper->stagerService->selectPhysicalFileName(&(stgCnsHelper->cnsFileid), stgRequestHelper->svcClass);
                stgReplyHelper->setAndSendIoResponse(stgRequestHelper,&(stgCnsHelper->cnsFileid), 0, "",filepath);
              }else{              
                stgReplyHelper->setAndSendIoResponse(stgRequestHelper,&(stgCnsHelper->cnsFileid), 0, "");
              }
              stgReplyHelper->endReplyToClient(stgRequestHelper);
            
              delete stgReplyHelper;
              stgReplyHelper = 0;
            }
            else {
              // no reply needs to be sent to the client, hence update subRequest and commit the db transaction
              stgRequestHelper->dbSvc->updateRep(stgRequestHelper->baseAddr, stgRequestHelper->subrequest, true);
            }
          }	  
          
        }catch(castor::exception::Exception e){
          
          /* since if an error happens we are gonna reply to the client(and internally, update subreq on DB)*/
          /* we don t execute: dbSvc->updateRep ..*/
          if(stgReplyHelper != NULL) delete stgReplyHelper;
          
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
