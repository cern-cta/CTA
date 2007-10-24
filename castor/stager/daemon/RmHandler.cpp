
/************************************************************************************/
/* handler for the Rm subrequest, simply call to the stagerService->stageRm()      */
/* since it isn' t job oriented, it inherits from the StagerRequestHandler        */
/* it always needs to reply to the client                                        */
/********************************************************************************/

#include "castor/stager/dbService/StagerRmHandler.hpp"
#include "castor/stager/dbService/StagerRequestHandler.hpp"
#include "castor/stager/dbService/StagerJobRequestHandler.hpp"

#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"



#include "castor/stager/IStagerSvc.hpp"

#include "stager_constants.h"

#include "expert_api.h"

#include "Cpwd.h"
#include "Cgrp.h"

#include "castor/IClientFactory.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"

#include "serrno.h"
#include <errno.h>
#include <iostream>
#include <string>


namespace castor{
  namespace stager{
    namespace dbService{
      
      
      
      /* constructor */
      StagerRmHandler::StagerRmHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper) throw() :
        StagerRequestHandler()
      {
        this->stgRequestHelper = stgRequestHelper;
        this->stgCnsHelper = stgCnsHelper;
        this->typeRequest = OBJ_StageRmRequest;
        
      }
      
      
      /* only handler which overwrite the preprocess part due to the specific behavior related with the svcClass */
      void StagerRmHandler::preHandle() throw(castor::exception::Exception)
      {
        /* get the uuid subrequest string version and check if it is valid */
        /* we can create one !*/
        stgRequestHelper->setSubrequestUuid();
        
        /* get the associated client and set the iClientAsString variable */
        stgRequestHelper->getIClient();
        
        
        /* set the euid, egid attributes on stgCnsHelper (from fileRequest) */ 
        stgCnsHelper->cnsSetEuidAndEgid(stgRequestHelper->fileRequest);
        
        /* get the uuid request string version and check if it is valid */
        stgRequestHelper->setRequestUuid();
        
        /*********************************************************************************************************************************/
        /* JUST FOR StagerRmHandler TO IMPLEMENT CONSIDERING THAT * CAN BE VALID : get the svcClass and set it on the  stgRequestHelper */
        /* an link it to */
        this->rmGetSvcClass();
        /*****************************************************************************************************************************/
        
        /* check the existence of the file, if the user hasTo/can create it and set the fileId and server for the file */
        /* create the file if it is needed/possible */
        stgCnsHelper->checkAndSetFileOnNameServer(this->typeRequest, stgRequestHelper->subrequest->flags(), stgRequestHelper->subrequest->modeBits(), stgRequestHelper->svcClass);
        
        /* check if the user (euid,egid) has the right permission for the request's type. otherwise-> throw exception  */
        stgRequestHelper->checkFilePermission();
        
        castor::dlf::Param parameter[] = {castor::dlf::Param("Standard Message","(RequestSvc) Detailed subrequest(fileName,{euid,egid},{userName,groupName},mode mask, cliendId, svcClassName,cnsFileid.fileid, cnsFileid.server"),
          castor::dlf::Param("Standard Message",stgCnsHelper->subrequestFileName),
          castor::dlf::Param("Standard Message",(unsigned long) stgCnsHelper->euid),
          castor::dlf::Param("Standard Message",(unsigned long) stgCnsHelper->egid),
          castor::dlf::Param("Standard Message",stgRequestHelper->username),
          castor::dlf::Param("Standard Message",stgRequestHelper->groupname),
          castor::dlf::Param("Standard Message",stgRequestHelper->fileRequest->mask()),
          castor::dlf::Param("Standard Message",stgRequestHelper->iClient->id()),
          castor::dlf::Param("Standard Message",stgRequestHelper->svcClassName),
          castor::dlf::Param("Standard Message",stgCnsHelper->cnsFileid.fileid),
        castor::dlf::Param("Standard Message",stgCnsHelper->cnsFileid.server)};
        castor::dlf::dlf_writep( nullCuuid, DLF_LVL_USAGE, 1, 11, parameter);
        
        
      }
      
      
      /*************************************************************************/
      /* TO BE CONFIRMED */
      /* we just need to GET THE svcClassId by getting the right svcClass */
      void StagerRmHandler::rmGetSvcClass() throw (castor::exception::Exception)
      {
        
        stgRequestHelper->svcClassName=stgRequestHelper->fileRequest->svcClassName(); 
        
        if(stgRequestHelper->svcClassName.empty()){  /* we set the default svcClassName */
          stgRequestHelper->svcClassName="default";
          
          /* the goal is to get svcClassId, so the following step might be useless (commented!) */
          /* stgRequestHelper->fileRequest->setSvcClassName(stgRequestHelper->svcClassName);*/
          stgRequestHelper->svcClass=stgRequestHelper->stagerService->selectSvcClass(stgRequestHelper->svcClassName);
          if(stgRequestHelper->svcClass == NULL){
            castor::exception::Exception ex(SEINTERNAL);
            ex.getMessage()<<"(StagerRmHandler handle) Impossible to get the svcClass"<<std::endl;
            throw(ex);
          }
          this->svcClassId = stgRequestHelper->svcClass->id();	   
          
        }else if(stgRequestHelper->svcClassName == "*"){
          this->svcClassId = 0;
          
        }else{
          stgRequestHelper->svcClass=stgRequestHelper->stagerService->selectSvcClass(stgRequestHelper->svcClassName);
          if(stgRequestHelper->svcClass == NULL){
            castor::exception::Exception ex(SEINTERNAL);
            ex.getMessage()<<"(StagerRmHandler handle) Impossible to get the svcClass"<<std::endl;
            throw(ex);
          }
          this->svcClassId = stgRequestHelper->svcClass->id();	  
        }
        
      }
      
      
      /******************************/
      /* handle for the rm request */
      /****************************/
      void StagerRmHandler::handle() throw(castor::exception::Exception)
      {
        StagerReplyHelper* stgReplyHelper=NULL;
        try{
          
          /**************************************************************************/
          /* common part for all the handlers: get objects, link, check/create file*/
          preHandle();
          /**********/
          
          /* execute the main function for the rm request                 */
          /* basically, a call to the corresponding stagerService method */
          std::string server(stgCnsHelper->cnsFileid.server);
          if(stgRequestHelper->stagerService->stageRm(
          stgRequestHelper->subrequest->id(),stgCnsHelper->cnsFileid.fileid, server,this->svcClassId, 0)) {
            
            stgRequestHelper->stagerService->archiveSubReq(stgRequestHelper->subrequest->id());
            
            /* replyToClient Part: */
            stgReplyHelper = new StagerReplyHelper(SUBREQUEST_READY);	  
            stgReplyHelper->setAndSendIoResponse(stgRequestHelper,stgCnsHelper->cnsFileid,0, "No error");
            stgReplyHelper->endReplyToClient(stgRequestHelper);
            delete stgReplyHelper;
          }
          else {  // user error, log it
          }
          
        }catch(castor::exception::Exception e){
          if(stgReplyHelper != NULL){
            delete stgReplyHelper;
          }
          castor::exception::Exception ex(e.code());
          ex.getMessage()<<"(StagerRmHandler) Error"<<e.getMessage().str()<<std::endl;
          throw ex;
          
        }
      }
      
    }//end dbService
  }//end stager
}//end castor
