/**********************************************************************************************************************/
/* helper class containing the objects and methods which interact to performe the response to the client             */
/* it is needed to provide:                                                                                         */
/*     - a common place where its objects can communicate                                                          */
/* it is always used by: StagerPrepareToGet, Repack, PrepareToPut, PrepareToUpdate, Rm, SetFileGCWeight, PutDone  */
/* just in case of error, by all the handlers                                                                    */
/****************************************************************************************************************/


#include "castor/stager/dbService/StagerReplyHelper.hpp"
#include "castor/stager/dbService/StagerRequestHelper.hpp"

#include "castor/rh/IOResponse.hpp"
#include "castor/replier/RequestReplier.hpp"
#include "castor/stager/FileRequest.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/IStagerSvc.hpp"

#include "castor/stager/SubRequestStatusCodes.hpp"

#include "castor/exception/Exception.hpp"

#include "serrno.h"
#include "Cns_struct.h"
#include "Cns_api.h"
#include "u64subr.h"


#include <iostream>
#include <string>

namespace castor{
  namespace stager{
    namespace dbService{
      
      
      StagerReplyHelper::StagerReplyHelper(castor::stager::SubRequestStatusCodes newSubreqStatus) throw(castor::exception::Exception)
      {
        try{
          this->ioResponse = new castor::rh::IOResponse;
          this->requestReplier = castor::replier::RequestReplier::getInstance();
          
          if(newSubreqStatus == SUBREQUEST_FAILED_FINISHED){
            this->newSubrequestStatus = SUBREQUEST_FAILED;
          }else if(newSubreqStatus == SUBREQUEST_READYFORSCHED){
            this->newSubrequestStatus = SUBREQUEST_READY;
          }else{
            this->newSubrequestStatus = newSubreqStatus;
          }
          
          
          
        }catch(castor::exception::Exception ex){
          if( ioResponse != NULL){
            delete ioResponse;
            ioResponse = 0;
          }
          throw ex;
        }
        
      }
      
      
      StagerReplyHelper::~StagerReplyHelper() throw()
      {
        if(ioResponse) delete ioResponse;
      }
      
      
      /****************************************************************************/
      /* set fileId, reqAssociated (reqId()), castorFileName,newSubReqStatus,    */
      /**************************************************************************/
      void StagerReplyHelper::setAndSendIoResponse(StagerRequestHelper* stgRequestHelper,Cns_fileid cnsFileid, int errorCode, std::string errorMessage) throw(castor::exception::Exception)
      {
        ioResponse->setFileId((u_signed64) cnsFileid.fileid);
        
        this->uuid_as_string = stgRequestHelper->fileRequest->reqId();
        if((uuid_as_string.empty()) == false){
          this->ioResponse->setReqAssociated(uuid_as_string);
        }else{
          castor::exception::Exception ex(SEINTERNAL);
          ex.getMessage()<<"(StagerReplyHelper setAndSendIoResponse) The fileRequest has not uuid"<<std::endl;
          throw ex;
        }
        
        this->ioResponse->setCastorFileName(stgRequestHelper->subrequest->fileName());
        this->ioResponse->setStatus(this->newSubrequestStatus);
        this->ioResponse->setId(stgRequestHelper->subrequest->id());
        
        /* errorCode = exception.code() */
        if(errorCode != 0){
          this->ioResponse->setErrorCode(errorCode);
          std::string ioRespErrorMessage = errorMessage;
          if((errorMessage.empty()) == true){
            ioRespErrorMessage = strerror(errorCode);
          }
          
          /* ioRespErrorMessage = ex.message() */
          this->ioResponse->setErrorMessage(ioRespErrorMessage);
        }
        
        /* sendResponse(..,.., lastResponse = false)  */
        this->requestReplier->sendResponse(stgRequestHelper->iClient,ioResponse,false);
        
        
      }
      
      
      
      
      /*********************************************************************************************/
      /* check if there is any subrequest left and send the endResponse to client if it is needed */
      /*******************************************************************************************/
      void StagerReplyHelper::endReplyToClient(StagerRequestHelper* stgRequestHelper) throw(castor::exception::Exception){
        
        /* to update the subrequest on DB */
        bool requestLeft = stgRequestHelper->stagerService->updateAndCheckSubRequest(stgRequestHelper->subrequest);
        if(requestLeft == false){
          requestReplier->sendEndResponse(stgRequestHelper->iClient, this->uuid_as_string);
        }  
        
      }
      
      
      
    }//end namespace dbService
  }//end namespace stager
}//end namespace castor


