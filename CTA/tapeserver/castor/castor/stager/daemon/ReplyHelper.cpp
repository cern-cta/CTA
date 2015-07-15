/**********************************************************************************************************************/
/* helper class containing the objects and methods which interact to performe the response to the client             */
/* it is needed to provide:                                                                                         */
/*     - a common place where its objects can communicate                                                          */
/* it is always used by: PrepareToGet, Repack, PrepareToPut, PrepareToUpdate, Rm, SetFileGCWeight, PutDone  */
/* just in case of error, by all the handlers                                                                    */
/****************************************************************************************************************/


#include "castor/stager/daemon/ReplyHelper.hpp"
#include "castor/stager/daemon/RequestHelper.hpp"

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
    namespace daemon{


      ReplyHelper::ReplyHelper() 
      {
        try{
          this->ioResponse = new castor::rh::IOResponse;
          this->requestReplier = castor::replier::RequestReplier::getInstance();

        }catch(castor::exception::Exception& ex){
          if( ioResponse != NULL){
            delete ioResponse;
            ioResponse = 0;
          }
          throw ex;
        }

      }


      ReplyHelper::~ReplyHelper() throw()
      {
        if(ioResponse) delete ioResponse;
      }


      /****************************************************************************/
      /* set fileId, reqAssociated (reqId()), castorFileName,newSubReqStatus,    */
      /**************************************************************************/
      void ReplyHelper::setAndSendIoResponse(RequestHelper* reqHelper, 
					     Cns_fileid* cnsFileid, 
					     int errorCode, 
					     std::string errorMessage, 
					     const castor::stager::DiskCopyInfo* diskCopy)
        
      {
        if(!reqHelper->fileRequest) {
          castor::exception::Exception e;
          e.getMessage() << "No request available, cannot answer to client";
          throw e;
        }

        if(cnsFileid) {
          ioResponse->setFileId((u_signed64) cnsFileid->fileid);
        } else {
          ioResponse->setFileId(0);
        }
        ioResponse->setReqAssociated(reqHelper->fileRequest->reqId());
        if (diskCopy) {
          ioResponse->setFileName(diskCopy->diskCopyPath());
	  ioResponse->setServer(diskCopy->diskServer());
        }
	ioResponse->setCastorFileName(reqHelper->subrequest->fileName());
	ioResponse->setStatus(reqHelper->subrequest->status() == SUBREQUEST_FAILED_FINISHED ? SUBREQUEST_FAILED : SUBREQUEST_READY);
        ioResponse->setId(reqHelper->subrequest->id());

        /* errorCode = exception.code() */
        if(errorCode != 0){
          this->ioResponse->setErrorCode(errorCode);
          std::string ioRespErrorMessage = errorMessage;
          if((errorMessage.empty()) == true){
            ioRespErrorMessage = sstrerror(errorCode);
          }

          /* ioRespErrorMessage = ex.message() */
          ioResponse->setErrorMessage(ioRespErrorMessage);
        }

        requestReplier->sendResponse(reqHelper->fileRequest->client(), ioResponse, false);
      }


      /*********************************************************************************************/
      /* check if there is any subrequest left and send the endResponse to client if it is needed */
      /*******************************************************************************************/
      void ReplyHelper::endReplyToClient(RequestHelper* reqHelper) {
        /* to update the subrequest on DB */
        bool requestLeft = reqHelper->stagerService->updateAndCheckSubRequest(reqHelper->subrequest);
        if(!requestLeft && reqHelper->fileRequest != 0) {
          requestReplier->closeClientConnection(reqHelper->fileRequest->client());
        }
      }



    }//end namespace daemon
  }//end namespace stager
}//end namespace castor


