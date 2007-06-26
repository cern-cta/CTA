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
      
      
      StagerReplyHelper::StagerReplyHelper() throw(castor::exception::Exception)
      {
	try{
	  this->ioResponse = new castor::rh::IOResponse;
	  if(this->ioResponse == NULL){
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<<"(StagerReplyHelper constructor) Impossible to get the ioResponse object"<<std::endl;
	    throw ex; 
	  }	
	 
	  this->requestReplier = castor::replier::RequestReplier::getInstance();
	  if(this->requestReplier == NULL){
	    castor::exception::Exception ex(SEINTERNAL);
	    ex.getMessage()<<"(StagerReplyHelper constructor) Impossible to get the requestReplier instance"<<std::endl;
	    throw ex;
	  }
	}catch(castor::exception::Exception ex){
	  if( ioResponse != NULL){
	    delete ioResponse;
	  }
	  throw ex;
	}

      }
	

      StagerReplyHelper::~StagerReplyHelper() throw()
      {
	
      }

      
      /****************************************************************************/
      /* set fileId, reqAssociated (reqId()), castorFileName,newSubReqStatus,    */
      /**************************************************************************/
      void StagerReplyHelper::setAndSendIoResponse(StagerRequestHelper* stgRequestHelper,Cns_fileid *fileID, int errorCode, std::string errorMessage) throw(castor::exception::Exception)
      {
	ioResponse->setFileId((u_signed64)(fileID==0?0:fileID->fileid));
	
	this->uuid_as_string = stgRequestHelper->fileRequest->reqId();
	if((uuid_as_string.empty()) == false){
	  this->ioResponse->setReqAssociated(uuid_as_string);
	}else{
	  castor::exception::Exception ex(SEINTERNAL);
	  ex.getMessage()<<"(StagerReplyHelper setAndSendIoResponse) The fileRequest has not uuid"<<std::endl;
	  throw ex;
	}
	
	if(stgRequestHelper->castorFile != NULL){
	  this->ioResponse->setCastorFileName(stgRequestHelper->subrequest->fileName());
	}else{
	  castor::exception::Exception ex(SEINTERNAL);
	  ex.getMessage()<<"(StagerReplyHelper setAndSendIoResponse) The castorFile is NULL"<<std::endl;
	  throw ex;
	}
	
	int newSubRequestStatus = stgRequestHelper->subrequest->status();
	if(newSubRequestStatus==SUBREQUEST_FAILED_FINISHED){
	  newSubRequestStatus=SUBREQUEST_FAILED;
	}
	this->ioResponse->setStatus(newSubRequestStatus);
	
	this->ioResponse->setId(stgRequestHelper->subrequest->id());
	
	/* errorCode = exception.code() */
	if(errorCode != 0){
	  this->ioResponse->setErrorCode(errorCode);
	  std::string ioRespErrorMessage = errorMessage;
	  if((errorMessage.empty()) == false){
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
      /*** inline void StagerReplyHelper::endReplyToClient(StagerRequestHelper* stgRequestHelper) throw() ***/
      


    }//end namespace dbService
  }//end namespace stager
}//end namespace castor


