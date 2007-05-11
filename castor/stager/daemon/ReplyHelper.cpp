/**********************************************************************************************************************/
/* helper class containing the objects and methods which interact to performe the response to the client             */
/* it is needed to provide:                                                                                         */
/*     - a common place where its objects can communicate                                                          */
/* it is always used by: StagerPrepareToGet, Repack, PrepareToPut, PrepareToUpdate, Rm, SetFileGCWeight, PutDone  */
/* just in case of error, by all the handlers                                                                    */
/****************************************************************************************************************/

#ifndef STAGER_REPLY_HELPER_HPP
#define STAGER_REPLY_HELPER_HPP 1

#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/rh/IOResponse.hpp"
#include "castor/replier/RequestReplier.hpp"
#include "castor/stager/FileRequest.hpp"
#include "castor/stager/IStagerSvc.hpp"

#include <iostream>
#include <string>

namespace castor{
  namespace stager{
    namespace dbService{
      

      StagerReplyHelper::StagerReplyHelper()
      {
	this->ioResponse = new castor::rh::IOResponse*;
	this->requestReplier = RequestReplier.getInstance();
      }
      

      StagerReplyHelper::~StagerReplyHelper()
      {
	delete ioResponse;
	delete requestReplier;
      }

      
      /****************************************************************************/
      /* set fileId, reqAssociated (reqId()), castorFileName,newSubReqStatus,    */
      /**************************************************************************/
      void StagerReplyHelper::setAndSendIoResponse(StagerRequestHelper stgRequestHelper,Cns_fileid *fileID, int errorCode, std::string errorMessage)
      {

	ioResponse->setFileId(fileID==0?0:fileID->fileid);


        this->uuid_as_string = stgRequestHelper->fileRequest->reqId();
	if(this->uuid_as_string != NULL){
	  this->ioResponse->setReqAssociated(uuid_as_string);
	}


	if(stgRequestHelper->castorFile != NULL){
	  this->ioResponse->setCastorFileName(stgRequestHelper->subrequest->fileName());
	}


	int newSubRequestStatus = stgRequestHelper->subrequest->status();
	(newSubRequestStatus==SUBREQUEST_FAILED_FINISHED)?SUBREQUEST_FAILED:newSubRequestStatus;
	this->ioResponse->setStatus(newSubRequestStatus);


	this->ioResponse->setId(stgRequestHelper->subrequestUuid);

	/* errorCode = rc  */
	if(errorCode != 0){
	  this->ioResponse->setErrorCode(errorCode);
	  this->ioResponse->setErrorMessage((errorMessage!=NULL)? errorMessage:strerror(serrno));
	}

	/* 0 = lastResponse parameter */
	this->requestReplier->sendResponse(stgRequestHelper->iClient,ioResponse,0 );
      }


	
      /*********************************************************************************************/
      /* check if there is any subrequest left and send the endResponse to client if it is needed */
      /*******************************************************************************************/
      void StagerReplyHelper::endReplyToClient(StagerRequestHelper &stgRequestHelper)
      {
	bool requestLeft = stgRequestHelper->stagerService->updateAndCheckSubrequest(stgRequestHelper->subrequest);
	if(requestLeft){
	  this->requestReplier->sendEnResponse(stgRequestHelper->iClient, this->uuid_as_string);
	  }
      }      



    }//end namespace dbService
  }//end namespace stager
}//end namespace castor


