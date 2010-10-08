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
#include "castor/exception/Internal.hpp"

#include "serrno.h"
#include "Cns_struct.h"
#include "Cns_api.h"
#include "u64subr.h"


#include <iostream>
#include <string>

namespace castor{
  namespace stager{
    namespace daemon{


      ReplyHelper::ReplyHelper() throw(castor::exception::Exception)
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
      void ReplyHelper::setAndSendIoResponse(RequestHelper* stgRequestHelper, 
					     Cns_fileid* cnsFileid, 
					     int errorCode, 
					     std::string errorMessage, 
					     const castor::stager::DiskCopyInfo* diskCopy)
	throw(castor::exception::Exception)
      {
        if(stgRequestHelper->fileRequest) {
          if(stgRequestHelper->fileRequest->client() == 0) {
            stgRequestHelper->dbSvc->fillObj(stgRequestHelper->baseAddr, stgRequestHelper->fileRequest, castor::OBJ_IClient, false);
          }
        }
        else {
          castor::exception::Internal e;
          e.getMessage() << "No request available, cannot answer to client";
          throw e;
        }

        if(cnsFileid) {
          ioResponse->setFileId((u_signed64) cnsFileid->fileid);
        } else {
          ioResponse->setFileId(0);
        }

        if (!stgRequestHelper->fileRequest->reqId().empty()) {
          this->ioResponse->setReqAssociated(stgRequestHelper->fileRequest->reqId());
        } else {
          // no UUID?? at this stage just log it and try to go on
          castor::dlf::Param params[]={ castor::dlf::Param(stgRequestHelper->subrequestUuid),
            castor::dlf::Param("Filename",stgRequestHelper->subrequest->fileName()),
            castor::dlf::Param("Username",stgRequestHelper->username),
            castor::dlf::Param("Groupname", stgRequestHelper->groupname),
            castor::dlf::Param("Function", "ReplyHelper.setAndSendIoResponse")
          };
          castor::dlf::dlf_writep(stgRequestHelper->requestUuid, DLF_LVL_WARNING, STAGER_REQUESTUUID_EXCEPTION, 5, params);
        }

        if (diskCopy) {
          ioResponse->setFileName(diskCopy->diskCopyPath());
	  ioResponse->setServer(diskCopy->diskServer());
        }
	ioResponse->setCastorFileName(stgRequestHelper->subrequest->fileName());
	ioResponse->setStatus(stgRequestHelper->subrequest->status() == SUBREQUEST_FAILED_FINISHED ? SUBREQUEST_FAILED : SUBREQUEST_READY);
        ioResponse->setId(stgRequestHelper->subrequest->id());

        /* errorCode = exception.code() */
        if(errorCode != 0){
          this->ioResponse->setErrorCode(errorCode);
          std::string ioRespErrorMessage = errorMessage;
          if((errorMessage.empty()) == true){
            ioRespErrorMessage = sstrerror(errorCode);
          }

          /* ioRespErrorMessage = ex.message() */
          this->ioResponse->setErrorMessage(ioRespErrorMessage);
        }

        this->requestReplier->sendResponse(stgRequestHelper->fileRequest->client(), ioResponse, false);
      }


      /*********************************************************************************************/
      /* check if there is any subrequest left and send the endResponse to client if it is needed */
      /*******************************************************************************************/
      void ReplyHelper::endReplyToClient(RequestHelper* stgRequestHelper) throw(castor::exception::Exception){
        /* to update the subrequest on DB */
        bool requestLeft = stgRequestHelper->stagerService->updateAndCheckSubRequest(stgRequestHelper->subrequest);
        if(!requestLeft && stgRequestHelper->fileRequest != 0) {
          requestReplier->sendEndResponse(stgRequestHelper->fileRequest->client(), stgRequestHelper->fileRequest->reqId());
        }
      }



    }//end namespace daemon
  }//end namespace stager
}//end namespace castor


