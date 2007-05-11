/**********************************************************************************************************************/
/* helper class containing the objects and methods which interact to performe the response to the client             */
/* it is needed to provide:                                                                                         */
/*     - a common place where its objects can communicate                                                          */
/* it is always used by: StagerPrepareToGet,Repack, PrepareToPut, PrepareToUpdate, Rm, SetFileGCWeight, PutDone   */
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


      /* forward declaration */
      class castor::rh::IOResponse;       
      class castor::replier::RequestReplier;
      class StagerRequestHelper;



      class StagerReplyHelper :public::castor::IObject{

      public:

	castor::rh::IOResponse *ioResponse;
	castor::replier::RequestReplier *requestReplier;
	std::string uuid_as_string;

	/* reserve memory for the attributes dinamically */
	StagerReplyHelper::StagerReplyRequestHelper() throw();
	/* free the ioResponse and the iResponse */
	StagerReplyHelper::~StagerReplyRequestHelper() throw();



	/****************************************************************************/
	/* set fileId, reqAssociated (reqId()), castorFileName,newSubReqStatus,    */
	/**************************************************************************/
	inline void StagerReplyHelper::setAndSendIoResponse(StagerRequestHelper stgRequestHelper,Cns_fileid *fileID, int errorCode, std::string errorMessage) throw();
	
	/*********************************************************************************************/
	/* check if there is any subrequest left and send the endResponse to client if it is needed */
	/*******************************************************************************************/
	inline void StagerReplyHelper::endReplyToClient(StagerRequestHelper &stgRequestHelper) throw();
	
	
      }// end StagerReplyHelper  


    }//end namespace dbService
  }//end namespace stager
}//end namespace castor


#endif
