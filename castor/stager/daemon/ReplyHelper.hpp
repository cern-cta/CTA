/**********************************************************************************************************************/
/* helper class containing the objects and methods which interact to performe the response to the client             */
/* it is needed to provide:                                                                                         */
/*     - a common place where its objects can communicate                                                          */
/* it is always used by: StagerPrepareToGet,Repack, PrepareToPut, PrepareToUpdate, Rm, SetFileGCWeight, PutDone   */
/* just in case of error, by all the handlers                                                                    */
/****************************************************************************************************************/

#ifndef STAGER_REPLY_HELPER_HPP
#define STAGER_REPLY_HELPER_HPP 1

#include "StagerRequestHelper.hpp"
#include "../../rh/IOResponse.hpp"
#include "../../replier/RequestReplier.hpp"
#include "../FileRequest.hpp"
#include "../IStagerSvc.hpp"
#include "../../exception/Exception.hpp"

#include "../SubRequestStatusCodes.hpp"

#include "../../../h/u64subr.h"


#include "../../ObjectSet.hpp"
#include "../../IObject.hpp"

#include <iostream>
#include <string>


namespace castor{
  namespace stager{
    namespace dbService{


      /* forward declaration */
      class castor::rh::IOResponse;       
      class castor::replier::RequestReplier;
      class StagerRequestHelper;



      class StagerReplyHelper : public virtual castor::IObject{

      public:

	castor::rh::IOResponse *ioResponse;
	castor::replier::RequestReplier *requestReplier;
	std::string uuid_as_string;

	/* constructor  */
	StagerReplyHelper::StagerReplyHelper() throw(castor::exception::Exception);
	/* destructor */
	StagerReplyHelper::~StagerReplyHelper() throw();



	/****************************************************************************/
	/* set fileId, reqAssociated (reqId()), castorFileName,newSubReqStatus,    */
	/**************************************************************************/
	void StagerReplyHelper::setAndSendIoResponse(StagerRequestHelper* stgRequestHelper,Cns_fileid *fileID, int errorCode, std::string errorMessage) throw(castor::exception::Exception);
	

	/*********************************************************************************************/
	/* check if there is any subrequest left and send the endResponse to client if it is needed */
	/*******************************************************************************************/
	inline void StagerReplyHelper::endReplyToClient(StagerRequestHelper* stgRequestHelper) throw(castor::exception::Exception){
	  try{
	    /* to update the subrequest on DB */
	    bool requestLeft = stgRequestHelper->stagerService->updateAndCheckSubRequest(stgRequestHelper->subrequest);
	    if(requestLeft){
	      this->requestReplier->sendEndResponse(stgRequestHelper->iClient, this->uuid_as_string);
	    }  

	    /* delete the attributes */
	    delete ioResponse;
	    delete requestReplier;    

	  }catch(castor::exception::Exception ex){
	    if( ioResponse != NULL){
	      delete ioResponse;
	    }
	    if(requestReplier){
	      delete requestReplier;
	    }
	    throw ex;
	  }
	}

	/*****************************************************************************************/
	/* virtual functions inherited from IObject                                             */
	/***************************************************************************************/
	virtual void setId(u_signed64 id);
	virtual u_signed64 id() const;
	virtual int type() const;
	virtual IObject* clone();
	virtual void print() const;
	virtual void print(std::ostream& stream, std::string indent, castor::ObjectSet& alreadyPrinted) const;

	
      }; // end StagerReplyHelper  


    }//end namespace dbService
  }//end namespace stager
}//end namespace castor


#endif
