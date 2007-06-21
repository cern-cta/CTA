/*************************************************************************************************/
/* StagerPutDoneHandler: Constructor and implementation of the PutDone request's handle */
/* basically it is a calll to the jobService->prepareForMigration()       */
/***********************************************************************************************/


#ifndef STAGER_PUT_DONE_HANDLER_HPP
#define STAGER_PUT_DONE_HANDLER_HPP 1


#include "StagerRequestHelper.hpp"
#include "StagerCnsHelper.hpp"
#include "StagerReplyHelper.hpp"

#include "StagerRequestHandler.hpp"
#include "StagerJobRequestHandler.hpp"

#include "../../../h/stager_uuid.h"
#include "../../../h/stager_constants.h"
#include "../../../h/serrno.h"
#include "../../../h/Cns_api.h"
#include "../../../h/expert_api.h"
#include "../../../h/rm_api.h"
#include "../../../h/Cpwd.h"
#include "../../../h/Cgrp.h"
#include "../../../h/u64subr.h"

#include "../../IClientFactory.h"
#include "../SubRequestStatusCodes.hpp"
#include "../DiskCopyForRecall.hpp"
#include "../IJobSvc.hpp"

#include "../../IObject.hpp"
#include "../../ObjectSet.hpp"
#include "../../exception/Exception.hpp"
#include "../../exception/Internal.hpp"

#include <iostream>
#include <string>




namespace castor{
  namespace stager{
    namespace dbService{

      class StagerRequestHelper;
      class StagerCnsHelper;

      class StagerPutDoneHandler : public virtual StagerJobRequestHandler{


      private:
	castor::stager::IJobSvc* jobService;
	std::list<castor::stager::DiskCopyForRecall*> *sources;
	
	
      public:
	/* constructor */
	StagerPutDoneHandler::StagerPutDoneHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper) throw(castor::exception::Exception);
	/* destructor */
	StagerPutDoneHandler::~StagerPutDoneHandler() throw();

	/* putDone request handler */
	void StagerPutDoneHandler::handle() throw(castor::exception::Exception);
     
	
      	/***********************************************************************************************/
	/* virtual functions inherited from IObject                                                   */
	/*********************************************************************************************/
	virtual void setId(u_signed64 id);
	virtual u_signed64 id() const;
	virtual int type() const;
	virtual IObject* clone();
	virtual void print() const;
	virtual void print(std::ostream& stream, std::string indent, castor::ObjectSet& alreadyPrinted) const;
	
      }; //end StagerPutDoneHandler class

    }//end dbService 
  }//end stager
}//end castor



#endif
