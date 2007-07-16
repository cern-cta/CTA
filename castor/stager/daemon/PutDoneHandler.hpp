/*************************************************************************************************/
/* StagerPutDoneHandler: Constructor and implementation of the PutDone request's handle */
/* basically it is a calll to the jobService->prepareForMigration()       */
/***********************************************************************************************/


#ifndef STAGER_PUT_DONE_HANDLER_HPP
#define STAGER_PUT_DONE_HANDLER_HPP 1


#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"

#include "castor/stager/dbService/StagerRequestHandler.hpp"
#include "castor/stager/dbService/StagerJobRequestHandler.hpp"

#include "stager_uuid.h"
#include "stager_constants.h"
#include "serrno.h"
#include "expert_api.h"
#include "rm_api.h"
#include "Cpwd.h"
#include "Cgrp.h"
#include "u64subr.h"

#include "castor/IClientFactory.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"

#include "castor/stager/DiskCopyForRecall.hpp"
#include "../IJobSvc.hpp"

#include "castor/IObject.hpp"
#include "castor/ObjectSet.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"

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
	
	
	
      public:
	/* constructor */
	StagerPutDoneHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper) throw(castor::exception::Exception);
	/* destructor */
	~StagerPutDoneHandler() throw();

	/* putDone request handler */
	void handle() throw(castor::exception::Exception);
     
	
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
