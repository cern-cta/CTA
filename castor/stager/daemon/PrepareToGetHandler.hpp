/**********************************************************************************************/
/* StagerPrepareToGetHandler: Constructor and implementation of the get subrequest's handler */
/* It inherits from the StagerJobRequestHandler and it needs to reply to the client         */
/*******************************************************************************************/


#ifndef STAGER_PREPARE_TO_GET_HANDLER_HPP
#define STAGER_PREPARE_TO_GET_HANDLER_HPP 1



#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"

#include "castor/stager/dbService/StagerRequestHandler.hpp"
#include "castor/stager/dbService/StagerJobRequestHandler.hpp"


#include "stager_uuid.h"
#include "stager_constants.h"
#include "serrno.h"
#include "Cns_api.h"
#include "expert_api.h"
#include "rm_api.h"
#include "Cpwd.h"
#include "Cgrp.h"
#include "u64subr.h"
#include "castor/IClientFactory.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"

#include "castor/exception/Exception.hpp"

#include "castor/ObjectSet.hpp"
#include "castor/IObject.hpp"

#include <iostream>
#include <string>



namespace castor{
  namespace stager{
    namespace dbService{

      class StagerRequestHelper;
      class StagerCnsHelper;
      
      class StagerPrepareToGetHandler : public virtual StagerJobRequestHandler{

	
      public:

	/* constructor */
	StagerPrepareToGetHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper) throw(castor::exception::Exception);
	/* destructor */
	~StagerPrepareToGetHandler() throw();

	/********************************************************************************************************/
	/* we are overwritting this function inherited from the StagerJobRequestHandler  because of the case 0 */
	/*  it isn't included on the switchScheduling, it's processed on the handle main flow                 */
	/* after asking the stagerService is it is toBeScheduled                     */
	/* - do a normal tape recall                                                */
	/* - check if it is to replicate:                                          */
	/*         +processReplica if it is needed:                               */
	/*                    +make the hostlist if it is needed                 */
	/************************************************************************/
	void switchScheduling(int caseToSchedule) throw(castor::exception::Exception);

	/* PrepareToGet request handler */
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

      }; //end StagerPrepareToGetHandler class


    }//end namespace dbService
  }//end namespace stager
}//end namespace castor


#endif
