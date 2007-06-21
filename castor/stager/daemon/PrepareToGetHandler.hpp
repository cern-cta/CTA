/**********************************************************************************************/
/* StagerPrepareToGetHandler: Constructor and implementation of the get subrequest's handler */
/* It inherits from the StagerJobRequestHandler and it needs to reply to the client         */
/*******************************************************************************************/


#ifndef STAGER_PREPARE_TO_GET_HANDLER_HPP
#define STAGER_PREPARE_TO_GET_HANDLER_HPP 1



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
#include "../SubRequestGetNextStatusCodes.hpp"

#include "../../exception/Exception.hpp"

#include "../../ObjectSet.hpp"
#include "../../IObject.hpp"
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
	StagerPrepareToGetHandler::StagerPrepareToGetHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper) throw(castor::exception::Exception);
	/* destructor */
	StagerPrepareToGetHandler::~StagerPrepareToGetHandler() throw();

	/********************************************************************************************************/
	/* we are overwritting this function inherited from the StagerJobRequestHandler  because of the case 0 */
	/*  it isn't included on the switchScheduling, it's processed on the handle main flow                 */
	/* after asking the stagerService is it is toBeScheduled                     */
	/* - do a normal tape recall                                                */
	/* - check if it is to replicate:                                          */
	/*         +processReplica if it is needed:                               */
	/*                    +make the hostlist if it is needed                 */
	/************************************************************************/
	void StagerPrepareToGetHandler::switchScheduling(int caseToSchedule) throw(castor::exception::Exception);

	/* PrepareToGet request handler */
        void StagerPrepareToGetHandler::handle() throw(castor::exception::Exception);



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
