/****************************************************************************************************/
/* StagerPrepareToPutHandler: Constructor and implementation of the PrepareToPut request's handler */
/**************************************************************************************************/

#ifndef STAGER_PREPARE_TO_PUT_HANDLER_HPP
#define STAGER_PREPARE_TO_PUT_HANDLER_HPP 1




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

#include "../../IClientFactory.hpp"
#include "../SubRequestStatusCodes.hpp"

#include "../../exception/Exception.hpp"
#include "../../exception/Internal.hpp"
#include "../../ObjectSet.hpp"
#include "../../IObject.hpp"


#include <iostream>
#include <string>



namespace castor{
  namespace stager{
    namespace dbService{

      class StagerRequestHelper;
      class StagerCnsHelper;


      class StagerPrepareToPutHandler : public virtual StagerJobRequestHandler{

    
      public:
	/* constructor */
	StagerPrepareToPutHandler::StagerPrepareToPutHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper) throw(castor::exception::Exception);

	/* destructor */
	StagerPrepareToPutHandler::~StagerPrepareToPutHandler() throw();

	/* PrepareToPut request handler */
	void StagerPrepareToPutHandler::handle() throw(castor::exception::Exception);


	/***********************************************************************************************/
	/* virtual functions inherited from IObject                                                   */
	/*********************************************************************************************/
	virtual void setId(u_signed64 id);
	virtual u_signed64 id() const;
	virtual int type() const;
	virtual IObject* clone();
	virtual void print() const;
	virtual void print(std::ostream& stream, std::string indent, castor::ObjectSet& alreadyPrinted) const;
	
      }; //end StagerPrepareToPutHandler class

    }//end dbService namespace
  }//end stager namespace
}//end castor namespace


#endif
