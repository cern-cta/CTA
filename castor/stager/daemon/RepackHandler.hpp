/**********************************************************************************************/
/* StagerRepackHandler: Constructor and implementation of the repack subrequest's handler */
/* It inherits from the StagerJobRequestHandler and it needs to reply to the client         */
/*******************************************************************************************/

#ifndef STAGER_REPACK_HANDLER_HPP
#define STAGER_REPACK_HANDLER_HPP 1


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

      class StagerRepackHandler : public virtual StagerJobRequestHandler{

     
      public:
	/* constructor */
	StagerRepackHandler::StagerRepackHandler(StagerRequestHelper* stgRequestHelper,StagerCnsHelper* stgCnsHelper) throw(castor::exception::Exception);
	/* destructor */
	StagerRepackHandler::~StagerRepackHandler() throw();

	/* repack subrequest' s handler*/
	void StagerRepackHandler::handle() throw(castor::exception::Exception);



	/***********************************************************************************************/
	/* virtual functions inherited from IObject                                                   */
	/*********************************************************************************************/
	virtual void setId(u_signed64 id);
	virtual u_signed64 id() const;
	virtual int type() const;
	virtual IObject* clone();
	virtual void print() const;
	virtual void print(std::ostream& stream, std::string indent, castor::ObjectSet& alreadyPrinted) const;




      }; // end StagerRepackHandler class


    }//end namespace dbService
  }//end namespace stager
}//end namespace castor


#endif
