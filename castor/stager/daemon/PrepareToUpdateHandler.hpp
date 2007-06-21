/******************************************************************************************************/
/* StagerPrepareToPutHandler: Constructor and implementation of the PrepareToUpdaterequest's handler */
/****************************************************************************************************/

#ifndef STAGER_PREPARE_TO_UPDATE_HANDLER_HPP
#define STAGER_PREPARE_TO_UPDATE_HANDLER_HPP 1




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


      class StagerPrepareToUpdateHandler : public virtual StagerJobRequestHandler{

      private:
	/* flag to schedule or to recreateCastorFile depending if the file exists... */
	bool toRecreateCastorFile;



      public:
	/* constructor */
	StagerPrepareToUpdateHandler::StagerPrepareToUpdateHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper, bool toRecreateCastorFile) throw(castor::exception::Exception);

	/* destructor */
	StagerPrepareToUpdateHandler::~StagerPrepareToUpdateHandler() throw();

	/* PrepareToUpdate request handler */
	void StagerPrepareToUpdateHandler::handle() throw(castor::exception::Exception);
	

	/***********************************************************************************************/
	/* virtual functions inherited from IObject                                                   */
	/*********************************************************************************************/
	virtual void setId(u_signed64 id);
	virtual u_signed64 id() const;
	virtual int type() const;
	virtual IObject* clone();
	virtual void print() const;
	virtual void print(std::ostream& stream, std::string indent, castor::ObjectSet& alreadyPrinted) const;
	

      }; //end StagerPrepareToUpdateHandler class

    }//end dbService namespace
  }//end stager namespace
}//end castor namespace


#endif
