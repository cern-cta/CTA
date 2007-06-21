/****************************************************************************************************************************/
/* handler for the Update subrequest, since it is jobOriented, it uses the mostly part of the StagerJobRequestHandler class*/
/* depending if the file exist, it can follow the huge flow (jobOriented, as Get) or a small one                          */
/*************************************************************************************************************************/


#ifndef STAGER_UPDATE_HANDLER_HPP
#define STAGER_UPDATE_HANDLER_HPP 1


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

#include "../../IObject.hpp"
#include "../../ObjectSet.hpp"
#include "../../exception/Exception.hpp"

#include <iostream>
#include <string>


namespace castor{
  namespace stager{
    namespace dbService{

      class StagerRequestHelper;
      class StagerCnsHelper;

      class StagerUpdateHandler : public virtual StagerJobRequestHandler{

      private:
	/* flag to schedule or to recreateCastorFile depending if the file exists... */
	bool toRecreateCastorFile;
	
      public:
	/* constructor */
	StagerUpdateHandler::StagerUpdateHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper, bool toRecreateCastorFile) throw(castor::exception::Exception);
	/* destructor */
	StagerUpdateHandler::~StagerUpdateHandler() throw();

	/* handler for the Update request  */
	void StagerUpdateHandler::handle() throw(castor::exception::Exception);



      	/***********************************************************************************************/
	/* virtual functions inherited from IObject                                                   */
	/*********************************************************************************************/
	virtual void setId(u_signed64 id);
	virtual u_signed64 id() const;
	virtual int type() const;
	virtual IObject* clone();
	virtual void print() const;
	virtual void print(std::ostream& stream, std::string indent, castor::ObjectSet& alreadyPrinted) const;
	
      }; //end StagerUpdateHandler class


    }//end namespace dbService
  }//end namespace stager
}//end namespace castor



#endif
