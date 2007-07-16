
/*************************************************************************************/
/* StagerGetHandler: Constructor and implementation of the get subrequest's handler */
/***********************************************************************************/

#ifndef STAGER_GET_HANDLER_HPP
#define STAGER_GET_HANDLER_HPP 1




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

      class StagerGetHandler : public virtual StagerJobRequestHandler{

	
      public:
	/* constructor */
	StagerGetHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper) throw(castor::exception::Exception);
	/* destructor */
	~StagerGetHandler() throw();
	
	/* Get request handler */
	void handle() throw(castor::exception::Exception);


	/*****************************************************************************************/
	/* virtual functions inherited from IObject                                             */
	/***************************************************************************************/
	virtual void setId(u_signed64 id);
	virtual u_signed64 id() const;
	virtual int type() const;
	virtual IObject* clone();
	virtual void print() const;
	virtual void print(std::ostream& stream, std::string indent, castor::ObjectSet& alreadyPrinted) const;

      }; // end StagerGetHandler class


    }//end namespace dbService
  }//end namespace stager
}//end namespace castor



#endif
