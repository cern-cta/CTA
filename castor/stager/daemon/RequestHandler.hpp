/****************************************************************************************************************/
/* Base class for all the request.                                                                             */
/* Basically: handle AS METHOD + {StagerRequestHelper, StagerCnsHelper, StagerReplyHelper}      AS ATTRIBUTES */
/*************************************************************************************************************/



#ifndef STAGER_REQUEST_HANDLER_HPP
#define STAGER_REQUEST_HANDLER_HPP 1


#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"
#include "castor/stager/SubRequestStatusCodes.hpp"


#include <iostream>
#include <string>

namespace castor{
  namespace stager{
    namespace dbService{

      class StagerRequestHelper;
      class StagerCnsHelper;
      class StagerReplyHelper;


      class StagerRequestHandler :public castor::IObject{

      protected:
	StagerRequestHelper* stgRequestHelper;
	StagerCnsHelper* stgCnsHelper;
	StagerReplyHelper* stgReplyHelper;
	SubRequestStatusCodes newSubrequestStatus;

	std::string message; /* string containing the error message, needed for exception handler and replyToClient(if necessary) */

      public:
	virtual void StagerRequestHandler::handle(void *param) const = 0;


	

      }//end of StagerRequestHandler class

    }//end of dbService space
  }//end of stager space
}//end of castor space


#endif
