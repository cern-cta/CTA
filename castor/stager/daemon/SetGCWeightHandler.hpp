/*******************************************************************************************************/
/* handler for the SetFileGCWeight request, simply call to the stagerService->setFileGCWeight()       */
/* since it isn't job oriented, it inherits from the StagerRequestHandler                            */
/* it always needs to reply to the client                                                           */
/***************************************************************************************************/


#ifndef STAGER_SET_GC_HANDLER_HPP
#define STAGER_SET_GC_HANDLER_HPP 1

#include "castor/stager/dbService/StagerRequestHandler.hpp"

#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"

#include "u64subr.h"

#include "castor/stager/SetFileGCWeight.hpp"

#include "castor/IObject.hpp"
#include "castor/ObjectSet.hpp"
#include "castor/exception/Exception.hpp"

#include "serrno.h"
#include <errno.h>
#include <iostream>
#include <string>

namespace castor{
  namespace stager{
    namespace dbService{

      class StagerRequestHelper;
      class StagerCnsHelper;
      class SetFileGCWeight;

      class StagerSetGCHandler : public virtual StagerRequestHandler{

     
      public:
	/* constructor */
	StagerSetGCHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper) throw(castor::exception::Exception);
	/* destructor */
	~StagerSetGCHandler() throw();

	/* SetFileGCWeight handle implementation */
	void handle() throw(castor::exception::Exception);




      private:
	castor::stager::SetFileGCWeight* setFileGCWeight;
     
      }; //end StagerSetGCHandler class


    }//end namespace dbService
  }//end namespace stager
}//end namespace castor

#endif 
