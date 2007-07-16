/************************************************************************************/
/* handler for the Rm subrequest, simply call to the stagerService->stageRm()      */
/* since it isn't job oriented, it inherits from the StagerRequestHandler         */
/*********************************************************************************/


#ifndef STAGER_RM_HANDLER_HPP
#define STAGER_RM_HANDLER_HPP 1

#include "castor/stager/dbService/StagerRequestHandler.hpp"

#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"

#include "u64subr.h"

#include "castor/stager/IStagerSvc.hpp"
#include "castor/stager/SubRequest.hpp"

#include "castor/IObject.hpp"
#include "castor/ObjectSet.hpp"
#include "castor/exception/Exception.hpp"

#include <iostream>
#include <string>



namespace castor{
  namespace stager{
    namespace dbService{

      class StagerRequestHelper;
      class StagerCnsHelper;

      class StagerRmHandler : public virtual StagerRequestHandler{
	
      public:
	/* constructor */
	StagerRmHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper) throw(castor::exception::Exception);
	/* destructor */
	~StagerRmHandler() throw();

	/* rm subrequest handler */
	void handle() throw(castor::exception::Exception);
     
	

      private: 
	u_signed64 svcClassId;
	
      }; // end StagerRmHandler class


    }//end namespace dbService 
  }//end namespace stager
}//end namespace castor



#endif
