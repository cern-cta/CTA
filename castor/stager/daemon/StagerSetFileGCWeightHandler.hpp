/*******************************************************************************************************/
/* handler for the SetFileGCWeight request, simply call to the stagerService->setFileGCWeight()       */
/* since it isn't job oriented, it inherits from the StagerRequestHandler                            */
/* it always needs to reply to the client                                                           */
/***************************************************************************************************/


#ifndef STAGER_SET_FILE_GC_WEIGHT_HANDLER_HPP
#define STAGER_SET_FILE_GC_WEIGHT_HANDLER_HPP 1

#include "castor/stager/dbService/StagerRequestHandler.hpp"

#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/stager/dbService/StagerReplyHelper.hpp"

#include <iostream>
#include <string>

namespace castor{
  namespace stager{
    namespace dbService{

      class StagerRequestHelper;
      class StagerCnsHelper;
      class SetFileGCWeight;

      class StagerSetFileGCWeightHandler : public StagerRequestHandler{

      private:
	castor::stager::SetFileGCWeight* setFileGCWeight;
     
      public:
	/* constructor */
	StagerSetFileGCWeight::StagerSetFileGCWeightHandler(StagerRequestHelper* stgRequestHelper, StagerCnsHelper* stgCnsHelper, std::string message) throw();
	/* destructor */
	StagerSetFileGCWeight::~StagerSetFileGCWeightHandler() throw();

	/* SetFileGCWeight handle implementation */
	void StagerSetFileGCWeight::handle(void *param) throw();
	
      }
    }//end namespace dbService
  }//end namespace stager
}//end namespace castor



#endif 
