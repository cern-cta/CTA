/*******************************************************************************************************/
/* handler for the SetFileGCWeight request, simply call to the stagerService->setFileGCWeight()       */
/* since it isn't job oriented, it inherits from the RequestHandler                            */
/* it always needs to reply to the client                                                           */
/***************************************************************************************************/


#ifndef STAGER_SET_GC_HANDLER_HPP
#define STAGER_SET_GC_HANDLER_HPP 1

#include "castor/stager/daemon/RequestHandler.hpp"

#include "castor/stager/daemon/RequestHelper.hpp"
#include "castor/stager/daemon/CnsHelper.hpp"
#include "castor/stager/daemon/ReplyHelper.hpp"

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
    namespace daemon{

      class RequestHelper;
      class CnsHelper;
      class SetFileGCWeight;

      class SetGCWeightHandler : public virtual RequestHandler{

     
      public:
	/* constructor */
	SetGCWeightHandler(RequestHelper* stgRequestHelper) throw(castor::exception::Exception);
	/* destructor */
	~SetGCWeightHandler() throw();

	/* SetFileGCWeight handle implementation */
	void handle() throw(castor::exception::Exception);

     
      }; //end SetGCWeightHandler class


    }//end namespace daemon
  }//end namespace stager
}//end namespace castor

#endif 
