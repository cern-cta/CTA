
/*********************************************************************************************************/
/* cpp version of the "stager_db_service.c" represented by a thread calling the right request's handler */
/*******************************************************************************************************/

#ifndef PRE_REQUEST_SVC_HPP
#define PRE_REQUEST_SVC_HPP 1


#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/server/SelectProcessThread.hpp"
#include "castor/Constants.hpp"

#include "serrno.h"
#include <errno.h>

#include "castor/exception/Exception.hpp"
#include "castor/IObject.hpp"

#include <string>
#include <iostream>

namespace castor {
  namespace stager{
    namespace dbService {
      
      class StagerRequestHelper;
      class StagerCnsHelper;
    
      class PreRequestSvc : public virtual castor::server::SelectProcessThread{
	
      private:
	StagerRequestHelper* stgRequestHelper;
	StagerCnsHelper* stgCnsHelper;
	
	
	
      public: 
	/* constructor */
	PreRequestSvc() throw();
	~PreRequestSvc() throw();

	/***************************************************************************/
	/* abstract functions inherited from the SelectProcessThread to implement */
	/*************************************************************************/
	virtual castor::IObject* select() throw(castor::exception::Exception);
	virtual void process(castor::IObject* subRequestToProcess) throw(castor::exception::Exception);


	
	std::vector<ObjectsIds> types;
	
	
      };// end class PreRequestSvc
      
    }// end dbService
  } // end namespace stager
}//end namespace castor


#endif
