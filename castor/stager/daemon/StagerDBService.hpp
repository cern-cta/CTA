
/*********************************************************************************************************/
/* cpp version of the "stager_db_service.c" represented by a thread calling the right request's handler */
/*******************************************************************************************************/

#ifndef STAGER_DB_SERVICE_HPP
#define STAGER_DB_SERVICE_HPP 1


#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/server/SelectProcessThread.hpp"
#include "castor/Constants.hpp"


#include "castor/exception/Exception.hpp"
#include "castor/IObject.hpp"

#include <string>
#include <iostream>

namespace castor {
  namespace stager{
    namespace dbService {
      
      class StagerRequestHelper;
      class StagerCnsHelper;
    
      class StagerDBService : public virtual castor::server::SelectProcessThread{
	
      private:
	StagerRequestHelper* stgRequestHelper;
	StagerCnsHelper* stgCnsHelper;
	
	
	
      public: 
	/* constructor */
	StagerDBService() throw();
	~StagerDBService() throw();

	virtual void run(void* param) throw();
	virtual void stop() throw() {};
	
	/***************************************************************************/
	/* abstract functions inherited from the SelectProcessThread to implement */
	/*************************************************************************/
	castor::IObject* StagerDBService::select() throw(castor::exception::Exception);
	void StagerDBService::process(castor::IObject* subRequestToProcess) throw(castor::exception::Exception);
	
	
	std::vector<ObjectsIds> types;
	
	
      };// end class StagerDBService
      
    }// end dbService
  } // end namespace stager
}//end namespace castor


#endif
