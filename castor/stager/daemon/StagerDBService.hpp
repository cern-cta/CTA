
/*********************************************************************************************************/
/* cpp version of the "stager_db_service.c" represented by a thread calling the right request's handler */
/*******************************************************************************************************/

#ifndef STAGER_DB_SERVICE_HPP
#define STAGER_DB_SERVICE_HPP 1


#include "StagerRequestHelper.hpp"
#include "StagerCnsHelper.hpp"
#include "../../server/SelectProcessThread.hpp"
#include "../../Constants.hpp"


#include "../../exception/Exception.hpp"

#include <string>
#include <iostream>

namespace castor {
  namespace stager{
    namespace dbService {
      
    
      class StagerDBService : public virtual castor::server::SelectProcessThread{
	
      private:
	StagerRequestHelper* stgRequestHelper;
	StagerCnsHelper* stgCnsHelper;
	
	
	
      public: 
	//constructor
	StagerDBService::StagerDBService() throw();
	StagerDBService::~StagerDBService() throw();

	void StagerDBService::run(void* param) throw();
	virtual void StagerDBService::stop() throw() const;
	
	
	std::vector<ObjectsIds> types;
	
	
      };// end class StagerDBService
      
    }// end dbService
  } // end namespace stager
}//end namespace castor


#endif
