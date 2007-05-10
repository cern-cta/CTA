
/*********************************************************************************************************/
/* cpp version of the "stager_db_service.c" represented by a thread calling the right request's handler */
/*******************************************************************************************************/

#ifndef STAGER_DB_SERVICE_HPP
#define STAGER_DB_SERVICE_HPP 1


#include "castor/stager/dbService/StagerRequestHelper.hpp"
#include "castor/stager/dbService/StagerCnsHelper.hpp"
#include "castor/server/dbService/SelectProcessThread.hpp"

#include <string>
#include <iostream>

namespace castor {
  namespace stager{
    namespace dbService {
      
    
      class StagerDBService : public castor::server::SelectProcessThread{
	
      private:
	StagerRequestHelper* stgRequestHelper;
	StagerCnsHelper* stgCnsHelper;
	
	std::string message;
	
      public: 
	//constructor
	StagerDBService::StagerDBService();
	StagerDBService::~StagerDBService();
	
	
	std::vector<ObjectsIds> types;
	
	
      }	// end class StagerDBService
      
    }// end dbService
  } // end namespace stager
}//end namespace castor


#endif
