
/*************************************/
/* main class for the ___RequestSvc */
/***********************************/

#ifndef REQUEST_SVC_HPP
#define REQUEST_SVC_HPP 1

#include "castor/server/SelectProcessThread.hpp"
#include "castor/Constants.hpp"

#include "castor/stager/dbService/StagerRequestHelper.hpp"

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
      
      class RequestSvc : public virtual castor::server::SelectProcessThread{
	
      protected:
	std::string nameRequestSvc;
	
	/* vector containing the types of subrequest that the specific RequestSvc(Job,Pre,Stg) can process*/
	std::vector<ObjectsIds> types;
	int typeRequest;
	
      public: 
	/* empty destructor */
	virtual ~RequestSvc() throw() {};

	/***************************************************************************/
	/* abstract functions inherited from the SelectProcessThread to implement */
	/*************************************************************************/
	virtual castor::IObject* select() throw(castor::exception::Exception) = 0;
	virtual void process(castor::IObject* subRequestToProcess) throw(castor::exception::Exception) = 0;


	void handleException(StagerRequestHelper* stgRequestHelper,StagerCnsHelper* stgCnsHelper, int errorCode, std::string errorMessage);

	
	
      };// end class RequestSvc
      
    }// end dbService
  } // end namespace stager
}//end namespace castor


#endif
