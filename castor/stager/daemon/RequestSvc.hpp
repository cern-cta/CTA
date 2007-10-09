
/*************************************/
/* main class for the ___RequestSvc */
/***********************************/

#ifndef REQUEST_SVC_HPP
#define REQUEST_SVC_HPP 1


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
    
      class RequestSvc : public virtual castor::server::SelectProcessThread{
	
      protected:
	StagerRequestHelper* stgRequestHelper;
	StagerCnsHelper* stgCnsHelper;
	/* vector containing the types of subrequest that the specific RequestSvc(Job,Pre,Stg) can process*/
	std::vector<ObjectsIds> types;
	int typeRequest;
	bool fileExist;
	
	
      public: 
	/* empty destructor */
	virtual ~RequestSvc() throw() {};

	/***************************************************************************/
	/* abstract functions inherited from the SelectProcessThread to implement */
	/*************************************************************************/
	virtual castor::IObject* select() throw(castor::exception::Exception) = 0;
	virtual void process(castor::IObject* subRequestToProcess) throw(castor::exception::Exception) = 0;

	/* function to perform the common flow for all RequestSvc (Job, Pre, Stg) */
	void preprocess(castor::stager::SubRequest* subrequest) throw(castor::exception::Exception);

	void handleException(int errorCode, std::string errorMessage);

	
	
      };// end class RequestSvc
      
    }// end dbService
  } // end namespace stager
}//end namespace castor


#endif
