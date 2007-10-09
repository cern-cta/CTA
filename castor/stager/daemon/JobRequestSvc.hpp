
/**********************************************************/
/* Service to perform the JOB Request (Get, Update, Put) */
/********************************************************/

#ifndef JOB_REQUEST_SVC_HPP
#define JOB_REQUEST_SVC_HPP 1


#include "castor/stager/dbService/RequestSvc.hpp"
#include "castor/IObject.hpp"
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
    
      
    
      class JobRequestSvc : public virtual castor::stager::dbService::RequestSvc{

	
      public: 
	/* constructor */
	JobRequestSvc() throw();
	~JobRequestSvc() throw();

	/***************************************************************************/
	/* abstract functions inherited from the SelectProcessThread to implement */
	/*************************************************************************/
	virtual castor::IObject* select() throw(castor::exception::Exception);
	virtual void process(castor::IObject* subRequestToProcess) throw(castor::exception::Exception);



      };// end class JobRequestSvc
      
    }// end dbService
  } // end namespace stager
}//end namespace castor


#endif
