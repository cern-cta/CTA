
/******************************************************************************/
/* Service to perform the pure Stager Request (SetFileGCWeight, Rm, PutDone) */
/****************************************************************************/

#ifndef STG_REQUEST_SVC_HPP
#define STG_REQUEST_SVC_HPP 1


#include "castor/stager/dbService/RequestSvc.hpp"
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
      
     
      class StgRequestSvc : public virtual castor::stager::dbService::RequestSvc{
	
      public: 
	/* constructor */
	StgRequestSvc() throw();
	~StgRequestSvc() throw();

	/***************************************************************************/
	/* abstract functions inherited from the SelectProcessThread to implement */
	/*************************************************************************/
	virtual void process(castor::IObject* subRequestToProcess) throw();


      };// end class StgRequestSvc
      
    }// end dbService
  } // end namespace stager
}//end namespace castor


#endif
