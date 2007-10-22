
/***************************************************************************************************/
/* service to perform the PrepareTo Request (PrepareToPut, PrepareToGet, PrepareToUpdate, Repack) */
/*************************************************************************************************/

#ifndef PRE_REQUEST_SVC_HPP
#define PRE_REQUEST_SVC_HPP 1

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
      
     
    
      class PreRequestSvc : public virtual castor::stager::dbService::RequestSvc{
	
      public: 
	/* constructor */
	PreRequestSvc(std::string jobManagerHost, int jobManagerPort) throw();
	~PreRequestSvc() throw();

	/***************************************************************************/
	/* abstract functions inherited from the SelectProcessThread to implement */
	/*************************************************************************/
	virtual castor::IObject* select() throw(castor::exception::Exception);
	virtual void process(castor::IObject* subRequestToProcess) throw(castor::exception::Exception);
  
      private:
        
        /// jobManager host and port
        std::string m_jobManagerHost;
        int m_jobManagerPort;

      };// end class PreRequestSvc
      
    }// end dbService
  } // end namespace stager
}//end namespace castor


#endif
