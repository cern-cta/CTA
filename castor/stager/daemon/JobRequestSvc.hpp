
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
      
      
      
      class JobRequestSvc : public castor::stager::dbService::RequestSvc{
        
        public: 
        /* constructor */
        JobRequestSvc(std::string jobManagerHost, int jobManagerPort) throw();
        ~JobRequestSvc() throw() {};
        
        /***************************************************************************/
        /* abstract functions inherited from the SelectProcessThread to implement */
        /*************************************************************************/
        
        virtual void process(castor::IObject* subRequestToProcess) throw();
        
        /// jobManager host and port
        std::string m_jobManagerHost;
        int m_jobManagerPort;
        
        
      };// end class JobRequestSvc
      
    }// end dbService
  } // end namespace stager
}//end namespace castor


#endif
