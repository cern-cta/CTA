
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
     
      class PreRequestSvc : public castor::stager::dbService::RequestSvc{
        
      public: 
        /* constructor */
        PreRequestSvc() throw (castor::exception::Exception);
        ~PreRequestSvc() throw() {};
        
        /***************************************************************************/
        /* abstract functions inherited from the SelectProcessThread to implement */
        /*************************************************************************/
        virtual void process(castor::IObject* subRequestToProcess) throw();
        
      private:
      
        /// jobManager host and port
        std::string m_jobManagerHost;
        unsigned m_jobManagerPort;
  
      };// end class PreRequestSvc
      
    }// end dbService

  } // end namespace stager

}//end namespace castor


#endif
