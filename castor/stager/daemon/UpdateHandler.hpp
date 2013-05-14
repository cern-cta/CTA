/****************************************************************************************************************************/
/* handler for the Update subrequest, since it is jobOriented, it uses the mostly part of the OpenRequestHandler class*/
/* depending if the file exist, it can follow the huge flow (jobOriented, as Get) or a small one                          */
/*************************************************************************************************************************/


#ifndef STAGER_UPDATE_HANDLER_HPP
#define STAGER_UPDATE_HANDLER_HPP 1


#include "serrno.h"
#include <errno.h>
#include <iostream>
#include <string>

#include "stager_constants.h"
#include "Cns_api.h"
#include "u64subr.h"
#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/IObject.hpp"
#include "castor/ObjectSet.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/stager/daemon/RequestHelper.hpp"
#include "castor/stager/daemon/ReplyHelper.hpp"
#include "castor/stager/daemon/OpenRequestHandler.hpp"


namespace castor{
  namespace stager{
    namespace daemon{
      
      
      class UpdateHandler : public OpenRequestHandler {
        
      public:
      
        /* constructor */
        UpdateHandler(RequestHelper* reqHelper) throw(castor::exception::Exception) :
          OpenRequestHandler(reqHelper) {};
        /* destructor */
        ~UpdateHandler() throw() {};
        
        /* handler for the Update request  */
        virtual bool handle() throw(castor::exception::Exception);
        
      }; //end UpdateHandler class
      
      
    }//end namespace daemon
  }//end namespace stager
}//end namespace castor



#endif
