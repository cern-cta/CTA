/*******************************************************************************************************/
/* base class for the 7 job handlers(Get,PrepareToGet,Update,PrepareToUpdate,Put,PrepareToPut,Repack) */
/*****************************************************************************************************/

#include "castor/stager/daemon/RequestHelper.hpp"
#include "castor/stager/daemon/CnsHelper.hpp"
#include "castor/stager/daemon/ReplyHelper.hpp"
#include "castor/stager/daemon/DlfMessages.hpp"
#include "castor/stager/daemon/RequestHandler.hpp"
#include "castor/stager/daemon/JobRequestHandler.hpp"

#include "castor/stager/IStagerSvc.hpp"
#include "castor/Services.hpp"
#include "castor/BaseAddress.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/stager/FileRequest.hpp"
#include "castor/IClient.hpp"
#include "castor/stager/SvcClass.hpp"
#include "castor/stager/CastorFile.hpp"
#include "castor/stager/FileClass.hpp"

#include "castor/stager/SubRequestStatusCodes.hpp"
#include "castor/stager/SubRequestGetNextStatusCodes.hpp"
#include "stager_uuid.h"
#include "stager_constants.h"
#include "Cns_api.h"

#include "Cpwd.h"
#include "Cgrp.h"


#include "castor/stager/DiskCopyForRecall.hpp"
#include "castor/stager/DiskCopyStatusCodes.hpp"

#include "castor/exception/Exception.hpp"

#include "castor/Constants.hpp"

#include "marshall.h"
#include "net.h"

#include "getconfent.h"
#include "Cnetdb.h"

#include "serrno.h"
#include <errno.h>
#include <iostream>
#include <string>
#include <list>
#include <iterator>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


namespace castor{
  namespace stager{
    namespace daemon{
      
      /****************************************************************************************/
      /* job oriented block                                                                  */
      /**************************************************************************************/
      void JobRequestHandler::jobOriented() throw(castor::exception::Exception)
      {
        /* get the fileclass */
        stgCnsHelper->getCnsFileclass();
        /* free the tppols*/
        free(stgCnsHelper->cnsFileclass.tppools);
        /* link the castorFile to the ServiceClass( selecting with stagerService using cnsFilestat.name ) */
        stgRequestHelper->getCastorFileFromSvcClass(stgCnsHelper);
      }
      
    }//end namespace daemon
  }//end namespace stager
}//end namespce castor
