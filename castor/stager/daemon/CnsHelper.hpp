/************************************************************************/
/* container for the c methods and structures related with the cns_api */
/**********************************************************************/

#ifndef STAGER_CNS_HELPER_HPP
#define STAGER_CNS_HELPER_HPP 1

#include <sys/types.h>
#include <sys/stat.h>
#include "Cns_api.h"
#include "Cns_struct.h"
#include "Cglobals.h"
#include "serrno.h"
#include <errno.h>
#include <iostream>
#include <string>

#include <fcntl.h>

#include "castor/BaseObject.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/stager/SvcClass.hpp"
#include "castor/stager/FileRequest.hpp"
#include "castor/Constants.hpp"

#include "castor/dlf/Dlf.hpp"
#include "castor/dlf/Param.hpp"
#include "castor/stager/daemon/DlfMessages.hpp"


namespace castor{
  namespace stager{
    namespace daemon{

      class CnsHelper : public virtual castor::BaseObject{

      private:

        Cuuid_t requestUuid;

      public:

        struct Cns_fileid cnsFileid;
        struct Cns_filestat cnsFilestat;
        struct Cns_fileclass cnsFileclass;

        uid_t euid;/* stgRequestHelper->fileRequest->euid() */
        uid_t egid;/* stgRequestHelper->fileRequest->egid() */

        CnsHelper(Cuuid_t requestUuid) throw(castor::exception::Exception);
        ~CnsHelper() throw();


        /****************************************************************************************/
        /* get the Cns_fileclass needed to create the fileClass object using cnsFileClass.name */
        void getCnsFileclass() throw(castor::exception::Exception);

        /***************************************************************/
        /* set the user and group id needed to call the Cns functions */
        /*************************************************************/
        void cnsSetEuidAndEgid(castor::stager::FileRequest* fileRequest) throw(castor::exception::Exception);


        /**
         * checks the existence of the file in the nameserver, and creates it if the request allows for
         * creation. Internally sets the fileId and nsHost for the file.
         * @param subReq the subRequest being processed
         * @param svcClass the service class to which it belongs
         * @return true if the file has been created
         * @throw exception when the file does not exist and the user or the request don't allow for creation
         */
        bool checkFileOnNameServer(castor::stager::SubRequest* subReq, castor::stager::SvcClass* svcClass)
          throw(castor::exception::Exception);

      }; // end CnsHelper class
    }//end namespace daemon
  }// end namespace stager
}// end namespace castor


#endif

