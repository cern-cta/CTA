/**********************************************************************************************************************/
/* helper class containing the objects and methods which interact to performe the response to the client             */
/* it is needed to provide:                                                                                         */
/*     - a common place where its objects can communicate                                                          */
/* it is always used by: StagerPrepareToGet,Repack, PrepareToPut, PrepareToUpdate, Rm, SetFileGCWeight, PutDone   */
/* just in case of error, by all the handlers                                                                    */
/****************************************************************************************************************/

#ifndef STAGER_REPLY_HELPER_HPP
#define STAGER_REPLY_HELPER_HPP 1

#include "castor/stager/daemon/StagerRequestHelper.hpp"
#include "castor/rh/IOResponse.hpp"
#include "castor/replier/RequestReplier.hpp"
#include "castor/stager/FileRequest.hpp"
#include "castor/stager/IStagerSvc.hpp"
#include "castor/exception/Exception.hpp"

#include "castor/stager/SubRequestStatusCodes.hpp"

#include "u64subr.h"
#include "serrno.h"
#include "Cns_struct.h"
#include "Cns_api.h"


#include "castor/ObjectSet.hpp"
#include "castor/BaseObject.hpp"

#include <iostream>
#include <string>


namespace castor{
  namespace stager{
    namespace daemon{
      
      
      /* forward declaration */
      class castor::rh::IOResponse;       
      class castor::replier::RequestReplier;
      class StagerRequestHelper;
      
      
      
      class StagerReplyHelper : public virtual castor::BaseObject{
        
        public:
        
        castor::rh::IOResponse *ioResponse;
        castor::replier::RequestReplier *requestReplier;
        
        /* constructor  */
        StagerReplyHelper() throw(castor::exception::Exception);
        /* destructor */
        ~StagerReplyHelper() throw();
        
        
        
        /****************************************************************************/
        /* set fileId, reqAssociated (reqId()), castorFileName,newSubReqStatus,    */
        /**************************************************************************/
        void setAndSendIoResponse(StagerRequestHelper* stgRequestHelper, Cns_fileid* cnsFileid, int errorCode, std::string errorMessage) throw(castor::exception::Exception);
        
        
        /*********************************************************************************************/
        /* check if there is any subrequest left and send the endResponse to client if it is needed */
        /*******************************************************************************************/
        void endReplyToClient(StagerRequestHelper* stgRequestHelper) throw(castor::exception::Exception);
        
        
      }; // end StagerReplyHelper  
      
      
    }//end namespace daemon
  }//end namespace stager
}//end namespace castor


#endif
