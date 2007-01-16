#ifndef _REPACKFILESTAGER_HPP_
#define _REPACKFILESTAGER_HPP_

#include "RepackCommonHeader.hpp"
#include "FileListHelper.hpp"
#include "DatabaseHelper.hpp"
#include "castor/server/IThread.hpp"
#include "stager_client_api.h"
#include <common.h>
/* for sending the request to stager */
#include "castor/stager/StageRepackRequest.hpp"
#include "castor/stager/SubRequest.hpp"
#include "castor/rh/Response.hpp"
#include "castor/rh/FileResponse.hpp"
#include "castor/client/VectorResponseHandler.hpp"
#include "castor/client/BaseClient.hpp"
#include "castor/stager/RequestHelper.hpp"
/*************************************/

namespace castor {

  namespace repack {

    /** forward declaration */
    class RepackServer;


    /**
     * class RepackFileStager
     */
    class RepackFileStager : public castor::server::IThread
    {
    public :

      /**
       * Constructor.
       * @param srv The refernce to the constructing RepackServer Instance.
       */
      RepackFileStager(RepackServer* srv);

      /**
       * Destructor
       */
      ~RepackFileStager() throw();

      /**
       * Inherited from IThread : not implemented
       */
      virtual void stop() throw();

      /**
       * Checks the DB for new RepackSubRequests in TOBESTAGED/RESTART and
       * performs the procedure.
       * (execute startRepack/restartRepack method, described below).
       */
      virtual void run(void* param) throw();


    private:

      /**
       * Stages the files. It retrieves the files for the tape and
       * sends a StageRepackRequest to the stager. If the submit
       * was successfull the state of the RepackSubRequest is changed
       * to SUBREQUEST_STAGING.
       * @param sreq The RepackSubRequest to stage in files
       * @throws castor::exeption::Internal in case of an error
       */
      void stage_files(RepackSubRequest* sreq) throw (castor::exception::Exception);


      /** Sends the request to the stager.
       * The  RepackSubRequest cuuid is updated as soon as the
       * RequestHandler from the Stager answers.
       * @param rsreq The RepackSubRequest to update the cuuid
       * @param req The Request to send
       * @param reqId the returned request id (cuuid) from stager
       * @param opts The Stager options struct
       * @returns The Number of files for which the stager request failed.
       * @throws castor::exeption::Internal in case of an error
       */
      int sendStagerRepackRequest( RepackSubRequest* rsreq,
                                   castor::stager::StageRepackRequest* req,
                                   std::string *reqId,
                                   struct stage_options* opts
                                   )
        throw (castor::exception::Exception);

      /**
       * Excutes stage_files and updates the RepackSubRequest. In case
       * stage_files gives an error the state is set to FAILED.
       */
      void startRepack(RepackSubRequest* sreq);


      /**
       * Restarts a RepackSubRequest. The RepackMonitor is used to get the
       * latest stats for this repack request and if files in invalid found,
       * those are send as a new StageRepackRequest.
       * If no answers are found, the NameServer is contacted for the remaining
       * files on tape and for those a new StageRepackReques is send.
       * In both cases the Cuuid is updated and the stats set to '0';
       */
      void restartRepack(RepackSubRequest* sreq);


      /**
       * Pointer to DatabaseHelper instance. Created by the contructor.
       * Stores and updates RepackRequest.
       */
      DatabaseHelper* m_dbhelper;


      /**
       * Pointer to a FileListHelper instance. Created by the contructor.
       */
      FileListHelper* m_filehelper;

      /**
       * A pointer to the server instance, which keeps information
       * about Nameserver, stager etc.
       */
      RepackServer* ptr_server;
    };



  } //END NAMESPACE REPACK
} //END NAMESPACE CASTOR



#endif //_REPACKFILESTAGER_HPP_
