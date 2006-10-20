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
		 * Stopping the run() method. It is just a flag which is set to false
		 * and the loop is not executed anymore.
		 */
		virtual void stop()				throw();
		
    
   /**
		 * The run method polls the database for new requests. If a request
		 * arrives the stage_files Method is executed.
		 * this can be stopped by calling the stop method, then it is not possible
		 * to start the thread anymore! It has to be destroyed and recreated.
		 */
		virtual void run(void* param)	throw();


		private:
		/**
		 * Stages the files. It retrieves the files for the tape and
     * sends a StagePrepareToGetRequest to the stager. If the submit
     * was successfull the state of the RepackSubRequest is changed 
     * to SUBREQUEST_STAGING.
     * @param sreq The RepackSubRequest to stage in files
     * @throws castor::exeption::Internal in case of an error
		 */
		void stage_files(RepackSubRequest* sreq)
                                          throw (castor::exception::Exception);


    /** private method to send the request to the stager.
      * The passed RepackSubRequest cuuid is updated as soon as the 
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
      * Method to start a Repack process  
      */
    void RepackFileStager::startRepack(RepackSubRequest* sreq);

    /**
      * Method to restart a Repack process
      */
    void RepackFileStager::restartRepack(RepackSubRequest* sreq);


    /**
		 * Pointer to DatabaseHelper instance. Created by the contructor.
		 * Stores and updates RepackRequest.
		 */
		DatabaseHelper* m_dbhelper;
    

    /**
     * Pointer to a FileListHelper instance. Created by the contructor.
     */
    FileListHelper* m_filehelper;

	  /** A pointer to the server instance, which keeps information
     * about Nameserver, stager etc.
     */	
    RepackServer* ptr_server;
	};



	} //END NAMESPACE REPACK
} //END NAMESPACE CASTOR



#endif //_REPACKFILESTAGER_HPP_
