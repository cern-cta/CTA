#ifndef _REPACKFILESTAGER_HPP_
#define _REPACKFILESTAGER_HPP_

#include "RepackCommonHeader.hpp"
#include "FileListHelper.hpp"
#include "DatabaseHelper.hpp"
#include "castor/server/IThread.hpp"
#include "stager_client_api.h"
#include <common.h>

namespace castor {
	
	namespace repack {

	/**
     * class RepackFileStager
     */
	class RepackFileStager : public castor::server::IThread
	{
		public : 
		
		/**
	     * Empty Constructor
	     */
		RepackFileStager() throw ();
		
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
		 * Stages the files
		 */
		void stage_files(RepackSubRequest* sreq) throw();
		void sortReadOrder(std::vector<u_signed64>* fileidlist) throw();

		/**
		 * Pointer to DatabaseHelper Instance. Created by the contructor.
		 * Stores and updates RepackRequest.
		 */
		castor::repack::DatabaseHelper* m_dbhelper;
		castor::repack::FileListHelper* m_filehelper;

		/**
		 * Flag to implement the stop() method.
		 */
		bool m_run;
		
		/**
		 * The nameserver
		 */
		std::string* m_ns;
		
		/**
		 * The polling time for the DB, standard is 10sec.
		 */
		unsigned long polling_time;
	};



	} //END NAMESPACE REPACK
} //END NAMESPACE CASTOR



#endif //_REPACKFILESTAGER_HPP_
