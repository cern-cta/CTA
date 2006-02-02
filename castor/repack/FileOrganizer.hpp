#ifndef _FILEORGANIZER_HPP_
#define _FILEORGANIZER_HPP_

#include "RepackCommonHeader.hpp"
#include "FileListHelper.hpp"
#include "DatabaseHelper.hpp"
#include "castor/server/IThread.hpp"
#include "stager_client_api.h"


namespace castor {
	
	namespace repack {

	/**
     * class FileOrganizer
     */
	class FileOrganizer : public castor::server::IThread
	{
		public : 
		
		/**
	     * Empty Constructor
	     */
		FileOrganizer() throw();
		
		/**
	     * Destructor
	     */
		~FileOrganizer() throw();
		
		virtual void stop()				throw();
		virtual void run(void* param)	throw();


		private:
		/**
		 * Stages the files
		 */
		void stage_files(RepackSubRequest* sreq) throw();
		void sortReadOrder(std::vector<u_signed64>* fileidlist) throw();
		
		castor::repack::DatabaseHelper* m_dbhelper;
		bool m_run;
		
	};



	} //END NAMESPACE REPACK
} //END NAMESPACE CASTOR



#endif //_FILEORGANIZER_HPP_
