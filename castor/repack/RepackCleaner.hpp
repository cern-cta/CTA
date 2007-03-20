/******************************************************************************
 *                      RepackCleaner.hpp
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 *
 *
 *
 * @author Felix Ehm
 *****************************************************************************/

#ifndef _REPACKCLEANER_HPP_
#define _REPACKCLEANER_HPP_

#include "RepackCommonHeader.hpp"
#include "DatabaseHelper.hpp"
#include "FileListHelper.hpp"
#include "castor/server/IThread.hpp"
#include "stager_client_api.h"
#include "castor/stager/SubRequestStatusCodes.hpp"


namespace castor {
	namespace repack {
  
  /** forward declaration */
  class RepackServer;	
	
	class RepackCleaner : public castor::server::IThread {
		
		public :
		
		  RepackCleaner(RepackServer* svr);
		  ~RepackCleaner() throw();
		
		  /** 
		   * Implementation from IThread
		   */
		  virtual void run(void* param) throw();
		  /** 
		   * Implementation from IThread
		   */
	    virtual void stop() throw();
		
		private:
			

      /**
			 * Finishes a RepackSubRequest.It calls 'removeFilesFromStager' to remove 
       * the files from stager. Then the status is set to SUBREQUEST_DONE and updated in the DB.
       * @param sreq The RepackSubRequest to cleanup
       * @throws exception in case on an error.
			 */
			int cleanupTape(RepackSubRequest* sreq) throw(castor::exception::Exception);
		  

      /**
        * Removes the files for this RepackSubRequest from the stager. In fact
        * it sends a stage_rm command with the filelist.
        */
      void removeFilesFromStager(RepackSubRequest* sreq) throw(castor::exception::Exception);
		
			/**
			 * The DatabaseHelper for updatting finished jobs in the Repack Tables
			 */
			DatabaseHelper *m_dbhelper;
      
      /**
       * Pointer to the server instance, which I was added to.
       */
      RepackServer* ptr_server;
	};
		
		
	}
}



#endif //_REPACKCLEANER_HPP_
