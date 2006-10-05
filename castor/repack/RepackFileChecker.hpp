/******************************************************************************
 *                      RepackFileChecker.hpp
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

#ifndef _REPACKFILECHECKER_HPP_
#define _REPACKFILECHECKER_HPP_

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
	
	class RepackFileChecker : public castor::server::IThread {
		
		public :
		
		  RepackFileChecker(RepackServer* svr);
		  ~RepackFileChecker();
		
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
      * Checks for double tapecopy repacking. This happens whenever there 
      * are >1 tapecopies of a file to be repacked. If the first one is already
      * staged, the user gets a warning in DLF to restart the repack for the 
      * second tape. The reason for this is the creation of the tapecopies of
      * a file, whenever it is recalled. The FILERECALLED PL/SQL creates the 
      * amount of tapecopies which have been submitted as Stager SubRequest till
      * that time. Everyone reaching it after the physical recall, would be 
      * ignored. This is here checked and in case the user informed.
      * @param sreq The RepackSubRequest to check
      * @throws castor::exception::Internal in case of an error
      */
    int checkMultiRepack(RepackSubRequest* sreq)
                                      throw (castor::exception::Internal);
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



#endif //_REPACKFILECHECKER_HPP_
