/******************************************************************************
 *                      RepackSynchroniser.hpp
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

#ifndef _REPACKSYNCHRONISER_HPP_
#define _REPACKSYNCHRONISER_HPP_

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
	
	class RepackSynchroniser : public castor::server::IThread {
		
		public :
		
		  RepackSynchroniser(RepackServer* svr);
		  ~RepackSynchroniser();
		
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



#endif //_REPACKSYNCHRONISER_HPP_
