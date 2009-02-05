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

#include "castor/server/IThread.hpp"
#include "RepackServer.hpp"

namespace castor {
	namespace repack {
  
  /** forward declaration */
  class RepackServer;	
	
	class RepackFileChecker : public castor::server::IThread {
		
		public :
		
		  RepackFileChecker(RepackServer* svr);
		  ~RepackFileChecker() throw();
                  
		  ///empty initialization
		  virtual void init() {};

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
                   * Pointer to the server instance, which I was added to.
                   */
                   RepackServer* ptr_server;
	};
		
		
	}
}



#endif //_REPACKFILECHECKER_HPP_
