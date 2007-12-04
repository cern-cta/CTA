/******************************************************************************
 *                      LSFSingleton.hpp
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
 * @(#)$RCSfile: LSFSingleton.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2007/12/04 13:16:09 $ $Author: waldron $
 *
 * Singleton used to access LSF related information
 *
 * @author Dennis Waldron
 *****************************************************************************/

#ifndef RMMASTER_LSF_HELPER_HPP
#define RMMASTER_LSF_HELPER_HPP 1

// Include files
#include "castor/exception/Exception.hpp"

// LSF headers
extern "C" {
  #ifndef LSBATCH_H
    #include "lsf/lssched.h"
    #include "lsf/lsbatch.h"
  #endif
}

namespace castor {

  namespace monitoring {
    
    namespace rmmaster {

      /**
       * LSFSingleton singleton class
       */
      class LSFSingleton {
	
      public:
	
	/**
	 * Static method to get this singleton's instance
	 */
	static castor::monitoring::rmmaster::LSFSingleton* getInstance()
	  throw(castor::exception::Exception);
	      
	/**
	 * Get the name of the LSF master used to determine if the rmmaster
	 * daemon is operating in slave or master mode. I.e. if the hostname
	 * of the machine matches the name of the LSF master then we operate
	 * in master mode otherwise we are the slave. In order not to stress
	 * LSF too much the result of this call is cached for up to 5 minutes
	 * @exception Exception in case of error
	 */
	std::string getLSFMasterName()
	  throw(castor::exception::Exception);
	
      private:

	/// This singleton's instance
	static castor::monitoring::rmmaster::LSFSingleton *s_instance;

	/**
	 * Default constructor. Initializes the LSF API
	 * @exception Exception in case of error
	 */
	LSFSingleton() throw (castor::exception::Exception);
	      
	/**
	 * Default destructor
	 */
	virtual ~LSFSingleton() throw() {};

      private:
	
	/// The name of the LSF master
	std::string m_masterName;

	/// The last time the LSF master name was refreshed
	u_signed64 m_lastUpdate;

      };
    
    } // End of namespace rmmaster

  } // End of namespace monitoring

} // End of namespace castor

#endif // RMMASTER_LSF_HELPER_HPP
