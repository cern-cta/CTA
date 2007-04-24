/******************************************************************************
 *                      HeartbeatThread.hpp
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
 * @(#)$RCSfile: HeartbeatThread.hpp,v $ $Revision: 1.2 $ $Release$ $Date: 2007/04/24 14:08:08 $ $Author: itglp $
 *
 * The Heartbeat thread of the rmMasterDaemon is responsible for checking all
 * disk servers in shared memory and automatically disabling them if no data
 * has been received from them with X number of seconds.
 *
 * @author castor-dev team
 *****************************************************************************/

#ifndef RMMASTER_HEARTBEATTHREAD_HPP
#define RMMASTER_HEARTBEATTHREAD_HPP 1

#include "castor/server/IThread.hpp"
#include "castor/monitoring/ClusterStatus.hpp"

namespace castor {
  
  namespace monitoring {
    
    // Forward declarations
    class ClusterStatus;

    namespace rmmaster {
      
      /**
       * Heartbeat thread.
       */
      class HeartbeatThread : public castor::server::IThread {

      public:

	/**
	 * Constructor
	 * @param clusterStatus pointer to the status of the cluster
	 */
	HeartbeatThread(ClusterStatus* clusterStatus);
	
	/**
	 * Method called periodically to check whether an rmNodeDaemon is alive
	 */
	virtual void run(void *param) throw();

	/// not implemented
	virtual void stop() {};

      private:

	/// Machine Status List
	castor::monitoring::ClusterStatus* m_clusterStatus;

	/// The startup time of the thread
	u_signed64 m_startup;

	/// The timeout after which a diskserver is declared down
	int timeout;
      };

    } // end of namespace rmmaster
    
  } // end of namespace monitoring
  
} // end of namespace castor

#endif // RMMASTER_HEARTBEATTHREAD_HPP
