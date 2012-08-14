/******************************************************************************
 *                      RmMasterDaemon.hpp
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
 * @(#)$RCSfile$ $Revision$ $Release$ $Date$ $Author$
 *
 * The monitoring resource master daemon is resonsible for collecting all
 * input from the diskservers and updating both the database and the rmmaster
 * shared memory. It also receives UDP messages migration and recall
 * processes whenever a stream is open/closed
 *
 * @author castor-dev team
 *****************************************************************************/

#ifndef RMMASTER_RMMASTERDAEMON_HPP
#define RMMASTER_RMMASTERDAEMON_HPP 1

// Include Files
#include "castor/server/BaseDaemon.hpp"
#include "castor/exception/Exception.hpp"

namespace castor {

  // Forward Declarations
  namespace stager {
    class IStagerSvc;
  }

  namespace monitoring{

    // Forward Declarations
    class ClusterStatus;

    namespace rmmaster{

      /**
       * Castor RmMaster daemon.
       */
      class RmMasterDaemon : public castor::server::BaseDaemon {

      public:

        /**
         * Constructor
         */
        RmMasterDaemon();

        /**
         * Destructor
         */
        virtual ~RmMasterDaemon() throw() {};

        /**
         * Accessor to the Cluster status
         */
        castor::monitoring::ClusterStatus* clusterStatus() {
          return m_clusterStatus;
        }

      private:

        /// The shared memory area
        castor::monitoring::ClusterStatus* m_clusterStatus;

      };

    } // end of namespace rmmaster

  } // end of namespace monitoring

} // end of namespace castor

#endif // RMMASTER_RMMASTERDAEMON_HPP
