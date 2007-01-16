/******************************************************************************
 *                      UpdateThread.hpp
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
 * @(#)$RCSfile: UpdateThread.hpp,v $ $Author: sponcec3 $
 *
 * The Update thread of the RmMaster daemon.
 * It receives updates from the stager about openings and closings
 * of streams and updates the memory representation accordingly
 *
 * @author castor-dev team
 *****************************************************************************/

#ifndef RMMASTER_UPDATETHREAD_HPP
#define RMMASTER_UPDATETHREAD_HPP 1

#include "castor/server/IThread.hpp"
#include "castor/monitoring/ClusterStatus.hpp"

namespace castor {

  namespace monitoring {

    // Forward declarations
    class StreamReport;
    class ClusterStatus;

    namespace rmmaster {

      /**
       * Update  tread.
       */
      class UpdateThread : public castor::server::IThread {

      public:

        /**
         * constructor
         * @param clusterStatus pointer to the status of the cluster
         */
        UpdateThread(castor::monitoring::ClusterStatus* clusterStatus);

        /**
         * Method called once per request, where all the code resides
         * @param param the socket obtained from the calling thread pool
         */
        virtual void run(void *param) throw();

        /// not implemented
        virtual void stop() {};

      private:

        /**
         * handles stream reports
         * @param reprot the stream report
         */
        void handleStreamReport
        (castor::monitoring::StreamReport* report)
          throw (castor::exception::Exception);

      private:

        // Machine Status List
        castor::monitoring::ClusterStatus* m_clusterStatus;

      };

    } // end of namespace rmmaster

  } // end of namespace monitoring

} // end of namespace castor

#endif // RMMASTER_UPDATETHREAD_HPP
