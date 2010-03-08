/******************************************************************************
 *                      StatusUpdateHelper.hpp
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
 * @(#)$RCSfile: StatusUpdateHelper.hpp,v $ $Author: waldron $
 *
 * Status update helper class. Shared between the OraRmMasterSvc and the
 * Collector thread of the RmMasterDaemon.
 *
 * @author castor-dev team
 *****************************************************************************/

#ifndef MONITORING_STATUSUPDATEHELPER_HPP
#define MONITORING_STATUSUPDATEHELPER_HPP 1

// Include files
#include "castor/monitoring/ClusterStatus.hpp"
#include "castor/monitoring/rmmaster/LSFStatus.hpp"

namespace castor {

  namespace monitoring {

    // Forward declarations
    class DiskServerStateReport;
    class DiskServerMetricsReport;
    class ClusterStatus;
    namespace admin {
      class DiskServerAdminReport;
      class FileSystemAdminReport;
    }

    namespace rmmaster {

      namespace ora {

        /**
         * Status update helper
         */
        class StatusUpdateHelper {

        public:

          /**
           * Constructor
           * @param clusterStatus pointer to the status of the cluster
           */
          StatusUpdateHelper
          (castor::monitoring::ClusterStatus* clusterStatus);

          /**
           * Destructor
           */
          ~StatusUpdateHelper() {};

          /**
           * Handles state updates; this method is public because
           * it is also used by OraRmMasterSvc.retrieveClusterStatus
           * @param state the new state
           * @param flag to indicate that this is the initialisation
           * phase of the rmmaster daemon and as a consequence the
           * lastStateUpdate attributes in the shared memory should
           * be set to 0.
           * @exception Exception in case of error
           */
          void handleStateUpdate
          (castor::monitoring::DiskServerStateReport* state,
           const bool init = false)
            throw (castor::exception::Exception);

          /**
           * Handles metrics updates
           * @param metrics the new metrics
           * @exception Exception in case of error
           */
          void handleMetricsUpdate
          (castor::monitoring::DiskServerMetricsReport* metrics)
            throw (castor::exception::Exception);

          /**
           * Handles DiskServer admin updates
           * @param admin the new admin report
           * @param ip the address of the client
           * @exception Exception in case of error
           */
          void handleDiskServerAdminUpdate
          (castor::monitoring::admin::DiskServerAdminReport* admin,
           unsigned long ip)
            throw (castor::exception::Exception);

          /**
           * Handles FileSystem admin updates
           * @param admin the new admin report
           * @param ip the address of the client
           * @exception Exception in case of error
           */
          void handleFileSystemAdminUpdate
          (castor::monitoring::admin::FileSystemAdminReport* admin,
           unsigned long ip)
            throw (castor::exception::Exception);

        private:

          /**
           * Gets an iterator on a specific machine from the
           * ClusterStatus map or create one if the machine is
           * not existing in the map yet
           * @param name name of the machine
           * @param it the returned iterator
           * @return true if the machine was found or created successfully,
           * false if the creation failed
           */
          bool getOrCreateDiskServer
          (std::string name,
           castor::monitoring::ClusterStatus::iterator& it) throw();

          /**
           * Gets an iterator on a specific fileSystem from a
           * DiskServerStatus map or create one if the fileSystem is
           * not existing in the map yet
           * @param it dss the DiskServerStatus map
           * @param mountPoint the mountPoint of the fileSystem
           * @param it2 the returned iterator
           * @return true if the fileSystem was found or created successfully,
           * false if the creation failed
           */
          bool getOrCreateFileSystem
          (castor::monitoring::DiskServerStatus& dss,
           std::string mountPoint,
           castor::monitoring::DiskServerStatus::iterator& it2) throw();

          /**
           * Check to see if the diskserver and optionally its mountpoint
           * have files associated to them. If the mountpoint is not provided
           * the check will be executed for all mountpoints associated to the
           * diskserver.
           * @param diskServer the name of the diskServer
           * @param mountPoint the mountPoint of the fileSystem
           * @return true if files exists otherwise false.
           */
          bool checkIfFilesExist
          (std::string diskServer,
           std::string mountPoint = "")
            throw (castor::exception::Exception);

        private:

          // Machine Status List
          castor::monitoring::ClusterStatus* m_clusterStatus;

        };

      } // end of namespace ora

    } // end of namespace rmmaster

  } // end of namespace monitoring

} // end of namespace castor

#endif // MONITORING_STATUSUPDATEHELPER_HPP
