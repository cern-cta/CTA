/******************************************************************************
 *            OraRmMasterSvc.hpp
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
 * @(#)$RCSfile: OraRmMasterSvc.hpp,v $ $Revision: 1.6 $ $Release$ $Date: 2009/01/05 15:53:57 $ $Author: sponcec3 $
 *
 * Implementation of the IRmMasterSvc for Oracle
 *
 * @author castor dev team
 *****************************************************************************/

#ifndef ORA_ORARMMASTERSVC_HPP
#define ORA_ORARMMASTERSVC_HPP 1

// Include Files
#include "castor/BaseSvc.hpp"
#include "castor/ICnvSvc.hpp"
#include "castor/db/newora/OraCommonSvc.hpp"
#include "castor/monitoring/rmmaster/IRmMasterSvc.hpp"
#include "occi.h"
#include "castor/monitoring/ClusterStatus.hpp"

namespace castor {

  namespace monitoring {

    namespace rmmaster {

      namespace ora {

        /**
         * Implementation of the IRmMasterSvc for Oracle
         */
        class OraRmMasterSvc : public castor::db::ora::OraCommonSvc,
             public virtual castor::monitoring::rmmaster::IRmMasterSvc {

        public:

          /**
           * Default constructor
           */
          OraRmMasterSvc(const std::string name, castor::ICnvSvc* cnvSvc = 0);

          /**
           * Default destructor
           */
          virtual ~OraRmMasterSvc() throw();

          /**
           * Get the service id
           */
          virtual inline unsigned int id() const;

          /**
           * Get the service id
           */
          static unsigned int ID();

          /**
           * Reset the converter statements.
           */
          void reset() throw ();

        public:

          /**
           * Stores the current ClusterStatus into the stager database
           * @param clusterStatus the ClusterStatus as known by RmMaster
           * @exception Exception in case of error
           */
          virtual void storeClusterStatus
          (castor::monitoring::ClusterStatus* clusterStatus)
            throw (castor::exception::Exception);

          /**
           * Retrieves the last known cluster status from the stager database
           * and updates the passed ClusterStatus
           * @param clusterStatus the ClusterStatus as known by RmMaster
           * @exception Exception in case of error
           */
          virtual void retrieveClusterStatus
          (castor::monitoring::ClusterStatus* clusterStatus)
            throw (castor::exception::Exception);

          /**
           * Check to see if the diskserver and optionally its mountpoint
           * have files associated to them. If the mountpoint is not provided
           * the check will be executed for all mountpoints associated to the
           * diskserver.
           * @param diskServer the name of the diskServer
           * @param mountPoint the mountPoint of the fileSystem
           * @return true if files exists otherwise false.
           * @exception Exception in case of error
           */
          virtual bool checkIfFilesExist
          (std::string diskServer, std::string fileSystem)
            throw (castor::exception::Exception);

        private:

          /// SQL statement for function storeClusterStatus
          static const std::string s_storeClusterStatusStatementString;

          /// SQL statement object for function storeClusterStatus
          oracle::occi::Statement *m_storeClusterStatusStatement;

          /// SQL statements for function retrieveClusterStatus
          static const std::string s_getDiskServersStatementString;
          static const std::string s_getFileSystemsStatementString;

          /// SQL statement object for function retrieveClusterStatus
          oracle::occi::Statement *m_getDiskServersStatement;
          oracle::occi::Statement *m_getFileSystemsStatement;

          /// SQL statement for function checkIfFilesExist
          static const std::string s_checkIfFilesExistStatementString;

          /// SQL statement object for function checkIfFilesExist
          oracle::occi::Statement *m_checkIfFilesExistStatement;

        }; // end of class OraRmMasterSvc

      } // end of namespace ora

    } // end of namespace rmmaster

  } // end of namespace monitoring

} // end of namespace castor

#endif // ORA_ORARMMASTERSVC_HPP
