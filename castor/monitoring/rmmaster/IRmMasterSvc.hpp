/******************************************************************************
 *                castor/monitoring/rmmaster/IRmMasterSvc.hpp
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
 * @(#)$RCSfile: IRmMasterSvc.hpp,v $ $Revision: 1.3 $ $Release$ $Date: 2007/04/13 13:45:59 $ $Author: itglp $
 *
 * This class provides stager methods related to File System handling
 *
 * @author castor dev team
 *****************************************************************************/

#ifndef RMMASTER_IRMMASTERSVC_HPP
#define RMMASTER_IRMMASTERSVC_HPP 1

// Include Files
#include "castor/stager/ICommonSvc.hpp"
#include "castor/exception/Exception.hpp"

namespace castor {

  namespace monitoring {

    // Forward declaration
    class ClusterStatus;

    namespace rmmaster {

      /**
       * This class provides methods related to RmMaster DataBase backend
       */
      class IRmMasterSvc : public virtual castor::stager::ICommonSvc {

      public:
        /**
         * Stores the current ClusterStatus into the stager database
         * @param clusterStatus the ClusterStatus as known by RmMaster
         * @exception Exception in case of error
         */
        virtual void storeClusterStatus
        (castor::monitoring::ClusterStatus* clusterStatus)
          throw (castor::exception::Exception) = 0;

        /**
         * Retrieves the last known cluster status from the stager database
         * and updates the passed ClusterStatus
         * @param clusterStatus the ClusterStatus as known by RmMaster
         * @exception Exception in case of error
         */
        virtual void retrieveClusterStatus
        (castor::monitoring::ClusterStatus* clusterStatus)
          throw (castor::exception::Exception) = 0;

      }; // end of class IRmMasterSvc

    } // end of namespace rmmaster

  } // end of namespace monitoring

} // end of namespace castor

#endif // RMMASTER_IRMMASTERSVC_HPP
