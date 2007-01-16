/******************************************************************************
 *                castor/db/ora/OraRmMasterSvc.hpp
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
 * @(#)$RCSfile: OraRmMasterSvc.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2007/01/16 16:28:53 $ $Author: sponcec3 $
 *
 * Implementation of the IRmMasterSvc for Oracle
 *
 * @author castor dev team
 *****************************************************************************/

#ifndef ORA_ORARMMASTERSVC_HPP
#define ORA_ORARMMASTERSVC_HPP 1

// Include Files
#include "castor/BaseSvc.hpp"
#ifdef ORACDBC
#include "castor/db/newora/OraCommonSvc.hpp"
#else
#include "castor/db/ora/OraCommonSvc.hpp"
#endif
#include "castor/monitoring/rmmaster/IRmMasterSvc.hpp"
#include "occi.h"
#include "castor/monitoring/ClusterStatus.hpp"

namespace castor {

  namespace db {

    namespace ora {

      /**
       * Implementation of the IRmMasterSvc for Oracle
       */
      class OraRmMasterSvc : public OraCommonSvc,
			     public virtual castor::monitoring::rmmaster::IRmMasterSvc {

      public:

        /**
         * default constructor
         */
        OraRmMasterSvc(const std::string name);

        /**
         * default destructor
         */
        virtual ~OraRmMasterSvc() throw();

        /**
         * Get the service id
         */
        virtual inline const unsigned int id() const;

        /**
         * Get the service id
         */
        static const unsigned int ID();

        /**
         * Reset the converter statements.
         */
        void reset() throw ();

      public:

	/**
	 * Synchronizes the stager database and the ClusterStatus, as known
	 * by RmMaster
	 * @param clusterStatus the ClusterStatus as known by RmMaster
	 * @exception Exception in case of error
	 */
        virtual void syncClusterStatus
	(castor::monitoring::ClusterStatus* clusterStatus)
          throw (castor::exception::Exception);

      private:

        /// SQL statement for function syncClusterStatus
        static const std::string s_syncClusterStatusStatementString;

        /// SQL statement object for function syncClusterStatus
        oracle::occi::Statement *m_syncClusterStatusStatement;

      }; // end of class OraRmMasterSvc

    } // end of namespace ora

  } // end of namespace db

} // end of namespace castor

#endif // ORA_ORARMMASTERSVC_HPP
