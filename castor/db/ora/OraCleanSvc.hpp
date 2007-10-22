/******************************************************************************
 *                      OraCleanSvc.hpp
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
 * @(#)$RCSfile: OraCleanSvc.hpp,v $ $Author: sponcec3 $
 *
 * This interface provides sereral functions allowing to cleanup the
 * stager database of too old cases.
 *
 * @author castor dev team
 *****************************************************************************/

#ifndef ORA_ORACLEANSVC_HPP
#define ORA_ORACLEANSVC_HPP 1

// Include Files

#ifdef ORACDBC
#include "castor/db/newora/OraCommonSvc.hpp"
#else
#include "castor/db/ora/OraCommonSvc.hpp"
#endif

#include "castor/BaseSvc.hpp"
#include "castor/cleaning/ICleanSvc.hpp"
#include "occi.h"
#include <vector>

namespace castor {

  namespace db {

    namespace ora {

      /**
       * Implementation of the ICleanSvc for Oracle
       */
      class OraCleanSvc : public OraCommonSvc,
                          public virtual castor::cleaning::ICleanSvc {

      public:

        /**
         * default constructor
         */
        OraCleanSvc(const std::string name);

        /**
         * default destructor
         */
        virtual ~OraCleanSvc() throw();

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

        /*
         * Removes all requests older than timeout second
         * @param timeout timeout in seconds
         * @exception castor::exception::Exception
         */
        virtual void removeOutOfDateRequests(int timeout)
          throw (castor::exception::Exception);

        /*
         * Removes successful requests older than timeout hours
         * @param timeout timeout in seconds
         * @exception castor::exception::Exception
         */
        virtual void removeArchivedRequests(int timeout)
          throw (castor::exception::Exception);

        /*
         * Removes all DiskCopies older than timeout second and
         * that are in FAILED status or in INVALID status
         * and have no a fileSystem
         * @param timeout timeout in seconds
         * @exception castor::exception::Exception
         */
        virtual void removeFailedDiskCopies(int timeout)
          throw (castor::exception::Exception);

        /*
         * Deals with DiskCopies that stayed in STAGEOUT longer
         * than timeout second. The ones with 0 filesize go for garbage
         * collection and a putDone is issued for the others
         * @param timeout timeout in seconds
         * @exception castor::exception::Exception
         */
        virtual void removeOutOfDateStageOutDCs(int timeout)
          throw (castor::exception::Exception);

        /*
         * Deals with DiskCopies that stayed WAITTAPERECALL longer
         * than timeout second. The recall is canceled and all
         * subrequests are failed.
         * @param timeout timeout in seconds
         * @exception castor::exception::Exception
         */
        virtual void removeOutOfDateRecallDCs(int timeout)
          throw (castor::exception::Exception);

      private:

        /// SQL statement for function removeOutOfDateRequests
        static const std::string s_removeOutOfDateRequestsString;

        /// SQL statement object for removeOutOfDateRequests
        oracle::occi::Statement *m_removeOutOfDateRequestsStatement;

        /// SQL statement for removeArchivedRequests function
        static const std::string s_removeArchivedRequestsString;

        /// SQL statement object for removeArchivedRequests function
        oracle::occi::Statement *m_removeArchivedRequestsStatement;

        /// SQL statement for function removeFailedDiskCopies
        static const std::string s_removeFailedDiskCopiesString;

        /// SQL statement object for removeFailedDiskCopies
        oracle::occi::Statement *m_removeFailedDiskCopiesStatement;

        /// SQL statement for function removeOutOfDateStageOutDCs
        static const std::string s_removeOutOfDateStageOutDCsString;

        /// SQL statement object for removeOutOfDateStageOutDCs
        oracle::occi::Statement *m_removeOutOfDateStageOutDCsStatement;

        /// SQL statement for function removeOutOfDateRecallDCs
        static const std::string s_removeOutOfDateRecallDCsString;

        /// SQL statement object for removeOutOfDateRecallDCs
        oracle::occi::Statement *m_removeOutOfDateRecallDCsStatement;

      }; // end of class OraCleanSvc

    } // end of namespace ora

  } // end of namespace db

} // end of namespace castor

#endif // ORA_ORACLEANSVC_HPP
