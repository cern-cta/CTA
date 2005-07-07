/******************************************************************************
 *                castor/db/ora/OraFSSvc.hpp
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
 * @(#)$RCSfile: OraFSSvc.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2005/07/07 15:08:21 $ $Author: itglp $
 *
 * Implementation of the IFSSvc for Oracle
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef ORA_ORAFSSVC_HPP
#define ORA_ORAFSSVC_HPP 1

// Include Files
#include "castor/BaseSvc.hpp"
#include "castor/db/ora/OraCommonSvc.hpp"
#include "castor/db/ora/OraCnvSvc.hpp"
#include "castor/stager/IFSSvc.hpp"
#include "occi.h"
#include <vector>

namespace castor {

  namespace db {

    namespace ora {

      /**
       * Implementation of the IFSSvc for Oracle
       */
      class OraFSSvc : public OraCommonSvc,
                       public virtual castor::stager::IFSSvc {

      public:

        /**
         * default constructor
         */
        OraFSSvc(const std::string name);

        /**
         * default destructor
         */
        virtual ~OraFSSvc() throw();

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
         * Retrieves a DiskPool from the database based on name.
         * Caller is in charge of the deletion of the allocated
         * memory.
         * @param name the name of the disk pool
         * @return the DiskPool object or 0 if none found
         * @exception Exception in case of error
         */
        virtual castor::stager::DiskPool* selectDiskPool
        (const std::string name)
          throw (castor::exception::Exception);

        /**
         * Retrieves a TapePool from the database based on name.
         * Caller is in charge of the deletion of the allocated
         * memory.
         * @param name the name of the tape pool
         * @return the TapePool object or 0 if none found
         * @exception Exception in case of error
         */
        virtual castor::stager::TapePool* selectTapePool
        (const std::string name)
          throw (castor::exception::Exception);

        /**
         * Retrieves a DiskServer from the database based on name.
         * Caller is in charge of the deletion of the allocated
         * memory.
         * @param name the name of the disk server
         * @return the DiskServer object or 0 if none found
         * @exception Exception in case of error
         */
        virtual castor::stager::DiskServer* selectDiskServer
        (const std::string name)
          throw (castor::exception::Exception);

      private:

        /// SQL statement for function selectDiskPool
        static const std::string s_selectDiskPoolStatementString;

        /// SQL statement object for function selectDiskPool
        oracle::occi::Statement *m_selectDiskPoolStatement;

        /// SQL statement for function selectTapePool
        static const std::string s_selectTapePoolStatementString;

        /// SQL statement object for function selectTapePool
        oracle::occi::Statement *m_selectTapePoolStatement;

        /// SQL statement for function selectDiskServer
        static const std::string s_selectDiskServerStatementString;

        /// SQL statement object for function selectDiskServer
        oracle::occi::Statement *m_selectDiskServerStatement;

      }; // end of class OraFSSvc

    } // end of namespace ora

  } // end of namespace db

} // end of namespace castor

#endif // ORA_ORAFSSVC_HPP
