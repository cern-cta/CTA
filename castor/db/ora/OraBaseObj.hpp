/******************************************************************************
 *                      OraBaseObj.hpp
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
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef DB_ORABASEOBJ_HPP
#define DB_ORABASEOBJ_HPP 1

// Include files
#include "occi.h"
#include "castor/BaseObject.hpp"
#include "castor/exception/Exception.hpp"

namespace castor {

  // Forward Declarations
  class IObject;
  class ICnvSvc;

  namespace db {

    namespace ora {

      // Forward Declarations
      class OraCnvSvc;

      /**
       * A base converter for Oracle database
       */
      class OraBaseObj : public virtual BaseObject {

      public:

        /**
         * Constructor
         */
        OraBaseObj(castor::ICnvSvc* cnvSvc);

        /**
         * Destructor
         */
        virtual ~OraBaseObj() throw();

        /**
         * Resets the converter. In particular any prepared
         * statements are destroyed
         */
        virtual void reset() throw() = 0;

      protected:

        /**
         * creates a statement from a string. Note that the
         * deallocation of the statement is the responsability
         * of the caller. delete Statement should be used
         * @param stmtString the statement as a string
         * @exception Exception if the creation fails
         * @return the newly created statement
         * @see deleteStatement()
         */
        oracle::occi::Statement* createStatement(const std::string &stmtString)
          throw (castor::exception::Exception);

        /**
         * deletes a statement and frees the associated memory
         * @exception SQLException if the deletion fails
         * @param stmt the statement to be deleted
         */
        void deleteStatement(oracle::occi::Statement* stmt)
          throw (oracle::occi::SQLException);

        /**
         * access to the Oracle conversion service for child classes
         */
        castor::db::ora::OraCnvSvc* cnvSvc() const;

	void handleException(oracle::occi::SQLException e);

      protected:

        /***********/
        /* Members */
        /***********/

        /// The corresponding conversion service
        castor::db::ora::OraCnvSvc* m_cnvSvc;

        /// Whether the conversion service should be released
        bool m_ownsCnvSvc;
        
      };

    } // end of namespace ora

  } // end of namespace db

} // end of namespace castor

#endif // DB_ORABASEOBJ_HPP







