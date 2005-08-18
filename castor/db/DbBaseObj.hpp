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
 * @(#)$RCSfile: DbBaseObj.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2005/08/18 10:01:35 $ $Author: itglp $
 *
 *
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

#ifndef DB_DBBASEOBJ_HPP
#define DB_DBBASEOBJ_HPP 1

// Include files
#include "castor/BaseObject.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/db/IDbStatement.hpp"

namespace castor {

  // Forward Declarations
  class IObject;
  class ICnvSvc;

  namespace db {
	  
	 class DbCnvSvc;


	 /**
       * A base converter for Oracle database
       */
      class DbBaseObj : public BaseObject {

      public:

        /**
         * Constructor
         */
        DbBaseObj(castor::ICnvSvc* cnvSvc);

        /**
         * Destructor
         */
        virtual ~DbBaseObj() throw();

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
         * @return the newly created statement, wrapped by CDBC API
         */
        castor::db::IDbStatement* createStatement(const std::string &stmtString)
          throw (castor::exception::Exception);

        /**
         * access to the underlying database conversion service for child classes
         */
        castor::db::DbCnvSvc* cnvSvc() const;

      protected:

        /// The corresponding conversion service
        castor::db::DbCnvSvc* m_cnvSvc;

        /// Whether the conversion service should be released
        bool m_ownsCnvSvc;
        
      };

  } // end of namespace db

} // end of namespace castor

#endif // DB_DBBASEOBJ_HPP
