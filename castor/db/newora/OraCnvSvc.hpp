/******************************************************************************
 *                      OraCnvSvc.hpp
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
 * @(#)$RCSfile: OraCnvSvc.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2005/08/22 16:32:33 $ $Author: itglp $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CASTOR_ORACNVSVC_HPP
#define CASTOR_ORACNVSVC_HPP 1

// Include Files
#include "occi.h"
#include "castor/db/DbCnvSvc.hpp"

namespace castor {

  // Forward Declarations
  class IAddress;
  class IObject;

  namespace db {

    // Forward Declarations
    class DbBaseObj;

    namespace ora {

      // Forward Declarations
      class OraStatement;
      
      /**
       * Conversion service for Oracle Database
       */
      class OraCnvSvc : public DbCnvSvc {

      public:

        /** default constructor */
        explicit OraCnvSvc(const std::string name);

        /** default destructor */
        virtual ~OraCnvSvc() throw();

        /** Get the service id */
        virtual const unsigned int id() const;

        /** Get the service id */
        static const unsigned int ID();

        /**
         * Get the real representation type from the
         * concrete class, that is the database type
         * to which this conversion service is connected.
         */
        virtual const unsigned int getPhysRepType() const;

        /**
         * Forces the commit of the last changes.
         * @param address what to commit
         * @exception Exception throws an Exception in case of error
         */
        virtual void commit()
		  throw (castor::exception::Exception);

        /**
         * Forces the rollback of the last changes
         * @exception Exception throws an Exception in case of error
         */
        virtual void rollback()
		  throw (castor::exception::Exception);

        /**
         * Creates an Oracle prepared statement wrapped with the
         * db-independent CDBC API
         * @param stmt the string statement to be prepared 
         */
        virtual castor::db::IDbStatement* createStatement(const std::string& stmt)
		  throw (castor::exception::Exception);

        /**
         * Creates an Oracle prepared statement exposed with the Oracle API
         * @param stmt the string statement to be prepared 
         */
        oracle::occi::Statement*
        castor::db::ora::OraCnvSvc::createOraStatement(const std::string& stmt)
          throw (castor::exception::Exception);

        /**
         * Deletes the connection to the database
         */
        virtual void dropConnection() throw();

      private:

        /**
         * Get a connection to the database. The service opens
         * this connection when this function is called for the
         * first time and returns pointers to it for all
         * subsequent calls. The does thus not own the connection
         * @return the newly created connection
         * @exception SQLException thrown by Oracle
         */
        oracle::occi::Connection* getConnection()
          throw (oracle::occi::SQLException, castor::exception::Exception);

        /**
         * Closes a prepared statement wrapped with the
         * db-independent CDBC API.
         * This is called by ~OraStatement.
         * @param stmt the statement to be deleted 
         */
        virtual void closeStatement(castor::db::IDbStatement* stmt)
          throw (castor::exception::Exception);

        /// Oracle user name
        std::string m_user;

        /// Oracle user password
        std::string m_passwd;

        /// Oracle database name
        std::string m_dbName;

        /**
         * The Oracle environment for this service
         */
        oracle::occi::Environment* m_environment;

        /**
         * The Oracle connection for this service
         */
        oracle::occi::Connection* m_connection;

        
      /**
       * Friend declaration for OraStatement to allow call
       * the private method closeStatement()
       */
      friend class OraStatement;

      };

    } // end of namespace ora

  } // end of namespace db

} // end of namespace castor

#endif // CASTOR_ORACNVSVC_HPP
