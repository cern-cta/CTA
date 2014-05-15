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
*
* The conversion service to Oracle
*
* @author Sebastien Ponce
*****************************************************************************/

#pragma once

// Include Files
#include "occi.h"
#include "castor/db/DbCnvSvc.hpp"
#include "castor/exception/SQLError.hpp"
#include <map>

namespace castor {
  
  // Forward Declarations
  class IAddress;
  class IObject;
  
  namespace db {
    
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
        virtual unsigned int id() const;
        
        /** Get the service id */
        static unsigned int ID();
        
        /**
         * Get the real representation type from the
         * concrete class, that is the database type
         * to which this conversion service is connected.
         */
        virtual unsigned int getPhysRepType() const;
        
        /**
         * Forces the commit of the last changes.
         * @exception Exception throws an Exception in case of error
         */
        virtual void commit()
          ;
        
        /**
         * Forces the rollback of the last changes
         * @exception Exception throws an Exception in case of error
         */
        virtual void rollback()
          ;
        
        /**
         * Creates an Oracle prepared statement wrapped with the
         * db-independent CDBC API
         * @param stmt the string statement to be prepared 
         */
        virtual castor::db::IDbStatement* createStatement(const std::string& stmt)
          ;
        
        /**
         * Creates an Oracle prepared statement exposed with the Oracle API
         * @param stmt the string statement to be prepared 
         * @exception Exception throws an Exception in case of error
         * creating the statement or establishing the underlying DB connection.
         */
        oracle::occi::Statement*
        createOraStatement(const std::string& stmt)
          ;

        /**
         * Creates or reuses an Oracle prepared statement exposed with the Oracle API.
         * The statement is always autocommited
         * @param stmtStr the string statement
         * @param wasCreated set to true if the oracle statement was just created
         * @exception Exception throws an Exception in case of error
         * creating the statement or establishing the underlying DB connection in case
         * the statement did not exist yet, or reusing the stored statement.
         * Note that statements are thread specific.
         */
        oracle::occi::Statement*
        createOrReuseOraStatement(const std::string& stmtStr, bool *wasCreated)
          ;
        
        /**
         * Terminates a prepared statement created with the Oracle API
         * @param stmt the statement to be deleted
         * @exception Exception throws an Exception in case the
         * Oracle API failed to terminate the statement or if
         * the DB connection was not initialized.
         */
        void terminateStatement(oracle::occi::Statement* oraStmt)
          ;
        
        /**
         * Resets the service. In particular, drops the connection to the database
         */
        void reset() throw();
        
        /**
         * Handles Oracle exceptions and make sure everything is reset
         * so that next time a new connection is established
         * @param e an Oracle exception
         */
        virtual void handleException(std::exception& e) throw();
        
      private:
        
        /**
         * Gets a connection to the database. The service opens
         * this connection when this function is called for the
         * first time and returns pointers to it for all
         * subsequent calls. The does thus not own the connection
         * @return the newly created connection
         * @exception a Castor exception including the Oracle error
         * when the connection couldn't be established
         */
        oracle::occi::Connection* getConnection()
          ;
        
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
         * Set of reusable, thread specific, prepared statements
         */
        std::map<std::string, oracle::occi::Statement*> m_reusableStatements;
        
        /**
         * Friend declaration for OraStatement to allow
         * its bulk methods to access the Oracle DB connection
         */
        friend class OraStatement;
        
      };
      
    } // end of namespace ora
    
  } // end of namespace db
  
} // end of namespace castor

