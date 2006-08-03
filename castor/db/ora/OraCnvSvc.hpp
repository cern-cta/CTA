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
 * @(#)$RCSfile$ $Revision$ $Release$ $Date$ $Author$
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CASTOR_ORACNVSVC_HPP
#define CASTOR_ORACNVSVC_HPP 1

// Include Files

#include "castor/BaseCnvSvc.hpp"
#include <set>

#ifdef ORACDBC
#include "castor/db/newora/OraCommonSvc.hpp"
#else
#include "castor/db/ora/OraCommonSvc.hpp"
#endif

#include "castor/BaseSvc.hpp"

#include "occi.h"
#include <vector>

namespace castor {

  // Forward Declarations
  class IAddress;
  class IObject;

  namespace db {

    namespace ora {

      // Forward Declarations
      class OraBaseObj;

      /**
       * Conversion service for Oracle Database
       */
      class OraCnvSvc : public BaseCnvSvc {

      public:

        /** default constructor */
        OraCnvSvc(const std::string name);

        /** default destructor */
        virtual ~OraCnvSvc() throw();

        /** Get the service id */
        virtual const unsigned int id() const;

        /** Get the service id */
        static const unsigned int ID();

        /**
         * gets the representation type, that is the type of
         * the representation this conversion service can deal with
         */
        virtual const unsigned int repType() const;

        /**
         * gets the representation type, that is the type of
         * the representation this conversion service can deal with
         */
        static const unsigned int REPTYPE();

        /**
         * Reset the converter statements.
         */

        void reset() throw ();

        /**
         * Get a connection to the database. The service opens
         * this connection when this function is called for the
         * first time and returns pointers to it for all
         * subsequent calls. The does thus not own the connection
         * @return the newly created connection
         * @exception SQLException thrown by ORACLE
         */
        oracle::occi::Connection* getConnection()
          throw (oracle::occi::SQLException,
                 castor::exception::Exception);

        /**
         * deletes the connection to the database
         */
        void dropConnection() throw();

        /**
         * create C++ object from foreign representation.
         * Reimplemented from BaseCnvSvc. This version is able to
         * make use of OraAdresses and to deduce the object type in
         * the address from the id by querying the database
         * @param address the place where to find the foreign
         * representation
         * @return the C++ object created from its reprensentation
         * or 0 if unsuccessful. Note that the caller is responsible
         * for the deallocation of the newly created object
         * @exception Exception throws an Exception in case of error
         */
        IObject* createObj (IAddress* address)
          throw (castor::exception::Exception);

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
         * Get an object from its id.
         * Essentially a wrapper around createObj that
         * don't call it if the object is in the newlyCreated
         * vector
         * @param id the id of the object to retrieve
         * @exception Exception throws an Exception in case of error
         */
        castor::IObject* getObjFromId (u_signed64 id)
          throw (castor::exception::Exception);
        
      public:

        /**
         * registration of Oracle converters.
         * This allows converters to be aware of a reset
         * of the Oracle connection
         */
        void registerCnv(castor::db::ora::OraBaseObj* cnv)
          throw() { m_registeredCnvs.insert(cnv); }

        /**
         * unregistration of Oracle converters.
         */
        void unregisterCnv(castor::db::ora::OraBaseObj* cnv)
          throw() { m_registeredCnvs.erase(cnv); }

      protected:

        /**
         * retrieves the type of an object given by its id
         */
        const unsigned int getTypeFromId(const u_signed64 id)
          throw (castor::exception::Exception);

      private:

        /// Oracle user name
        std::string m_user;

        /// Oracle user password
        std::string m_passwd;

        /// Oracle database name
        std::string m_dbName;

        /// List of registered converters
        std::set<castor::db::ora::OraBaseObj*> m_registeredCnvs;

        /**
         * The ORACLE environment for this service
         */
        oracle::occi::Environment* m_environment;

        /**
         * The ORACLE connection for this service
         */
        oracle::occi::Connection* m_connection;

        /// SQL statement for type retrieval
        static const std::string s_getTypeStatementString;

        /// SQL statement object for type retrieval
        oracle::occi::Statement *m_getTypeStatement;

      };

    } // end of namespace ora

  } // end of namespace db

} // end of namespace castor

#endif // CASTOR_ORACNVSVC_HPP


