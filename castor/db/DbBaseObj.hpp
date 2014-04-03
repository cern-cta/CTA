/******************************************************************************
 *                      DbBaseObj.hpp
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
 * Base class for all database oriented objects
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

#pragma once

// Include files
#include "castor/BaseObject.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/db/IDbStatement.hpp"
#include "castor/db/DbCnvSvc.hpp"

namespace castor {

  // Forward Declarations
  class IObject;
  class ICnvSvc;

  namespace db {

      /**
       * A base converter for Oracle database
       */
      class DbBaseObj : public virtual BaseObject {

      public:

        /**
         * Constructor. Binds this object to the provided conversion service.
         * If nothing is passed, tries to create a "DbCnvSvc" service. 
         */
        DbBaseObj(castor::ICnvSvc* cnvSvc);

        /**
         * Destructor
         */
        virtual ~DbBaseObj() throw() {};

      protected:

        /**
         * inits the conversion service to be used
         */
        void initCnvSvc() throw (castor::exception::Exception);

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
        castor::db::DbCnvSvc* cnvSvc() throw (castor::exception::Exception);

        /**
         * register a Service to the underlying conversion service.
         * The conversion service is created if needed.
         * @param svc the service to register
         */
        void registerToCnvSvc(castor::IService* svc) throw () {
          cnvSvc()->registerDepSvc(svc);
        }

        /**
         * unregister a Service from the underlying conversion service.
         * In case no conversion service exists, this is a noop
         * @param svc the service to unregister
         */
        void unregisterFromCnvSvc(castor::IService* svc) throw () {
          if (m_cnvSvc) m_cnvSvc->unregisterDepSvc(svc);
        }

        /**
         * resets the service
         */
        virtual void reset() throw();

        /**
         * helper method to commit
         */
        virtual void commit();          

        /**
         * helper method to rollback
         */
        virtual void rollback();

      private:
        
        /// The corresponding conversion service
        castor::db::DbCnvSvc* m_cnvSvc;
        
        /// The name of the conversion service to be used
        std::string m_cnvSvcName;
        
      };

  } // end of namespace db

} // end of namespace castor

