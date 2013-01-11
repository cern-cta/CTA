/******************************************************************************
 *                castor/db/ora/OraCommonSvc.hpp
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
 * @(#)$RCSfile: OraCommonSvc.hpp,v $ $Revision: 1.14 $ $Release$ $Date: 2009/03/26 10:59:47 $ $Author: itglp $
 *
 * Implementation of the ICommonSvc for Oracle/CDBC
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef ORA_ORACOMMONSVC_HPP
#define ORA_ORACOMMONSVC_HPP 1

// Include Files
#include "castor/BaseSvc.hpp"
#include "castor/ICnvSvc.hpp"
#include "castor/db/DbBaseObj.hpp"
#include "castor/db/ora/OraCnvSvc.hpp"
#include "castor/stager/ICommonSvc.hpp"
#include "occi.h"
#include <vector>

namespace castor {

  namespace db {

    namespace ora {

      /**
       * Implementation of the ICommonSvc for Oracle
       */
      class OraCommonSvc : public BaseSvc,
                           public DbBaseObj,
                           public virtual castor::stager::ICommonSvc {

      public:

        /**
         * default constructor
         */
        OraCommonSvc(const std::string name, castor::ICnvSvc* conversionService = 0);

        /**
         * default destructor
         */
        virtual ~OraCommonSvc() throw();

        /**
         * Get the service id
         */
        virtual unsigned int id() const;

        /**
         * Get the service id
         */
        static unsigned int ID();

        /**
         * Reset the converter statements.
         */
        void reset() throw ();

      public:

        /**
         * Selects the next request a stager service should deal with.
         * Selects a Request in START status and deletes it from the
         * NewRequests helper table to avoid double processing.
         * @param service the stager service that will process the Request
         * @return the Request to process
         * @exception Exception in case of error
         */
        virtual castor::stager::Request* requestToDo(std::string service)
          throw (castor::exception::Exception);

      protected:

        /**
         * helper method to commit
         */
        virtual void commit();

        /**
         * helper method to rollback
         */
        virtual void rollback();

        /**
         * Helper method to handle exceptions - see OraCnvSvc
         * @param e an Oracle exception
         */
        void handleException(oracle::occi::SQLException& e) throw ();

        /**
         * helper method to create Oracle statement
         */
        virtual oracle::occi::Statement* createStatement(const std::string& stmtString)
          throw (castor::exception::Exception);

        /**
         * helper method to delete Oracle statement
         */
        virtual void deleteStatement(oracle::occi::Statement* stmt)
          throw (castor::exception::Exception);

      private:

        /// SQL statement for function requestToDo
        static const std::string s_requestToDoStatementString;

        /// SQL statement object for function requestToDo
        oracle::occi::Statement *m_requestToDoStatement;

      }; // end of class OraCommonSvc

    } // end of namespace ora

  } // end of namespace db

} // end of namespace castor

#endif // ORA_ORACOMMONSVC_HPP
