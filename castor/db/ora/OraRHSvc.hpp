/******************************************************************************
 *                      OraRHSvc.hpp
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
 * @(#)$RCSfile: OraRHSvc.hpp,v $ $Revision: 1.3 $ $Release$ $Date: 2007/09/11 08:50:31 $ $Author: waldron $
 *
 * Implementation of the IRHSvc for Oracle
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef ORA_ORARHSVC_HPP 
#define ORA_ORARHSVC_HPP 1

// Include Files
#ifdef ORACDBC
#include "castor/db/newora/OraCommonSvc.hpp"
#else
#include "castor/db/ora/OraCommonSvc.hpp"
#endif
#include "castor/rh/IRHSvc.hpp"
#include "occi.h"

namespace castor {

  namespace db {

    namespace ora {

      /**
       * Implementation of the IRHSvc for Oracle
       */
      class OraRHSvc : public OraCommonSvc,
                       public virtual castor::rh::IRHSvc {

      public:

        /**
         * default constructor
         */
        OraRHSvc(const std::string name);

        /**
         * default destructor
         */
        virtual ~OraRHSvc() throw();

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
         * checks permission for a given user to make a give request
         * in a given service class. Just returns nothing in case
         * permission is granted and throw a permission denied
         * exception otherwise
         * @param svcClassName the name of the service class
         * @param euid the uid of the user
         * @param egid the gid of the user
         * @param type the type of request
         * @exception throws Exception in case of permission denial
         */
        virtual void checkPermission
        (const std::string svcClassName, unsigned long euid,
         unsigned long egid, int type)
          throw (castor::exception::Exception);

      private:

        /// SQL statement for function checkPermission
        static const std::string s_checkPermissionStatementString;

        /// SQL statement object for function checkPermission
        oracle::occi::Statement *m_checkPermissionStatement;

      }; // end of class OraRHSvc

    } // end of namespace ora

  } // end of namespace db

} // end of namespace castor

#endif // ORA_ORARHSVC_HPP
