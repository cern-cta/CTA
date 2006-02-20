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
 * @(#)$RCSfile: OraCleanSvc.hpp,v $ $Author: gtaur $
 *
 *
 * @author Giulia Taurelli
 *****************************************************************************/

#ifndef ORA_ORACLEANSVC_HPP
#define ORA_ORACLEANSVC_HPP 1

// Include Files
#include "castor/BaseSvc.hpp"
#include "castor/db/ora/OraCommonSvc.hpp"
//#include "castor/db/ora/OraCnvSvc.hpp"
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
         * @param 
         * @return 
         * @exception 
         */

        int removeOutOfDateRequests(int numDays)
        throw (castor::exception::Exception);
         
         /*
         * @param 
         * @return 
         * @exception 
         */

        int  removeArchivedRequests(int hours)
        throw (castor::exception::Exception);

         /*
         * @param 
         * @return 
         * @exception 
         */


      private:

        /// SQL statement for function removeOutOfDateRequests
        static const std::string s_removeOutOfDateRequestsString;

        /// SQL statement object for removeOutOfDateRequests 
        oracle::occi::Statement *m_removeOutOfDateRequestsStatement;

        /// SQL statement for removeArchivedRequests function
        static const std::string s_removeArchivedRequestsString;

        /// SQL statement object for removeArchivedRequests function
        oracle::occi::Statement *m_removeArchivedRequestsStatement;


      }; // end of class OraCleanSvc

    } // end of namespace ora

  } // end of namespace db

} // end of namespace castor

#endif // ORA_ORACLEANSVC_HPP
