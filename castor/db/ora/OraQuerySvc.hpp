/******************************************************************************
 *                      OraQuerySvc.hpp
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
 * @(#)$RCSfile: OraQuerySvc.hpp,v $ $Revision: 1.1 $ $Release$ $Date: 2004/12/17 15:25:11 $ $Author: sponcec3 $
 *
 * Implementation of the IQuerySvc for Oracle
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef ORA_ORAQUERYSVC_HPP 
#define ORA_ORAQUERYSVC_HPP 1

// Include Files
#include "castor/BaseSvc.hpp"
#include "castor/db/ora/OraBaseObj.hpp"
#include "castor/db/ora/OraCnvSvc.hpp"
#include "castor/query/IQuerySvc.hpp"
#include "occi.h"
#include <list>

namespace castor {

  namespace db {

    namespace ora {

      /**
       * Implementation of the IQuerySvc for Oracle
       */
      class OraQuerySvc : public BaseSvc,
                          public OraBaseObj,
                          public virtual castor::query::IQuerySvc {

      public:

        /**
         * default constructor
         */
        OraQuerySvc(const std::string name);

        /**
         * default destructor
         */
        virtual ~OraQuerySvc() throw();

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
         * Gets all DiskCopies for a given file.
         * The caller is responsible for the deallocation of
         * the returned objects
         * @param fileId the fileId identifying the file
         * @param nsHost the name server host for this file
         * @return the list of DiskCopies available
         * @exception in case of error
         */
        virtual std::list<castor::stager::DiskCopyForRecall*>
        diskCopies4File (std::string fileId,
                         std::string nsHost)
          throw (castor::exception::Exception);

      private:

        /// SQL statement for function tapesToDo
        static const std::string s_diskCopies4FileStatementString;

        /// SQL statement object for function tapesToDo
        oracle::occi::Statement *m_diskCopies4FileStatement;

      }; // end of class OraQuerySvc

    } // end of namespace ora

  } // end of namespace db

} // end of namespace castor

#endif // ORA_ORAQUERYSVC_HPP
