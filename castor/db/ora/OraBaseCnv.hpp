/******************************************************************************
 *                      OraBaseCnv.hpp
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

#ifndef DB_ORABASECNV_HPP 
#define DB_ORABASECNV_HPP 1

// Include files
#include "occi.h"
#include "castor/IConverter.hpp"
#include "castor/BaseObject.hpp"
#include "castor/ObjectCatalog.hpp"

namespace castor {

  // Forward Declarations
  class IObject;

  namespace db {

    // Forward Declarations
    class OraCnvSvc;

    /**
     * A base converter for Oracle database
     * Deals with the connection and representation type
     */
    class OraBaseCnv : public BaseObject, public IConverter {
      
    public:
      
      /**
       * Constructor
       */
      OraBaseCnv();

      /**
       * Destructor
       */
      virtual ~OraBaseCnv();

      /**
       * gets the representation type, that is the type of
       * the representation this converter can deal with
       */
      static const unsigned int RepType();

      /**
       * gets the representation type, that is the type of
       * the representation this converter can deal with
       */
      virtual const unsigned int repType() const;


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
        throw (castor::Exception);
      
      /**
       * deletes a statement and frees the associated memory
       * @exception SQLException if the deletion fails
       * @param stmt the statement to be deleted
       */
      void deleteStatement(oracle::occi::Statement* stmt)
        throw (oracle::occi::SQLException);

      /**
       * Get an object from its id.
       * Essentially a wrapper around createObj that
       * don't call it if the object is in the newlyCreated
       * vector
       */
      castor::IObject* getObjFromId (unsigned long id,
                                     ObjectCatalog& newlyCreated)
        throw (castor::Exception);
      
      /**
       * access to the Oracle connection for child classes
       */
      oracle::occi::Connection* connection() const;

      /**
       * access to the Oracle conversion service for child classes
       */
      castor::db::OraCnvSvc* cnvSvc() const;

    protected:

      /***********/
      /* Members */
      /***********/

      /// The corresponding conversion service
      castor::db::OraCnvSvc* m_cnvSvc;

      /// The database connection
      oracle::occi::Connection* m_connection;

    };

  } // end of namespace db

} // end of namespace castor

#endif // DB_ORABASECNV_HPP
