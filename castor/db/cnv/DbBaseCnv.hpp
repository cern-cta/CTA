/******************************************************************************
 *                      DbBaseCnv.hpp
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
 * @author Giuseppe Lo Presti
 *****************************************************************************/

#ifndef DB_DBBASECNV_HPP
#define DB_DBBASECNV_HPP 1

// Include files
#include "castor/IConverter.hpp"
#include "castor/db/IDbStatement.hpp"
#include "castor/db/IDbResultSet.hpp"
#include "castor/db/DbBaseObj.hpp"
#include "castor/exception/Exception.hpp"
#include "castor/exception/SQLError.hpp"

namespace castor {

  // Forward Declarations
  class IObject;
  class IAddress;
  class ICnvSvc;

  namespace db {

  namespace cnv {
      
      /**
       * A base converter for a generic database via CDBC
       */
      class DbBaseCnv : public DbBaseObj, public IConverter {

      public:

        /**
         * Constructor
         */
        DbBaseCnv(castor::ICnvSvc* cs);

        /**
         * Destructor
         */
        virtual ~DbBaseCnv() throw() {};

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

      };

  } // end of namespace cnv

  } // end of namespace db

} // end of namespace castor

#endif // DB_DBBASECNV_HPP
