/******************************************************************************
 *                      DbAddress.hpp
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
 * @(#)$RCSfile: DbAddress.hpp,v $ $Revision: 1.4 $ $Release$ $Date: 2004/10/05 13:37:28 $ $Author: sponcec3 $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef DB_DBADDRESS_HPP
#define DB_DBADDRESS_HPP 1

// Include Files
#include "castor/BaseAddress.hpp"
#include "castor/Constants.hpp"
#include "osdep.h"

namespace castor {

  namespace db {

    /**
     * An address containing only an id
     */
    class DbAddress : public BaseAddress {

    public:

      /**
       * constructor
       * @param id the id of the object pointed to
       * @param cnvSvcName the conversion service able to deal with this address
       * @param objType the type of object or OBJ_INVALID if not specified.
       * In this later case, the type will be deduced from the id.
       */
      DbAddress(const u_signed64 id,
                const std::string cnvSvcName,
                const unsigned int objType = OBJ_INVALID);
      
      /*
       * destructor
       */
      virtual ~DbAddress() {}

      /**
       * gets the id of this address
       */
      virtual const u_signed64 id() const { return m_id; }

      /**
       * set the id of this address
       */
      virtual void setId(const u_signed64 id) { m_id = id; }

    private:

      /**
       * the id of this address
       */
      u_signed64 m_id;

    };

  } // end of namespace db

} // end of namespace castor

#endif // DB_DBADDRESS_HPP
