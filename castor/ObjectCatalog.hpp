/******************************************************************************
 *                      ObjectCatalog.hpp
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
 * @(#)$RCSfile: ObjectCatalog.hpp,v $ $Revision: 1.3 $ $Release$ $Date: 2004/10/29 15:25:31 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CASTOR_OBJECTCATALOG_HPP 
#define CASTOR_OBJECTCATALOG_HPP 1

// Include Files
#include <map>
#include "osdep.h"

namespace castor {

  // Forward Declarations
  class IObject;

  /**
   * A class defining a catalog of objects indexed by id.
   * The ids are automatically generated on insertion, starting
   * from 1 and going up.
   */
  class ObjectCatalog : public std::map<unsigned int, castor::IObject*> {
    
  public:

    /**
     * Constructor
     */
    ObjectCatalog() : m_nextId(1) {};

    /**
     * Insertion of an object with automatic generation of the
     * associated key, ie of the id
     */
    void insert(castor::IObject* obj);
    
  private:

    // The next id to use
    unsigned int m_nextId;    

  };

} // end of namespace castor

#endif // CASTOR_OBJECTCATALOG_HPP
