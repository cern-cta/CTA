/******************************************************************************
 *                      IFactory.hpp
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
 * @(#)$RCSfile: IFactory.hpp,v $ $Revision: 1.3 $ $Release$ $Date: 2004/11/04 08:54:26 $ $Author: sponcec3 $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CASTOR_IFACTORY_HPP
#define CASTOR_IFACTORY_HPP 1

// Include Files
#include <string>

namespace castor {

  // Forward Declarations
  class IService;

  /**
   * A factory for a service
   */
  template<class TYPE> class IFactory {

  public:

    /**
     * virtual destructor
     */
    virtual ~IFactory(){};

    /**
     * Instantiate an object
     * @param svcs a pointer to the services class to be used
     * for service lookup by the new service
     * @param name the object name, if any
     */
    virtual TYPE* instantiate(const std::string name) const = 0;

  };

} // end of namespace castor

#endif // CASTOR_IFACTORY_HPP
