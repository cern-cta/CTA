/******************************************************************************
 *                      ICnvFactory.hpp
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
 * @(#)$RCSfile: ICnvFactory.hpp,v $ $Revision: 1.2 $ $Release$ $Date: 2004/11/04 08:54:26 $ $Author: sponcec3 $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CASTOR_ICNVFACTORY_HPP
#define CASTOR_ICNVFACTORY_HPP 1

// Include Files
#include "IFactory.hpp"

namespace castor {

  // Forward Declarations
  class IConverter;

  /**
   * The interface for converters facotories
   */
  class ICnvFactory : public IFactory<IConverter> {

  public:

    /**
     * virtual destructor
     */
    virtual ~ICnvFactory(){};

    /**
     * Instantiate an object
     * @param svcs a pointer to the services class to be used
     * for service lookup by the new service
     * @param name the object name, if any
     */
    virtual IConverter* instantiate(const std::string name = "") const = 0;

    /**
     * gets the object type, that is the type of
     * object the underlying converter can convert
     */
    virtual const unsigned int objType() const = 0;

    /**
     * gets the representation type, that is the type of
     * the representation the underlying converter can deal with
     */
    virtual const unsigned int repType() const = 0;

  };

} // end of namespace castor

#endif // CASTOR_ICNVFACTORY_HPP
