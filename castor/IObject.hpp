/******************************************************************************
 *                      castor/IObject.hpp
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
 * @author Sebastien Ponce, sebastien.ponce@cern.ch
 *****************************************************************************/

#ifndef CASTOR_IOBJECT_HPP
#define CASTOR_IOBJECT_HPP

// Include Files
#include <iostream>
#include <string>

namespace castor {

  // Forward declarations
  class ObjectSet;

  /**
   * class IObject
   * Base class for all objects Implements the id methods
   */
  class IObject {

  public:

    /**
     * Empty Destructor
     */
    virtual ~IObject() throw() {};

    /**
     * Sets the id of the object
     * @param id The new id
     */
    virtual void setId(unsigned long id) = 0;

    /**
     * gets the id of the object
     */
    virtual unsigned long id() const = 0;

    /**
     * Gets the type of the object
     */
    virtual int type() const = 0;

    /**
     * Outputs this object in a human readable format
     * @param stream The stream where to print this object
     * @param indent The indentation to use
     * @param alreadyPrinted The set of objects already printed.
     * This is to avoid looping when printing circular dependencies
     */
    virtual void print(std::ostream& stream,
                       std::string indent,
                       castor::ObjectSet& alreadyPrinted) const = 0;

    /**
     * Outputs this object in a human readable format
     */
    virtual void print() const = 0;

  }; // end of class IObject

}; // end of namespace castor

#endif // CASTOR_IOBJECT_HPP
