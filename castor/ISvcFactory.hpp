/******************************************************************************
 *                      ISvcFactory.hpp
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
 * @(#)$RCSfile: ISvcFactory.hpp,v $ $Revision: 1.2 $ $Release$ $Date: 2004/11/04 08:54:26 $ $Author: sponcec3 $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CASTOR_ISVCFACTORY_HPP
#define CASTOR_ISVCFACTORY_HPP 1

// Include Files
#include "IFactory.hpp"

namespace castor {

  // Forward Declarations
  class IService;

  /**
   * An abtract interface for services factories
   */
  class ISvcFactory : public IFactory<IService> {

  public:

    /**
     * virtual destructor
     */
    virtual ~ISvcFactory(){};

    /** Get ID of the factory, i.e. of the underlying Service */
    virtual const unsigned int id() const = 0;

  };

} // end of namespace castor

#endif // CASTOR_ISVCFACTORY_HPP
