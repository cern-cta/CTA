/******************************************************************************
 *                      IService.hpp
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
 * @(#)$RCSfile: IService.hpp,v $ $Revision: 1.3 $ $Release$ $Date: 2004/11/04 08:54:26 $ $Author: sponcec3 $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CASTOR_ISERVICE_HPP
#define CASTOR_ISERVICE_HPP 1

// Include files
#include <string>

namespace castor {

  /**
   * base of all services
   */
  class IService {

  public:

    /**
     * virtual destructor
     */
    virtual ~IService(){};

    /**
     * Get the service id
     */
    virtual const unsigned int id() const = 0;

    /**
     * Get the service name
     */
    virtual const std::string name() const = 0;

    /**
     * adds a reference to this service
     */
    virtual void addRef() = 0;
    
    /**
     * removes a reference to the service and releases it
     * if the count goes to 0
     */
    virtual void release() = 0;

  };

} // end of namespace castor

#endif // CASTOR_ISERVICE_HPP
