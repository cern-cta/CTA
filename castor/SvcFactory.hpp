/******************************************************************************
 *                      SvcFactory.hpp
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
 * @(#)$RCSfile: SvcFactory.hpp,v $ $Revision: 1.2 $ $Release$ $Date: 2004/05/26 15:43:39 $ $Author: sponcec3 $
 *
 *
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CASTOR_SVCFACTORY_HPP
#define CASTOR_SVCFACTORY_HPP 1

// Include Files
#include "ISvcFactory.hpp"
#include "Factories.hpp"

namespace castor {

  // Forward Declarations
  class IService;

  /**
   * A simple factory for services
   */
  template <class Service> class SvcFactory : public ISvcFactory {

  public:
    /** Default constructor */
    SvcFactory();

    /** Default destructor */
    virtual ~SvcFactory() {};

    /**
     * Instantiate the service 
     * @param name the service name
     */
    virtual IService* instantiate(const std::string name) const;

    /** Get ID of the factory, i.e. of the underlying Service */
    const unsigned int id() const;

  };

} // end of namespace castor

template <class Service>
inline castor::SvcFactory<Service>::SvcFactory() {
  castor::Factories::instance()->addFactory(this);
}

template <class Service> inline castor::IService*
castor::SvcFactory<Service>::instantiate(const std::string name) const {
  return new Service(name);
}

template <class Service>
inline const unsigned int castor::SvcFactory<Service>::id() const {
  return Service::ID();
}

#endif // CASTOR_SVCFACTORY_HPP
