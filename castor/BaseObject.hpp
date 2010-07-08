/******************************************************************************
 *                      BaseObject.hpp
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
 * @(#)$RCSfile: BaseObject.hpp,v $ $Revision: 1.13 $ $Release$ $Date: 2009/07/13 06:22:05 $ $Author: waldron $
 *
 * Basic object support, including pointer to Services and log support
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#ifndef CASTOR_BASEOBJECT_HPP
#define CASTOR_BASEOBJECT_HPP 1

// Include Files
#include "castor/dlf/Dlf.hpp"
#include "castor/exception/Exception.hpp"
#include <pthread.h>

namespace castor {

  // Forward Declarations
  class Services;

  /**
   * Basic object support, including pointer to Services and log support
   */
  class BaseObject {

  public:

    /**
     * constructor
     */
    BaseObject() throw();

    /**
     * destructor
     */
    virtual ~BaseObject() throw() {};

    /**
     * Static access to the underlying thread-safe Services object
     */
    static Services* services() throw(castor::exception::Exception);

    /**
     * Static access to the underlying thread-shared Services object
     */
    static Services* sharedServices() throw(castor::exception::Exception);

    /**
     * Non static access to the underlying Services object
     */
    Services* svcs() throw(castor::exception::Exception);

  protected:

    /**
     * gets the thread local storage for a given key
     * Note that the key must have been created with
     * pthread_key_create or .... Otherwise, the behavior is
     * undefined (see the pthread_getspecific man page)
     * @param key the thread local storage key
     * @param tls a pointer to the thread local storage,
     * itself containing a pointer. This contained pointer
     * will be set to 0 in case of creation of the storage
     */
    static void getTLS(pthread_key_t& key, void**& tls)
      throw(castor::exception::Exception);

  private:

    /**
     * small function that creates a thread-specific storage key
     * for the services 
     */
    static void makeServicesKey() throw (castor::exception::Exception);

  protected:

    /**
     * The thread-shared services catalog
     */
    static Services* s_sharedServices;

  private:

    /**
     * The key to thread-specific storage for Services
     */
    static pthread_key_t s_servicesKey;

    /**
     * The key for creating only once the thread-specific storage key for services
     */
    static pthread_once_t s_servicesOnce;

  }; // end of class BaseObject

} // end of namespace castor

#endif // CASTOR_BASEOBJECT_HPP
