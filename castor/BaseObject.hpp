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
 *
 * Basic object support, including pointer to Services and log support
 *
 * @author Sebastien Ponce
 *****************************************************************************/

#pragma once

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
    static Services* services() ;

    /**
     * Static function to reset (deallocate + set the thread specific pointer
     * to NULL) the thread-specific services. Useful in test environments and
     * for clients which to not run the thread pools (but use the marshallers)
     */
    static void resetServices() ;
    
    /**
     * Static access to the underlying thread-shared Services object
     */
    static Services* sharedServices() ;

    /**
     * Non static access to the underlying Services object
     */
    Services* svcs() ;

  protected:

    /**
     * The thread-shared services catalog
     */
    static Services* s_sharedServices;

  private:
    
    /**
     * A little class that will construct the pthread key at init time
     */
    class pthreadKey {
    public:
      pthreadKey()   {
        int rc = pthread_key_create(&m_key, NULL);
        if (rc != 0) {
          castor::exception::Exception e(rc);
          e.getMessage() << "Error caught in call to pthread_key_create (from castor::BaseObject::pthreadKey::pthreadKey";
          throw e;
        }
      }
      operator pthread_key_t () const {
        return m_key;
      }
    private:
      pthread_key_t m_key;
    };

    /**
     * The key to thread-specific storage for Services
     */
    static pthreadKey s_servicesKey;
    
  }; // end of class BaseObject

} // end of namespace castor

