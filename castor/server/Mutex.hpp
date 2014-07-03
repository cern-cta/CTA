/******************************************************************************
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
 * C++ interface to mutex handling with the Cthread API
 *
 * @author Giuseppe Lo Presti
 *****************************************************************************/

#pragma once

#include <iostream>
#include <string>
#include "castor/exception/Exception.hpp"

namespace castor {

 namespace server {

  /**
   * C++ interface to mutex handling with the Cthread API
   */
  // template<class TYPE> class Mutex  could be next step
  class Mutex {

  public:
    static const unsigned int TIMEOUT = 10;
    
    /**
     * Mutex initialization.
     * @param var the shared variable.
     * @param timeout interval to be used in Cthread calls.
     */
    Mutex(int value, unsigned int timeout = TIMEOUT)
      ;
    
    /**
     * Destructor
     */
    ~Mutex();
    
    /**
     * Gets the internal variable value.
     */
    int getValue() {
      return m_var;
    }
    
    /**
     * Sets the internal variable value.
     * This method is thread-safe, meaning that
     * it will wait for mutex on the var.
     * @throws exception if the mutex can't be
     * acquired or the mutex initialization failed.
     */
    void setValue(int newValue)
      ;
    
    /**
     * Sets the internal variable value.
     * This method tries to be thread-safe, but won't
     * throw an exception when the mutex can't be
     * acquired: in that case it does thread-
     * unsafely change the value.
     */
    void setValueNoEx(int newValue);
    
    /**
     * Sets the internal variable value.
     * This method is NOT thread-safe, and
     * it is provided for convenience only.
     */
    void setValueNoMutex(int newValue) {
      m_var = newValue;
    }
    
    /**
     * waits for a signal on this mutex.
     */
    void wait();
    
    /**
     * Tries to get a lock on this mutex.
     */
    void lock() ;
    
    /**
     * Tries to release the lock on this mutex.
     */
    void release() ;
    
    /**
     * Tries to signal the mutex by calling Cthread_cond_signal().
     */
    void signal() ;

  private:
    
    int m_var;
    unsigned int m_timeout;
    void* m_mutexCthread;
    
    int createLock();

  };
   
 } // end of namespace server
  
} // end of namespace castor


