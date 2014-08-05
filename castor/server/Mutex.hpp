/******************************************************************************
 *                      Payload.hpp
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
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/
#pragma once

#include <pthread.h>
#include <semaphore.h>
#include "castor/exception/Errnum.hpp"
#include "castor/tape/tapeserver/exception/Exception.hpp"

namespace castor {
namespace server {
  /**
   * A simple exception throwing wrapper for pthread mutexes.
   * Inspired from the interface of Qt.
   */
  class Mutex {
  public:
    Mutex() ;
    ~Mutex();
    void lock() ;
    void unlock();
  private:
    pthread_mutex_t m_mutex;
  };
  
  /**
   * A simple scoped locker for mutexes. Highly recommended as
   * the mutex will be released in all cases (exception, mid-code return, etc...)
   * To use, simply instanciate and forget.
   * @param m pointer to a Mutex instance
   */
  class MutexLocker {
  public:
    MutexLocker(Mutex * m) :m_mutex(m) {m->lock();}
    ~MutexLocker() { try { m_mutex->unlock(); } catch (...) {} }
  private:
    Mutex * m_mutex;
  };
  
}}