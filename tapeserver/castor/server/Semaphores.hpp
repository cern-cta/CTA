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
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

#include <pthread.h>
#include <semaphore.h>
#include <stdint.h>
#include "castor/server/Mutex.hpp"

namespace castor {
namespace server {   

/**
   * An exception throwing wrapper to posix semaphores.
   */
  class PosixSemaphore {
  public:
    class Timeout{};
    PosixSemaphore(int initial = 0) ;
    ~PosixSemaphore();
    void acquire() ;
    void acquireWithTimeout(uint64_t timeout_us); /**< Throws an exception (Timeout) in case of timeout */
    bool tryAcquire() ;
    void release(int n=1) ;
  private:
    sem_t m_sem;
    /* this mutex protects against destruction unser the feet of the last
     *  the poster (race condition in glibc) */
    Mutex m_mutexPosterProtection;
  };

  /**
   * An exception throwing alternative implementation of semaphores, for systems
   * where posix semaphores are not availble (MacOSX at the time of writing)
   */
  class CondVarSemaphore {
  public:
    CondVarSemaphore(int initial = 0) ;
    ~CondVarSemaphore();
    void acquire() ;
    bool tryAcquire() ;
    void release(int n=1) ;
  private:
    pthread_cond_t m_cond;
    pthread_mutex_t m_mutex;
    int m_value;
  };

#ifndef __APPLE__
  class Semaphore: public PosixSemaphore {
  public:
    Semaphore(int initial=0): PosixSemaphore(initial) {}
  };
#else
  /* Apple does not like posix semaphores :((
     We have to roll our own. */
  class Semaphore: public CondVarSemaphore {
  public:
    Semaphore(int initial=0): CondVarSemaphore(initial) {}
  };
#endif // ndef __APPLE__
  
  }}
