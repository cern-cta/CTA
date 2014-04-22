/******************************************************************************
 *                      Threading.hpp
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
#include "../exception/Exception.hpp"

namespace castor {
namespace tape {
namespace threading {
  /**
   * A simple exception throwing wrapper for pthread mutexes.
   * Inspired from the interface of Qt.
   */
  class Mutex {
  public:
    Mutex() throw (castor::exception::Exception);
    ~Mutex();
    void lock() throw (castor::exception::Exception);
    void unlock() throw (castor::exception::Exception) {
      castor::exception::Errnum::throwOnReturnedErrno(
        pthread_mutex_unlock(&m_mutex),
        "Error from pthread_mutex_unlock in castor::tape::threading::Mutex::unlock()");
    }
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
    MutexLocker(Mutex * m) throw (castor::exception::Exception):m_mutex(m) {m->lock();}
    ~MutexLocker() { try { m_mutex->unlock(); } catch (...) {} }
  private:
    Mutex * m_mutex;
  };
  
  /**
   * An exception throwing wrapper to posix semaphores.
   */
  class PosixSemaphore {
  public:
    PosixSemaphore(int initial = 0) throw (castor::exception::Exception);
    ~PosixSemaphore();
    void acquire() throw (castor::exception::Exception);
    bool tryAcquire() throw (castor::exception::Exception);
    void release(int n=1) throw (castor::exception::Exception);
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
    CondVarSemaphore(int initial = 0) throw (castor::exception::Exception);
    ~CondVarSemaphore();
    void acquire() throw (castor::exception::Exception);
    bool tryAcquire() throw (castor::exception::Exception);
    void release(int n=1) throw (castor::exception::Exception);
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
  

  /**
   * An exception class thrown by the Thread class.
   */
  class UncaughtExceptionInThread: public castor::tape::Exception {
  public:
    UncaughtExceptionInThread(const std::string& w= ""): castor::tape::Exception(w) {}
  };

  /**
   * A Thread class, based on the Qt interface. To be used, on should
   * inherit from it, and implement the run() method.
   * The thread is started with start() and joined with wait().
   */
  class Thread {
  public:
    Thread(): m_hadException(false), m_what("") {}
    virtual ~Thread () {}
    void start() throw (castor::exception::Exception);
    void wait() throw (castor::exception::Exception);
  protected:
    virtual void run () = 0;
  private:
    pthread_t m_thread;
    bool m_hadException;
    std::string m_what;
    std::string m_type;
    static void * pthread_runner (void * arg);
  };
  
} // namespace threading
} // namespace tape
} // namespace castor
