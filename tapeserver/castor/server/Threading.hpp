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
#include "common/exception/Errnum.hpp"
#include "castor/server/Mutex.hpp"
#include "common/exception/Exception.hpp"

namespace castor {
namespace server { 

  

  /**
   * An exception class thrown by the Thread class.
   */
  class UncaughtExceptionInThread: public cta::exception::Exception {
  public:
    UncaughtExceptionInThread(const std::string& w= ""): cta::exception::Exception(w) {}
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
    void start() ;
    void wait() ;
  protected:
    virtual void run () = 0;
  private:
    pthread_t m_thread;
    bool m_hadException;
    std::string m_what;
    std::string m_type;
    static void * pthread_runner (void * arg);
  };
  
}}
