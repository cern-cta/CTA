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

#include "Threading.hpp"
#include <errno.h>
#include <typeinfo>
#include <stdlib.h>
#include <cxxabi.h>
#include "castor/BaseObject.hpp"

/* Implmentations of the threading primitives */
//------------------------------------------------------------------------------
//start
//------------------------------------------------------------------------------
void castor::server::Thread::start()
 {
  cta::exception::Errnum::throwOnReturnedErrno(
    pthread_create(&m_thread, NULL, pthread_runner, this),
      "Error from pthread_create in castor::server::Thread::start()");
}
//------------------------------------------------------------------------------
//wait
//------------------------------------------------------------------------------
void castor::server::Thread::wait()
 {
  cta::exception::Errnum::throwOnReturnedErrno(
    pthread_join(m_thread, NULL),
      "Error from pthread_join in castor::server::Thread::wait()");
  if (m_hadException) {
    std::string w = "Uncaught exception of type \"";
    w += m_type;
    w += "\" in Thread.run():\n>>>>\n";
    w += m_what;
    w += "<<<< End of uncaught exception";
    throw UncaughtExceptionInThread(w);
  }
}
//------------------------------------------------------------------------------
//pthread_runner
//------------------------------------------------------------------------------
void * castor::server::Thread::pthread_runner (void * arg) {

  /* static_casting a pointer to and from void* preserves the address. 
   * See https://stackoverflow.com/questions/573294/when-to-use-reinterpret-cast
   */ 
  Thread * _this = static_cast<Thread *>(arg);
   
  // The threading init is needing by many castor components, so better do
  // it all the time (this should not have side effects)
  try {
    _this->run();
  } catch (std::exception & e) {
    _this->m_hadException = true;
    int status = -1;
    char * demangled = abi::__cxa_demangle(typeid(e).name(), NULL, NULL, &status);
    if (!status) {
      _this->m_type += demangled;
    } else {
      _this->m_type = typeid(e).name();
    }
    free(demangled);
    _this->m_what = e.what();
  } catch (...) {
    _this->m_hadException = true;
    _this->m_type = "unknown";
    _this->m_what = "uncaught non-standard exception";
  }
  BaseObject::resetServices();
  return NULL;
}
