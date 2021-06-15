/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "Thread.hpp"
#include <errno.h>
#include <typeinfo>
#include <stdlib.h>
#include <cxxabi.h>
#include <iostream>

namespace cta { 
namespace threading {
  
/* Implmentations of the threading primitives */
//------------------------------------------------------------------------------
//start
//------------------------------------------------------------------------------
void Thread::start()
 {
  cta::exception::Errnum::throwOnReturnedErrno(
    pthread_create(&m_thread, NULL, pthread_runner, this),
      "Error from pthread_create in cta::threading::Thread::start()");
  m_started = true;
}
//------------------------------------------------------------------------------
//wait
//------------------------------------------------------------------------------
void Thread::wait()
 {
  void *res;
  cta::exception::Errnum::throwOnReturnedErrno(
    pthread_join(m_thread, &res),
      "Error from pthread_join in cta::threading::Thread::wait()");
  if (m_hadException && res != PTHREAD_CANCELED) {
    std::string w = "Uncaught exception of type \"";
    w += m_type;
    w += "\" in Thread.run(): >>>>";
    w += m_what;
    w += "<<<< End of uncaught exception";
    throw UncaughtExceptionInThread(w);
  }
}
//------------------------------------------------------------------------------
//cancel
//------------------------------------------------------------------------------
void Thread::kill()
 {
  if (!m_started) throw cta::exception::Exception("Trying to kill a non-started thread!");
  std::cout << "About to kill thread:" << m_thread << " (0x" << std::hex << m_thread << std::dec << ")" << std::endl;
  cta::exception::Errnum::throwOnReturnedErrno(
    pthread_cancel(m_thread),
      "Error from pthread_cancel in cta::threading::Thread::cancel()");
}
//------------------------------------------------------------------------------
//pthread_runner
//------------------------------------------------------------------------------
void * Thread::pthread_runner (void * arg) {

  /* static_casting a pointer to and from void* preserves the address. 
   * See https://stackoverflow.com/questions/573294/when-to-use-reinterpret-cast
   */ 
  Thread * _this = static_cast<Thread *>(arg);

  // Set the thread cancellation type to immediate, for use in the tapeResourceManager tests
  cta::exception::Errnum::throwOnReturnedErrno(::pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL),
      "Error from pthread_setcancelstate in cta::threading::Thread::pthread_runner");
  cta::exception::Errnum::throwOnReturnedErrno(::pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL),
      "Error from pthread_setcanceltype in cta::threading::Thread::pthread_runner");
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
    throw;
  }
  return NULL;
}

} // namespace threading
} // namespace cta