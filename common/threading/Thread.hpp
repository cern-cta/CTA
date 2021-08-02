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

#pragma once

#include "common/exception/Errnum.hpp"
#include "common/exception/Exception.hpp"
#include "common/threading/Mutex.hpp"

#include <pthread.h>
#include <semaphore.h>

namespace cta {
namespace threading { 

/**
* An exception class thrown by the Thread class.
*/
CTA_GENERATE_EXCEPTION_CLASS(UncaughtExceptionInThread);

/**
* A Thread class, based on the Qt interface. To be used, on should
* inherit from it, and implement the run() method.
* The thread is started with start() and joined with wait().
*/
class Thread {
public:
  Thread(): m_hadException(false), m_what(""), m_started(false) {}
virtual ~Thread () {}
  void start() ;
  void wait() ;
  void kill();
protected:
  virtual void run () = 0;
private:
  pthread_t m_thread;
  bool m_hadException;
  std::string m_what;
  std::string m_type;
  static void * pthread_runner (void * arg);
  bool m_started;
};
  
} // namespace threading
} // namespace cta