/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include "common/exception/Errnum.hpp"
#include "common/exception/Exception.hpp"
#include "common/threading/Mutex.hpp"

#include <optional>
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
  explicit Thread(std::optional<size_t> stackSize): m_hadException(false), m_what(""), m_started(false), m_stackSize(stackSize) {}

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
  std::optional<size_t> m_stackSize;
};

} // namespace threading
} // namespace cta
