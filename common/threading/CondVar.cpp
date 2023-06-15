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

#include "common/exception/Exception.hpp"
#include "common/threading/CondVar.hpp"
#include "common/threading/MutexLocker.hpp"
#include "common/utils/utils.hpp"

namespace cta {
namespace threading {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
CondVar::CondVar() {
  const int initRc = pthread_cond_init(&m_cond, nullptr);
  if (0 != initRc) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Failed to initialise condition variable");
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
CondVar::~CondVar() {
  pthread_cond_destroy(&m_cond);
}

//------------------------------------------------------------------------------
// wait
//------------------------------------------------------------------------------
void CondVar::wait(MutexLocker& locker) {
  if (!locker.m_locked) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: Underlying mutex is not locked.");
  }

  const int waitRc = pthread_cond_wait(&m_cond, &locker.m_mutex.m_mutex);
  if (0 != waitRc) {
    throw exception::Exception(std::string(__FUNCTION__) +
                               " failed: pthread_cond_wait failed:" + utils::errnoToString(waitRc));
  }
}

//------------------------------------------------------------------------------
// signal
//------------------------------------------------------------------------------
void CondVar::signal() {
  const int signalRc = pthread_cond_signal(&m_cond);
  if (0 != signalRc) {
    throw exception::Exception(std::string(__FUNCTION__) +
                               " failed: pthread_cond_signal failed:" + utils::errnoToString(signalRc));
  }
}

//------------------------------------------------------------------------------
// broadcast
//------------------------------------------------------------------------------
void CondVar::broadcast() {
  const int broadcastRc = pthread_cond_broadcast(&m_cond);
  if (0 != broadcastRc) {
    throw exception::Exception(std::string(__FUNCTION__) +
                               " failed: pthread_cond_broadcast failed:" + utils::errnoToString(broadcastRc));
  }
}

}  // namespace threading
}  // namespace cta
