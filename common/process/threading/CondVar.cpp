/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/process/threading/CondVar.hpp"

#include "common/exception/Exception.hpp"
#include "common/process/threading/MutexLocker.hpp"
#include "common/utils/utils.hpp"

namespace cta::threading {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
CondVar::CondVar() {
  const int initRc = pthread_cond_init(&m_cond, nullptr);
  if (0 != initRc) {
    throw exception::Exception("Failed to initialise condition variable");
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
    throw exception::Exception("Underlying mutex is not locked.");
  }

  const int waitRc = pthread_cond_wait(&m_cond, &locker.m_mutex.m_mutex);
  if (0 != waitRc) {
    throw exception::Exception("pthread_cond_wait failed:" + utils::errnoToString(waitRc));
  }
}

//------------------------------------------------------------------------------
// signal
//------------------------------------------------------------------------------
void CondVar::signal() {
  const int signalRc = pthread_cond_signal(&m_cond);
  if (0 != signalRc) {
    throw exception::Exception("pthread_cond_signal failed:" + utils::errnoToString(signalRc));
  }
}

//------------------------------------------------------------------------------
// broadcast
//------------------------------------------------------------------------------
void CondVar::broadcast() {
  const int broadcastRc = pthread_cond_broadcast(&m_cond);
  if (0 != broadcastRc) {
    throw exception::Exception("pthread_cond_broadcast failed:" + utils::errnoToString(broadcastRc));
  }
}

}  // namespace cta::threading
