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

#include "common/threading/CondVar.hpp"
#include "common/threading/Mutex.hpp"
#include "common/threading/MutexLocker.hpp"
#include "common/threading/Thread.hpp"

#include <gtest/gtest.h>
#include <stdint.h>

namespace unitTests {

class cta_threading_CondVarTest : public ::testing::Test {
protected:
  virtual void SetUp() {}

  virtual void TearDown() {}
};

class WaitingThread : public cta::threading::Thread {
public:
  WaitingThread(cta::threading::CondVar& cond, cta::threading::Mutex& m) : m_cond(cond), m_mutex(m) {}

  void run() override {
    cta::threading::MutexLocker locker(m_mutex);
    m_cond.wait(locker);
  }

private:
  cta::threading::CondVar& m_cond;
  cta::threading::Mutex& m_mutex;
};  // class WaitingThread

class CounterThread : public cta::threading::Thread {
public:
  enum CounterType { ODD_COUNTER, EVEN_COUNTER };

  enum NotificationType { SIGNAL_NOTIFICATION, BROADCAST_NOTIFICATION };

  CounterThread(const CounterType counterType,
                const NotificationType notificationType,
                cta::threading::Mutex& counterMutex,
                cta::threading::CondVar& counterIsOdd,
                cta::threading::CondVar& counterIsEven,
                uint64_t& counter,
                const uint64_t maxCount) :
  m_counterType(counterType),
  m_notificationType(notificationType),
  m_counterMutex(counterMutex),
  m_counterIsOdd(counterIsOdd),
  m_counterIsEven(counterIsEven),
  m_counter(counter),
  m_maxCount(maxCount) {}

  void run() override {
    if (m_counterType == ODD_COUNTER) {
      incrementOddCounterValuesUntilMax();
    }
    else {  // EVEN_COUNTER
      incrementEvenCounterValuesUntilMax();
    }
  }

  void incrementOddCounterValuesUntilMax() {
    cta::threading::MutexLocker locker(m_counterMutex);

    while (m_counter < m_maxCount) {
      const bool counterValueIsOdd = m_counter % 2;
      if (counterValueIsOdd) {
        m_counter++;
        if (m_notificationType == SIGNAL_NOTIFICATION) {
          m_counterIsEven.signal();
        }
        else {  // BROADCAST_NOTIFICATION
          m_counterIsEven.broadcast();
        }
      }

      if (m_counter < m_maxCount) {
        m_counterIsOdd.wait(locker);
      }
    }
  }

  void incrementEvenCounterValuesUntilMax() {
    cta::threading::MutexLocker locker(m_counterMutex);

    while (m_counter < m_maxCount) {
      const bool counterValueIsEven = !(m_counter % 2);
      if (counterValueIsEven) {
        m_counter++;
        if (m_notificationType == SIGNAL_NOTIFICATION) {
          m_counterIsOdd.signal();
        }
        else {  // BROADCAST_NOTIFICATION
          m_counterIsOdd.broadcast();
        }
      }

      if (m_counter < m_maxCount) {
        m_counterIsEven.wait(locker);
      }
    }
  }

private:
  const CounterType m_counterType;
  const NotificationType m_notificationType;
  cta::threading::Mutex& m_counterMutex;
  cta::threading::CondVar& m_counterIsOdd;
  cta::threading::CondVar& m_counterIsEven;
  uint64_t& m_counter;
  const uint64_t m_maxCount;
};  // class CounterThread

TEST_F(cta_threading_CondVarTest, waitAndSignal) {
  using namespace cta::threading;

  cta::threading::Mutex counterMutex;
  cta::threading::CondVar counterIsOdd;
  cta::threading::CondVar counterIsEven;
  uint64_t counter = 0;
  const uint64_t maxCounter = 1024;

  CounterThread oddCounter(CounterThread::ODD_COUNTER, CounterThread::SIGNAL_NOTIFICATION, counterMutex, counterIsOdd,
                           counterIsEven, counter, maxCounter);
  CounterThread evenCounter(CounterThread::EVEN_COUNTER, CounterThread::SIGNAL_NOTIFICATION, counterMutex, counterIsOdd,
                            counterIsEven, counter, maxCounter);

  oddCounter.start();
  evenCounter.start();

  oddCounter.wait();
  evenCounter.wait();
}

TEST_F(cta_threading_CondVarTest, waitAndBroadcast) {
  using namespace cta::threading;

  cta::threading::Mutex counterMutex;
  cta::threading::CondVar counterIsOdd;
  cta::threading::CondVar counterIsEven;
  uint64_t counter = 0;
  const uint64_t maxCounter = 1024;

  CounterThread oddCounter(CounterThread::ODD_COUNTER, CounterThread::BROADCAST_NOTIFICATION, counterMutex,
                           counterIsOdd, counterIsEven, counter, maxCounter);
  CounterThread evenCounter(CounterThread::EVEN_COUNTER, CounterThread::BROADCAST_NOTIFICATION, counterMutex,
                            counterIsOdd, counterIsEven, counter, maxCounter);

  oddCounter.start();
  evenCounter.start();

  oddCounter.wait();
  evenCounter.wait();
}

}  // namespace unitTests
