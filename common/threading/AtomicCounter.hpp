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

#include "common/threading/MutexLocker.hpp"
#include "common/threading/Thread.hpp"

namespace cta {
namespace threading {
/**
* A helper class managing a thread safe message counter
 * When C++11 will be used, just delete it to use std::atomic
*/
template <class T> struct AtomicCounter{
  AtomicCounter(T init = 0): m_val(init) {};
      T operator ++ () {
        MutexLocker ml(m_mutex);
        return ++m_val;
      }
      T operator ++ (int) {
        MutexLocker ml(m_mutex);
        return m_val++;
      }
      T operator -- () {
        MutexLocker ml(m_mutex);
        return --m_val;
      }
      operator T() const {
        MutexLocker ml(m_mutex);
        return m_val;
      }
      
     T getAndReset(){
       MutexLocker ml(m_mutex);
        T old =m_val;
        m_val=0;
        return old;
     }
    private:
      T m_val;
      mutable Mutex m_mutex;
};
} // namespace threading
} // namespace cta

