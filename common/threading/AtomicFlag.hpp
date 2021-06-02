/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
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

#include "common/threading/MutexLocker.hpp"
#include "common/threading/Thread.hpp"

namespace cta {
namespace threading {
  
//A 1 way flag : Once set, it can be reset
struct AtomicFlag{
  AtomicFlag(): m_set(false) {};
     void set()  {
        MutexLocker ml(m_mutex);
        m_set=true;
      }
     operator bool() const {
        MutexLocker ml(m_mutex);
        return m_set;
      }
    private:
      bool m_set;
      mutable Mutex m_mutex;
};

} // namespace threading
} // namespace cta