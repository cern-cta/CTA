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

#include "common/exception/Exception.hpp"
#include "common/threading/RWLock.hpp"
#include "common/threading/RWLockWrLocker.hpp"

namespace cta { 
namespace threading {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
RWLockWrLocker::RWLockWrLocker(RWLock &lock): m_lock(lock) {
  try {
    m_lock.wrlock();
  } catch(exception::Exception &ne) {
    exception::Exception ex;
    ex.getMessage() << __FUNCTION__ << " failed to take write lock: " << ne.getMessage().str();
    throw ex;
  }
}
  
//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
RWLockWrLocker::~RWLockWrLocker() {
  m_lock.unlock();
}

} // namespace threading
} // namespace cta
