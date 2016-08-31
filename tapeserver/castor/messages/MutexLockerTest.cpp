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

#include "common/exception/Errnum.hpp"
#include "castor/messages/MutexLocker.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class castor_messages_MutexLockerTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST(castor_messages_MutexLockerTest,
  MutexLocker_locks_and_properly_throws_exceptions) {
  using namespace castor::messages;

  Mutex m;
  {
    MutexLocker ml(&m);
    // This is a different flavour of duplicate locking
    ASSERT_THROW(m.lock(),cta::exception::Errnum);
    ASSERT_NO_THROW(m.unlock());
    ASSERT_NO_THROW(m.lock());
  }

  // As the locker has been destroyed, and the mutex released, another
  // lock/unlock should be possible
  ASSERT_NO_THROW(m.lock());
  ASSERT_NO_THROW(m.unlock());
}

} // namespace unitTests
