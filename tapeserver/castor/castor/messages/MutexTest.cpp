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

#include "castor/exception/Errnum.hpp"
#include "castor/messages/Mutex.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class castor_messages_MutexTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST(castor_messages_MutexTest, Mutex_properly_throws_exceptions) {
  using namespace castor::messages;

  // Check that we properly get exception when doing wrong semaphore operations
  Mutex m;
  ASSERT_NO_THROW(m.lock());

  // Duplicate lock
  ASSERT_THROW(m.lock(),castor::exception::Errnum);
  ASSERT_NO_THROW(m.unlock());

  // Duplicate release
  ASSERT_THROW(m.unlock(),castor::exception::Errnum);
}

} // namespace unitTests
