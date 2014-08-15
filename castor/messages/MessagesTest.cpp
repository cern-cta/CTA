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

#include "castor/messages/messages.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class castor_messages : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

/**
 * Tests the translation of ZMQ specific errno values.
 */
TEST_F(castor_messages, zmqSpecificErrnoValues) {
  using namespace castor::messages;

  ASSERT_EQ(std::string("Operation cannot be accomplished in current state"),
    zmqErrnoToStr(EFSM));
  ASSERT_EQ(std::string("The protocol is not compatible with the socket type"),
    zmqErrnoToStr(ENOCOMPATPROTO));
  ASSERT_EQ(std::string("Context was terminated"),
    zmqErrnoToStr(ETERM));
  ASSERT_EQ(std::string("No thread available"),
    zmqErrnoToStr(EMTHREAD));
}

/**
 * Tests the translation of non-ZMQ errno values.
 */
TEST_F(castor_messages, nonZmqrrnoValues) {
  using namespace castor::messages;

  ASSERT_EQ(std::string("Invalid argument"),
    zmqErrnoToStr(EINVAL));
}

} // namespace unitTests
