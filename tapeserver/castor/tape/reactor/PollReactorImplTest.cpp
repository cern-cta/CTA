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

#include "castor/tape/reactor/DummyPollEventHandler.hpp"
#include "castor/tape/reactor/PollReactorImpl.hpp"
#include "castor/log/DummyLogger.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class castor_tape_reactor_PollReactorImplTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(castor_tape_reactor_PollReactorImplTest, goodDayRegisterAndLeave) {
  castor::log::DummyLogger log("unittests");
  castor::tape::reactor::PollReactorImpl reactor(log);
  reactor.registerHandler(
    new castor::tape::reactor::DummyPollEventHandler(1234, false));
}

TEST_F(castor_tape_reactor_PollReactorImplTest, goodDayRegisterAndRemove) {
  castor::log::DummyLogger log("unittests");
  castor::tape::reactor::PollReactorImpl reactor(log);
  reactor.registerHandler(
    new castor::tape::reactor::DummyPollEventHandler(1234, true));
}

TEST_F(castor_tape_reactor_PollReactorImplTest, registerTwiceTheSameHandler) {
  castor::log::DummyLogger log("unittests");
  castor::tape::reactor::PollReactorImpl reactor(log);
  castor::tape::reactor::DummyPollEventHandler *handler =
    new castor::tape::reactor::DummyPollEventHandler(1234,false);
  reactor.registerHandler(handler);
  ASSERT_THROW(reactor.registerHandler(handler), cta::exception::Exception);
}

} // namespace unitTests
