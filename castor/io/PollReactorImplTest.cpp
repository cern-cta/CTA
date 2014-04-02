/******************************************************************************
 *                      castor/io/PollReactorImplTest.cpp
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/io/DummyPollEventHandler.hpp"
#include "castor/io/PollReactorImpl.hpp"
#include "castor/log/DummyLogger.hpp"

#include <gtest/gtest.h>

namespace unitTests {

class castor_io_PollReactorImplTest : public ::testing::Test {
protected:

  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(castor_io_PollReactorImplTest, goodDayRegistier) {
  castor::log::DummyLogger log("unittests");
  castor::io::PollReactorImpl reactor(log);
  reactor.registerHandler(new castor::io::DummyPollEventHandler(1234));
}

TEST_F(castor_io_PollReactorImplTest, registerTwiceTheSameHandler) {
  castor::log::DummyLogger log("unittests");
  castor::io::PollReactorImpl reactor(log);
  castor::io::DummyPollEventHandler *handler =
    new castor::io::DummyPollEventHandler(1234);
  reactor.registerHandler(handler);
  ASSERT_THROW(reactor.registerHandler(handler), castor::exception::Exception);
}

} // namespace unitTests
