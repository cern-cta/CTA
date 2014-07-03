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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/tape/reactor/ZMQPollEventHandler.hpp"
#include "castor/tape/reactor/ZMQReactor.hpp"
#include "castor/log/DummyLogger.hpp"

#include <gtest/gtest.h>
#include <memory>
#include <sys/types.h>
#include <sys/socket.h>

// Anonymous namespace used to hide the TestEventHandler class from the rest
// of the World
namespace {

/**
 * Event handler used soley by this test file.
 */
class TestEventHandler: public castor::tape::reactor::ZMQPollEventHandler {
public:

  /**
   * Constructor.
   *
   * @param fd File descriptor to be owned by this event handler.  The file
   * descriptor will be closed by the destructor of this class.
   */
  TestEventHandler(const int fd) throw(): m_fd(fd) {
  }

  /**
   * Returns the human-readable name this event handler.
   */
  std::string getName() const throw() {
    return "TestEventHandler";
  }

  /**
   * Fills the specified poll file-descriptor ready to be used in a call to
   * poll().
   */
  void fillPollFd(zmq_pollitem_t &fd) {
    fd.fd = m_fd;
    fd.events = ZMQ_POLLIN;
    fd.revents = 0;
    fd.socket = NULL;
  }

  /**
   * Returns true indicating that this event handler should be removed from and
   * deleted by the reactor.
   *
   * @param fd The poll file-descriptor describing the event.
   * @return True indicating that this event handler should be removed from and
   * deleted by the reactor.
   */
  bool handleEvent(const zmq_pollitem_t &fd) {
    return true;
  }

  /**
   * Destructor.
   *
   * Calls close on the owned file descriptor.
   */
  ~TestEventHandler() throw() {
    close(m_fd);
  }

private:

  /**
   * File descriptor to be returned by getFd().
   */
  const int m_fd;

}; // class DummyPollEventHandler

} // anonymous namespace

namespace unitTests {

class castor_tape_reactor_ZMQReactorTest : public ::testing::Test {
protected:
  virtual void SetUp() {
  }

  virtual void TearDown() {
  }
};

TEST_F(castor_tape_reactor_ZMQReactorTest, constructor) {
  using namespace castor::tape::reactor;

  castor::log::DummyLogger log("unittests");
  std::auto_ptr<ZMQReactor> reactor;

  ASSERT_NO_THROW(reactor.reset(new ZMQReactor(log)));
}

/*
IMPORTANT NOTE:

This test should be uncommented when the destructor of
castor::tape::reactor::ZMQReactor is made virtual as it should be.  The
castorUnitsTest binary should also be ran with valgrind configured to track
file-descriptors, for example:

valgrind --track-fds=yes test/castorUnitTests

Please note that the destructor of castor::tape::reactor::ZMQReactor is
currently not a virtual method in order to all the new tape server to run
ableit with leakiing file descriptors.

TEST_F(castor_tape_reactor_ZMQReactorTest, closeFd) {
  using namespace castor::tape::reactor;

  castor::log::DummyLogger log("unittests");
  ZMQReactor reactor(log);

  int sv[2] = {-1, -1};
  ASSERT_EQ(0, socketpair(AF_LOCAL, SOCK_STREAM, 0, sv));

  std::auto_ptr<TestEventHandler> handler1;
  std::auto_ptr<TestEventHandler> handler2;

  handler1.reset(new TestEventHandler(sv[0]));
  handler2.reset(new TestEventHandler(sv[1]));

  reactor.registerHandler(handler1.get());
  handler1.release();
}
*/

} // namespace unitTests
