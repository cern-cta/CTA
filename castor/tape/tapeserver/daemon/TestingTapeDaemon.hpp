/******************************************************************************
 *         castor/tape/tapeserver/daemon/TestingTapeDaemon.hpp
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

#ifndef CASTOR_TAPE_TAPESERVER_DAEMON_TESTINGTAPEDAEMON_HPP
#define CASTOR_TAPE_TAPESERVER_DAEMON_TESTINGTAPEDAEMON_HPP 1

#include "castor/tape/tapeserver/daemon/TapeDaemon.hpp"

namespace castor     {
namespace tape       {
namespace tapeserver {
namespace daemon     {

/**
 * Class that faciliates unit testing TapeDaemon by making public the
 * appropriate protected class memebers.
 */
class TestingTapeDaemon : public TapeDaemon {
public:

  /**
   * Constructor.
   *
   * @param stdOut Stream representing standard out.
   * @param stdErr Stream representing standard error.
   * @param log The object representing the API of the CASTOR logging system.
   * @param vdqm The object representing the vdqmd daemon.
   * @param reactor The reactor responsible for dispatching the I/O events of
   * the parent process of the tape server daemon.
   */
  TestingTapeDaemon(std::ostream &stdOut, std::ostream &stdErr,
    log::Logger &log, Vdqm &vdqm, io::PollReactor &reactor)
    throw(castor::exception::Exception):
    TapeDaemon(stdOut, stdErr, log, vdqm, reactor) {
  }

  using TapeDaemon::checkTpconfigDgns;

}; // class TestingTapeDaemon

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TAPESERVER_DAEMON_TESTINGTAPEDAEMON_HPP
