/******************************************************************************
 *         castor/tape/tapeserver/daemon/DummyVdqm.hpp
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef CASTOR_TAPE_TAPESERVER_DAEMON_DUMMYVDQM_HPP
#define CASTOR_TAPE_TAPESERVER_DAEMON_DUMMYVDQM_HPP 1

#include "castor/tape/legacymsg/MessageHeader.hpp"
#include "castor/tape/tapeserver/daemon/Vdqm.hpp"

namespace castor     {
namespace tape       {
namespace tapeserver {
namespace daemon     {

/**
 * A dummy vdqm.
 *
 * The main goal of this class is to facilitate the development of unit tests.
 */
class DummyVdqm: public Vdqm {
public:

  /**
   * Constructor.
   *
   * @param job The vdqm job to be returned by the receiveJob() method.
   */
  DummyVdqm(const legacymsg::RtcpJobRqstMsgBody &job) throw();

  /**
   * Destructor.
   *
   * Closes the listening socket created in the constructor to listen for
   * connections from the vdqmd daemon.
   */
  ~DummyVdqm() throw();

  /**
   * Receives a job from the specified connection with the vdqm daemon,
   * sends back a positive acknowledgement and closes the connection.
   *
   * @param connection The file descriptor of the connection with the vdqm
   * daemon.
   * @param netTimeout The timeout to be applied when performing network read
   * and write operations.
   * @return The job request from the vdqm.
   */
  legacymsg::RtcpJobRqstMsgBody receiveJob(const int connection,
    const int netTimeout) throw(castor::exception::Exception);

private:

  /**
   * The vdqm job to be returned by the receiveJob() method.
   */
  legacymsg::RtcpJobRqstMsgBody m_job;

}; // class DummyVdqm

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TAPESERVER_DAEMON_DUMMYVDQM_HPP
