/******************************************************************************
 *                castor/tape/tapeserver/daemon/Vdqm.hpp
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

#ifndef CASTOR_TAPE_TAPESERVER_DAEMON_VDQM_HPP
#define CASTOR_TAPE_TAPESERVER_DAEMON_VDQM_HPP 1

#include "castor/exception/Exception.hpp"
#include "castor/tape/legacymsg/RtcpJobRqstMsgBody.hpp"

namespace castor     {
namespace tape       {
namespace tapeserver {
namespace daemon     {

/**
 * Proxy class representing the vdqm daemon.
 */
class Vdqm {
public:

  /**
   * Destructor.
   */
  virtual ~Vdqm() throw() = 0;

  /**
   * Receives a job from the specified connection with the vdqm daemon,
   * sends back a positive acknowledgement and closes the connection.
   *
   * @param connection The file descriptor of the connection with the vdqm
   * daemon.
   * @return The job request from the vdqm.
   */
  virtual legacymsg::RtcpJobRqstMsgBody receiveJob(const int connection)
    throw(castor::exception::Exception) = 0;

  /**
   * Sets the status of the specified tape drive to down.
   *
   * @param server The host name of the server to which the tape drive is
   * attached.
   * @param unitName The unit name of the tape drive. 
   * @param dgn The device group name of the tape drive.
   */
  virtual void setTapeDriveStatusDown(const std::string &server,
    const std::string &unitName, const std::string &dgn)
    throw(castor::exception::Exception) = 0;

  /**
   * Sets the status of the specified tape drive to up.
   *
   * @param server The host name of the server to which the tape drive is
   * attached.
   * @param unitName The unit name of the tape drive.
   * @param dgn The device group name of the tape drive.
   */
  virtual void setTapeDriveStatusUp(const std::string &server,
    const std::string &unitName, const std::string &dgn)
    throw(castor::exception::Exception) = 0;

}; // class Vdqm

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TAPESERVER_DAEMON_VDQM_HPP
