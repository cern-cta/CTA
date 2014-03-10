/******************************************************************************
 *         castor/tape/tapeserver/daemon/VdqmImpl.hpp
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

#ifndef CASTOR_TAPE_TAPESERVER_DAEMON_VDQMIMPL_HPP
#define CASTOR_TAPE_TAPESERVER_DAEMON_VDQMIMPL_HPP 1

#include "castor/tape/legacymsg/MessageHeader.hpp"
#include "castor/tape/tapeserver/daemon/Vdqm.hpp"

namespace castor     {
namespace tape       {
namespace tapeserver {
namespace daemon     {

/**
 * A concrete implementation of the interface to the vdqm daemon.
 */
class VdqmImpl: public Vdqm {
public:

  /**
   * Destructor.
   *
   * Closes the listening socket created in the constructor to listen for
   * connections from the vdqmd daemon.
   */
  ~VdqmImpl() throw();

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
   * Receives the header of a job message from the specified connection with
   * the vdqm daemon.
   *
   * @param connection The file descriptor of the connection with the vdqm
   * daemon.
   * @param netTimeout The timeout to be applied when performing network read
   * and write operations.
   * @return The message header.
   */
  legacymsg::MessageHeader receiveJobMsgHeader(const int connection,
    const int netTimeout) throw(castor::exception::Exception);

  /**
   * Throws an exception if the specified magic number is invalid for a vdqm
   * job message.
   */
  void checkJobMsgMagic(const uint32_t magic) const
    throw(castor::exception::Exception);

  /**
   * Throws an exception if the specified request type is invalid for a vdqm
   * job message.
   */
  void checkJobMsgReqType(const uint32_t reqType) const
    throw(castor::exception::Exception);

  /**
   * Receives the body of a job message from the specified connection with the
   * vdqm daemon.
   *
   * @param connection The file descriptor of the connection with the vdqm
   * daemon.
   * @param netTimeout The timeout to be applied when performing network read
   * and write operations.
   * @param len The length of the message body in bytes.
   * @return The message body.
   */
  legacymsg::RtcpJobRqstMsgBody receiveJobMsgBody(const int connection,
    const int netTimeout, const uint32_t len)
    throw(castor::exception::Exception);

  /**
   * Throws an exception if the specified message body length is invalid for
   * a vdqm job message.
   *
   * @param maxLen The maximum length in bytes
   * @param len The actual length in bytes.
   */
  void checkJobMsgLen(const size_t maxLen, const size_t len) const
    throw(castor::exception::Exception);

}; // class VdqmImpl

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TAPESERVER_DAEMON_VDQMIMPL_HPP
