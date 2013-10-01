/******************************************************************************
 *    test/unittest/castor/tape/mediachanger/MockAcs.hpp
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

#ifndef TEST_UNITTEST_CASTOR_TAPE_MEDIACHANGER_MOCKACS_HPP
#define TEST_UNITTEST_CASTOR_TAPE_MEDIACHANGER_MOCKACS_HPP 1

#include "castor/tape/mediachanger/AcsCmd.hpp"

namespace castor {
namespace tape {
namespace mediachanger {

class MockAcs: public Acs {
public:

  /**
   * Destructor.
   */
  ~MockAcs() throw() {
  }

  /**
   * C++ wrapper around the acs_mount function of the ACSLS C-API.
   *
   * @param seqNumber Client supplied sequence number.
   * @param lockId Lock identifier or 0 meaning no lock.
   * @param volId The volume ID of the tape to be mounted.
   * @param driveId The ID of the drive into which the tape is to be mounted.
   * @param readOnly Set to true to request the tape be mounted for read-only
   * access.
   * @param bypass Set to true to override the ACSLS verification of
   * compatibility between the tape drive and the media type of the cartridge.
   */
  void mount(
    const SEQ_NO seqNumber,
    const LOCKID lockId,
    const std::string &volId,
    const DRIVEID &driveId,
    const bool readOnly,
    const bool bypass)
    throw(castor::exception::Internal, castor::exception::MountFailed);

  /**
   * C++ wrapper around the acs_response function of the ACSLS C-API.
   *
   * @param timeout Time in seconds to wait for a response.  A value of -1
   * means block indefinitely and an a value of 0 means poll for the existence
   * of a response.
   * @param seqNumber Output parameter.  If a response exists then seqNumber
   * is set.
   * @param reqId Output parameter.  For an acknowledge response reqId is set
   * to the request identifier of the original request. For an intermediate or
   * final response reqId will be set to 0.
   * @param type Output parameter.  Set to the type of the response.
   * @param buffer Output parameter.  Set to the response information.
   * @return The status.
   */
  STATUS response(
    const int timeout,
    SEQ_NO &seqNumber,
    REQ_ID &reqId,
    ACS_RESPONSE_TYPE &type,
    ALIGNED_BYTES *buffer)
    throw();
}; // class MockAcs

} // namespace mediachanger
} // namespace tape
} // namespace castor

#endif // TEST_UNITTEST_CASTOR_TAPE_MEDIACHANGER_MOCKACS_HPP
