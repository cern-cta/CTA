/*
 * The CERN Tape Archive(CTA) project
 * Copyright(C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *(at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "common/exception/InvalidArgument.hpp"
extern "C" {
#include "acssys.h"
#include "acsapi.h"
}

#include <string>
#include <stdint.h>

namespace cta {
namespace mediachanger {
namespace acs {

/**
 * Abstract class that defines the interface to an object that wraps the ACLS
 * C-API.
 */
class Acs {
public:
  /**
   * Pure-virtual destructor to ensure this class is abstract.
   */
  virtual ~Acs() = 0;

  /**
   * Parses the specified string and returns the corresponding drive ID object.
   *
   * This method throws a cta::exception::InvalidArgument if the syntax of
   * the string is invalid.
   *
   * @param str The string to be parsed.
   * @return The drive ID object.
   */
  static DRIVEID str2DriveId(const std::string &str);

  /**
   * Converts acs, lsm, panel, drive numbers to the corresponding 
   * drive ID object.
   * 
   * @param acs The integer for acs.
   * @param lsm The integer for lsm.
   * @param panel The integer for panel.
   * @param drive The integer for drive.
   * @return The drive ID object.
   */
  static DRIVEID alpd2DriveId(const uint32_t acs, const uint32_t lsm, 
    const uint32_t panel, const uint32_t drive);

  /**
   * Returns the VOLID equibvalent of the specified string.
   *
   * This method throws a cta::exception::InvalidArgument if the string is
   * longer than EXTERNAL_LABEL_SIZE.
   *
   * @param str The string representation of the volume identifier.
   * @return The VOLID representation of the volume identifier.
   */
  static VOLID str2Volid(const std::string &str);
  
  /**
   * Returns the string reprsentation of the specified drive identifier.
   *
   * The string format is ACS:LSM:panel:drive
   *
   * @param driveId The drive identifier.
   * @return The string representation.
   */
  static std::string driveId2Str(const DRIVEID &driveId);
  
  /**
   * C++ wrapper around the acs_mount() function of the ACSLS C-API.
   *
   * @param seqNumber Client supplied sequence number.
   * @param lockId Lock identifier or 0 meaning no lock.
   * @param volId The indentifier of volume to be mounted.
   * @param driveId The ID of the drive into which the volume is to be mounted.
   * @param readOnly Set to true to request the volume be mounted for read-only
   * access.
   * @param bypass Set to true to override the ACSLS verification of
   * compatibility between the drive and the media type of the volume.
   * @return status value returned by acs_mount().
   */
  virtual STATUS mount(
    const SEQ_NO seqNumber,
    const LOCKID lockId,
    const VOLID &volId,
    const DRIVEID &driveId,
    const BOOLEAN readOnly,
    const BOOLEAN bypass)
    = 0;

  /**
   * C++ wrapper around the acs_dismount() function of the ACSLS C-API.
   *
   * @param seqNumber Client supplied sequence number.
   * @param lockId Lock identifier or 0 meaning no lock.
   * @param volId The identifier of the volume to be mounted.
   * @param driveId The ID of the drive into which the volume is to be mounted.
   * @param force Set to true if the dismount should be forced.  Forcing a
   * dismount means dismounting the volume from the specified drive without
   * checking the identifier of the volume.
   * @return status value returned by acs_dismount().
   */
  virtual STATUS dismount(
    const SEQ_NO seqNumber,
    const LOCKID lockId,
    const VOLID &volId,
    const DRIVEID &driveId,
    const BOOLEAN force)
    = 0;

  /**
   * C++ wrapper around the acs_response() function of the ACSLS C-API.
   *
   * @param timeout Time in seconds to wait for a response.  A value of -1
   * means block indefinitely and an a value of 0 means poll for the existence
   * of a response.
   * @param seqNumber Output parameter.  If a response exists then seqNumber
   * is set.
   * @param reqId Output parameter.  For an acknowledge response reqId is set
   * to the request identifier of the original request. For an intermediate or
   * final response reqId will be set to 0.
   * @param rType Output parameter.  Set to the type of the response.
   * @param rBuf Output parameter.  Set to the response information.
   * @return status value returned by acs_response().
   */
  virtual STATUS response(
    const int timeout,
    SEQ_NO &seqNumber,
    REQ_ID &reqId,
    ACS_RESPONSE_TYPE &rType,
    ALIGNED_BYTES rBuf) = 0;

  /**
   * C++ wrapper around the acs_query_volume() function of the ACSLS C-API.
   *
   * @param seqNumber Client supplied sequence number.
   * @param volIds Array of the volume identifiers to be queried.
   * @param count The number of volume identifiers contained iwthin the volId
   * parameter.
   * @return status value returned by acs_response().
   */
  virtual STATUS queryVolume(
    const SEQ_NO seqNumber,
    VOLID (&volIds)[MAX_ID],
    const unsigned short count) = 0;

protected:

  /**
   * Returns true if the specified string only contains numerals else false.
   *
   * @return True if the specified string only contains numerals else false.
   */
  static bool onlyContainsNumerals(const std::string &str);

}; // class  Acs

} // namespace acs
} // namespace mediachanger
} // namespace cta
