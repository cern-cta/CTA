/******************************************************************************
 *         castor/tape/utils/DriveConfigMap.hpp
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

#pragma once

#include "castor/tape/utils/DriveConfig.hpp"
#include "castor/tape/utils/TpconfigLine.hpp"
#include "castor/tape/utils/TpconfigLines.hpp"

#include <map>
#include <string>

namespace castor {
namespace tape {
namespace utils {

/**
 * Datatype representing a map from the unit name of a tape drive to its
 * configuration as specified in /etc/castor/TPCONFIG.
 */
class DriveConfigMap: public std::map<std::string, DriveConfig> {
public:

  /**
   * Enters the specified list of parsed lines from the TPCONFIG file into
   * the map of drive configurations.
   */
  void enterTpconfigLines(const TpconfigLines &lines);

  /**
   * Enters the specified parsed line from the TPCONFIG file into the map
   * of drive configurations.
   */
  void enterTpconfigLine(const TpconfigLine &line);

private:

  /**
   * Throws an exception if the specified TPCONFIG line is invalid when
   * compared to the specified current tape-drive configuration.
   *
   * @param drive The current configuration of the tape drive.
   * @param line The line parsed from /etc/castor/TPCONFIG.
   */
  void checkTpconfigLine(const DriveConfig &drive, const TpconfigLine &line);

  /**
   * Throws an exception if the specified TPCONFIG line is invalid when
   * compared to the specified current tape-drive configuration.
   *
   * @param drive The current configuration of the tape drive.
   * @param line The line parsed from /etc/castor/TPCONFIG.
   */
  void checkTpconfigLineDgn(const DriveConfig &drive,
    const TpconfigLine &line);

  /**
   * Throws an exception if the specified TPCONFIG line is invalid when
   * compared to the specified current tape-drive configuration.
   *
   * @param drive The current configuration of the tape drive.
   * @param line The line parsed from /etc/castor/TPCONFIG.
   */
  void checkTpconfigLineDevFilename(const DriveConfig &drive,
    const utils::TpconfigLine &line);

  /**
   * Throws an exception if the specified TPCONFIG line is invalid when
   * compared to the specified current tape-drive configuration.
   *
   * @param drive The current configuration of the tape drive.
   * @param line The line parsed from /etc/castor/TPCONFIG.
   */
  void checkTpconfigLineDensity(const DriveConfig &drive,
    const utils::TpconfigLine &line);

  /**
   * Throws an exception if the specified TPCONFIG line is invalid when
   * compared to the specified current tape-drive configuration.
   *
   * @param drive The current configuration of the tape drive.
   * @param line The line parsed from /etc/castor/TPCONFIG.
   */
  void checkTpconfigLineLibrarySlot(const DriveConfig &drive,
    const utils::TpconfigLine &line);

  /**
   * Throws an exception if the specified TPCONFIG line is invalid when
   * compared to the specified current tape-drive configuration.
   *
   * @param drive The current configuration of the tape drive.
   * @param line The line parsed from /etc/castor/TPCONFIG.
   */
  void checkTpconfigLineDevType(const DriveConfig &drive,
    const utils::TpconfigLine &line);
}; // class DriveConfigMap

} // namespace utils
} // namespace tape
} // namespace castor
