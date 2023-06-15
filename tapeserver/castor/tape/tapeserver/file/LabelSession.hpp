/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include <string>

namespace castor {
namespace tape {

namespace tapeserver {
namespace drive {
class DriveInterface;
}
}  // namespace tapeserver

namespace tapeFile {

/**
  * Class that labels a tape.  This class assumes the tape to be labeled is
  * already mounted and rewound.
  */
class LabelSession {
public:
  /**
    * Constructor of the LabelSession. This constructor will label the
    * tape in the specified drive.  This constructor assumes the tape to be
    * labeled is already mounted and rewound.
    * @param drive: drive object to which we bind the session
    * @param vid: volume name of the tape we would like to read from
    * @param lbp The boolean variable for logical block protection mode.
    *            If it is true than the label will be written with LBP or
    *            without otherwise.
    *
    */
  static void label(tapeserver::drive::DriveInterface* drive, const std::string& vid, const bool lbp);
};

}  // namespace tapeFile
}  // namespace tape
}  // namespace castor
