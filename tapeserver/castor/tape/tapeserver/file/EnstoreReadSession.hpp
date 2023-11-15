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

#include <memory>
#include <string>

#include "castor/tape/tapeserver/daemon/VolumeInfo.hpp"
#include "castor/tape/tapeserver/file/ReadSession.hpp"

namespace castor::tape::tapeFile {

/**
  * Class keeping track of a whole tape read session over an Enstore formatted
  * tape. The session will keep track of the overall coherency of the session
  * and check for everything to be coherent. The tape should be mounted in
  * the drive before the EnstoreReadSession is started (i.e. constructed).
  * Likewise, tape unmount is the business of the user.
  */
class EnstoreReadSession : public ReadSession {
public:
  /**
    * Constructor of the EnstoreReadSession. It will rewind the tape, and check the
    * VID value. Throws an exception in case of mismatch.
    * @param drive: drive object to which we bind the session
    * @param volInfo: volume name of the tape we would like to read from
    * @param useLbp: castor.conf option to use or not to use LBP in tapeserverd
    */
  EnstoreReadSession(tapeserver::drive::DriveInterface &drive, const tapeserver::daemon::VolumeInfo &volInfo,
    const bool useLbp);

  ~EnstoreReadSession() override = default;
};

} // namespace castor::tape::tapeFile
