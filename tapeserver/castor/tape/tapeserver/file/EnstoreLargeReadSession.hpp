/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
class EnstoreLargeReadSession : public ReadSession {
public:
  /**
    * Constructor of the EnstoreReadSession. It will rewind the tape, and check the
    * VID value. Throws an exception in case of mismatch.
    * @param drive: drive object to which we bind the session
    * @param volInfo: volume name of the tape we would like to read from
    * @param useLbp: castor.conf option to use or not to use LBP in tapeserverd
    */
  EnstoreLargeReadSession(tapeserver::drive::DriveInterface& drive,
                          const tapeserver::daemon::VolumeInfo& volInfo,
                          const bool useLbp);

  ~EnstoreLargeReadSession() override = default;
};

}  // namespace castor::tape::tapeFile
