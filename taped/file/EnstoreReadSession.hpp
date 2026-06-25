/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "ReadSession.hpp"
#include "taped/session/VolumeInfo.hpp"

#include <memory>
#include <string>

namespace cta::tape::tapeFile {

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
    * @param useLbp: option to enable Logical Block Protection in taped
    */
  EnstoreReadSession(drive::DriveInterface& drive, const daemon::VolumeInfo& volInfo, const bool useLbp);

  ~EnstoreReadSession() override = default;
};

}  // namespace cta::tape::tapeFile
