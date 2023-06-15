/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
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

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {

/**
 * Abstract class responsible for defining the interface to a tapeserverd
 * child-process session.
 */
class Session {
public:
  /**
   * Destructor.
   */
  virtual ~Session() = 0;

  /**
   * Enumeration of actions that can be performed after a session has been
   * completed.
   *
   * Please note that this enumeration will be used to set the integer
   * termination/return values of child-process sessions.
   */
  enum EndOfSessionAction {
    MARK_DRIVE_AS_UP = 0,
    MARK_DRIVE_AS_DOWN = 1,
    CLEAN_DRIVE = 2  // Unload and dismount tape if present
  };

  /**
   * Execute the session and return the type of action to be performed
   * immediately after the session has completed.
   *
   * The session is responsible for mounting a tape into the tape drive, working
   * with that tape, unloading the tape from the drive and then dismounting the
   * tape from the drive and storing it back in its home slot within the tape
   * library.
   *
   * If this method throws an exception and the session is not a cleaner
   * session then it assumed that the post session action is
   * EndOfSessionAction::CLEAN_DRIVE.
   *
   * If this method throws an exception and the session is a cleaner
   * session then it assumed that the post session action is
   * EndOfSessionAction::MARK_DRIVE_AS_DOWN.
   *
   * @return Returns the type of action to be performed after the session has
   * completed.
   */
  virtual EndOfSessionAction execute() = 0;

};  // class Session

}  // namespace daemon
}  // namespace tapeserver
}  // namespace tape
}  // namespace castor
