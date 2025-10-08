/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

namespace castor::tape::tapeserver::daemon {

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
    MARK_DRIVE_AS_UP   = 0,
    MARK_DRIVE_AS_DOWN = 1,
    CLEAN_DRIVE        = 2 // Unload and dismount tape if present
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

}; // class Session

} // namespace castor::tape::tapeserver::daemon
