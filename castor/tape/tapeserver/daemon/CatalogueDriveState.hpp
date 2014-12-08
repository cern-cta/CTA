/******************************************************************************
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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

namespace castor     {
namespace tape       {
namespace tapeserver {
namespace daemon     {

/**
 * Enumeration of the possible states of a catalogue drive.  The following FSTN
 * describes the states in detail.
 *
 *              start daemon /
 *  ------  send VDQM_UNIT_DOWN   ------------------
 * | INIT |--------------------->|       DOWN       |<-------------------
 *  ------                        ------------------                     |
 *     |                          |                ^                     |
 *     |                          |                |                     |
 *     |                          | tpconfig up    | tpconfig down       |
 *     |                          |                |                     |
 *     |      start daemon /      v                |                     |
 *     |    send VDQM_UNIT_UP     ------------------                     |
 *      ------------------------>|       UP         |                    |
 *                                ------------------                     |
 *                                |                ^                     |
 *                                |                |                     |
 *                                | received job   | SIGCHLD             |
 *                                |                | [success]           |
 *                                |                |                     |
 *                                v                |                     |
 *                                ------------------    SIGCHLD [fail]   |
 *                               |     RUNNING      |--------------------|
 *                                ------------------                     |
 *                                |                ^                     |
 *                                |                |                     |
 *                                | tpconfig down  | tpconfig up         |
 *                                |                |                     |
 *                                |                |                     |
 *                                v                |  SIGCHLD            |
 *                                ------------------  [success of fail]  |
 *                               |     WAITDOWN     |--------------------
 *                                ------------------
 *
 * When the tapeserverd daemon is started, depending on the initial state
 * defined in /etc/castor/TPCONFIG, the daemon sends either a VDQM_UNIT_UP
 * or VDQM_UNIT_DOWN status message to the vdqmd daemon.  The state of the
 * tape drive is then either DRIVE_STATE_UP or DRIVE_STATE_DOWN respectively.
 *
 * A tape operator toggles the state of tape drive between DOWN and UP
 * using the tpconfig adminstration tool.
 *
 * The tape daemon can receive a job the vdqmd daemon for a tape drive
 * when the state of that drive is DRIVE_STATE_UP.  On reception of the job
 * the daemon has the ProcessForker fork a child process.
 *
 * Once the child process is forked the drive enters the DRIVE_STATE_RUNNING
 * state.  The child process is responsible for running a data-transfer
 * session.  During such a sesion a tape will be mounted, data will be
 * transfered to and/or from the tape and finally the tape will be dismounted.
 *
 * Once the vdqm job has been carried out, the child process completes
 * and the state of the tape drive either returns to DRIVE_STATE_UP if the
 * associated drive is free or to DRIVE_STATE_DOWN if it could not be freed.
 *
 * If the tape daemon receives a tpconfig-down message during a tape session,
 * in other words whilst the drive in question is in the DRIVE_STATE_RUNNING
 * state, then the state of the drive is moved to DRIVE_STATE_WAITDOWN.  The
 * tape session continues to run in the DRIVE_STATE_WAITDOWN state, however
 * when the tape session is finished the state of the drive is moved to
 * DRIVE_STATE_DOWN.
 *
 * The tape daemon can also receive a label job from the tape labeling
 * command-line tool.  The tape daemon behaves as described above except a
 * label session is forked instead.
 *
 * The following FSTN showns the shutdown logic.
 *
 *  ------------------  shutdown
 * |       DOWN       |---------------------------------------->
 *  ------------------                                          |
 *                                                              |
 *                                                              |
 *  ------------------  shutdown                                |
 * |       UP         |---------------------------------------->|
 *  ------------------                                          |
 *                                                              |
 *                                                              |
 *  ------------------  shutdown / kill session                 |
 * |     RUNNING      |------------------------->               |
 *  ------------------                           |              |
 *                                               |              |
 *                                               |              |
 *  ------------------  shutdown / kill session  |              |
 * |     WAITDOWN     |------------------------->|              |
 *  ------------------                           |              |
 *                                               v              |
 *                                      -------------------     |
 *                                     | WAITSHUTDOWNKILL  |    |
 *                                      -------------------     |
 *                                               |              |
 *                  session killed / run cleaner |              |
 *                                               |              |
 *                                     ---------------------    |
 *                                    | WAITSHUTDOWNCLEANER |   |
 *                                     ---------------------    |
 *                                               |              |
 *                              cleaner finished |              |
 *                                               |              |
 *                                               v              v
 *                                               ----------------
 *                                              |    SHUTDOWN    |
 *                                               ----------------
 */
enum CatalogueDriveState {
  DRIVE_STATE_INIT,
  DRIVE_STATE_DOWN,
  DRIVE_STATE_UP,
  DRIVE_STATE_RUNNING,
  DRIVE_STATE_WAITDOWN,
  DRIVE_STATE_WAITSHUTDOWNKILL,
  DRIVE_STATE_WAITSHUTDOWNCLEANER,
  DRIVE_STATE_SHUTDOWN};

/**
 * Returns the string representation of the specified tape-drive state.
 *
 * Please note that this method does not throw an exception and to that end
 * will return the string "UNKNOWN" if the string representation of the
 * specified drive state is unknown.
 *
 * @param state The numerical tape-drive state.
 * @return The string representation if known else "UNKNOWN".
 */
const char *catalogueDriveStateToStr(const CatalogueDriveState state)
  throw();

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor
