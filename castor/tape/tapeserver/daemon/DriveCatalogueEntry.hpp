/******************************************************************************
 *         castor/tape/tapeserver/daemon/DriveCatalogueEntry.hpp
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
#include "castor/legacymsg/RtcpJobRqstMsgBody.hpp"
#include "castor/legacymsg/TapeLabelRqstMsgBody.hpp"
#include "castor/legacymsg/TapeUpdateDriveRqstMsgBody.hpp"

#include <string>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

namespace castor     {
namespace tape       {
namespace tapeserver {
namespace daemon     {

/**
 * Structure used to store a tape drive in the catalogue.
 */
struct DriveCatalogueEntry {

  /**
   * The state of a drive as described by the following FSTN:
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
   *                                |  |             ^                     |
   *              -----------------    |             |                     |
   *             | label job           | vdqm job    |                     |
   *             |                     |             |                     |
   *             v                     v             |                     |
   *        -----------        ------------------    | SIGCHLD             |
   *       | WAITLABEL |      |    WAITFORK      |   | [success]           |
   *        -----------        ------------------    |                     |
   *             |                  |                |                     |
   *             |                  |                |                     |
   *             | forked           | forked         |                     |
   *             |                  |                |                     |
   *             |                  v                |                     |
   *             |                  ------------------    SIGCHLD [fail]   |
   *              ---------------->|     RUNNING      |--------------------|
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
   * The tape daemon can receive a job from the vdqmd daemon for a tape drive
   * when the state of that drive is DRIVE_STATE_UP.  On reception of the job
   * the daemon prepares to fork a child process and enters the
   * DRIVE_STATE_WAITFORK state.
   *
   * The DRIVE_STATE_WAIT_FORK state allows the object responsible for handling
   * the connection from the vdqmd daemon (an instance of
   * VdqmConnectionHandler) to delegate the task of forking of a mount session.
   *
   * Once the child process is forked the drive enters the DRIVE_STATE_RUNNING
   * state.  The child process is responsible for running a mount session.
   * During such a sesion a tape will be mounted, data will be transfered to
   * and/or from the tape and finally the tape will be dismounted.
   *
   * Once the vdqm job has been carried out, the child process completes
   * and the state of the tape drive either returns to DRIVE_STATE_UP if there
   * were no problems or to DRIVE_STATE_DOWN if there were.
   *
   * If the tape daemon receives a tpconfig down during a tape session, in
   * other words whilst the drive in question is in the DRIVE_STATE_RUNNING
   * state, then the state of the drive is moved to DRIVE_STATE_WAITDOWN.  The
   * tape session continues to run in the DRIVE_STATE_WAITDOWN state, however
   * when the tape session is finished the state of the drive is moved to
   * DRIVE_STATE_DOWN.
   *
   * The tape daemon can receive a job to label a tape in a drive from an
   * administration client when the state of that drive is DRIVE_STATE_UP.  On
   * reception of the job the daemon prepares to fork a child process and
   * enters the DRIVE_STATE_WAITLABEL state.
   *
   * Once the child process is forked the drive enters the DRIVE_STATE_RUNNING
   * state.  The child process is responsible for running a label session.
   * During such a sesion a tape will be mounted, the tape will be labeled and
   * finally the tape will be dismounted.
   */
  enum DriveState {
    DRIVE_STATE_INIT,
    DRIVE_STATE_DOWN,
    DRIVE_STATE_UP,
    DRIVE_STATE_WAITFORK,
    DRIVE_STATE_WAITLABEL,
    DRIVE_STATE_RUNNING,
    DRIVE_STATE_WAITDOWN};

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
  static const char *drvState2Str(const DriveState state) throw();

  /**
   * Enumeration of the possible types of session.
   */
  enum SessionType {
    SESSION_TYPE_NONE,
    SESSION_TYPE_DATATRANSFER,
    SESSION_TYPE_LABEL};

  /**
   * Always returns a string representation of the specified session type.
   * If the session type is unknown then an appropriately worded string
   * representation is returned and no exception is thrown.
   *
   * @param sessionType The numerical sessionType.
   * @return The string representation if known else "UNKNOWN".
   */
  static const char *sessionType2Str(const SessionType sessionType) throw();

  /**
   * The configuration of the tape-drive.
   */
  castor::tape::utils::DriveConfig config;
  
  /**
   * Are we mounting for read, write (read/write), or dump
   */
  castor::legacymsg::TapeUpdateDriveRqstMsgBody::TapeMode mode;
  
  /**
   * The status of the tape with respect to the drive mount and unmount operations
   */
  castor::legacymsg::TapeUpdateDriveRqstMsgBody::TapeEvent event;
  
  /**
   * The Volume ID of the tape mounted in the drive. Empty string if drive is
   * empty.
   */
  std::string vid;
  
  /**
   * The point in time when the drive has been assigned a tape
   */
  time_t assignment_time;

  /**
   * The current state of the tape drive.
   */
  DriveState state;
  
  /**
   * The type of mount session.
   */
  SessionType sessionType;

  /**
   * The job received from the vdqmd daemon when the drive is in the
   * DRIVE_STATE_RUNNING state.  In all other states the value of this
   * member variable is undefined.
   */
  legacymsg::RtcpJobRqstMsgBody vdqmJob;
  
  /**
   * The label job received from the tape label command when the drive is in the
   * DRIVE_STATE_RUNNING state.  In all other states the value of this
   * member variable is undefined.
   */
  legacymsg::TapeLabelRqstMsgBody labelJob;
  
  /**
   * The process ID of the child process running the mount session.
   */
  pid_t sessionPid;

  /**
   * If the drive state is either DRIVE_WAITLABEL, DRIVE_STATE_RUNNING or
   * DRIVE_STATE_WAITDOWN and the type of the session is SESSION_TYPE_LABEL
   * then this is the file descriptor of the TCP/IP connection with the tape
   * labeling command-line tool castor-tape-label.  In any other state, the
   * value of this filed is undefined.
   */
  int labelCmdConnection;

  /**
   * Default constructor that initializes all strings to the empty string,
   * all integers to zero, all file descriptors to -1, all lists to empty,
   * the drive state to DRIVE_STATE_INIT and the sessionType to
   * SESSION_TYPE_NONE.  This initialization includes the individual member
   * variables of the nested vdqm job.
   */
  DriveCatalogueEntry() throw();

}; // class DriveCatalogueEntry

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor

