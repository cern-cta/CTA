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
#include "castor/legacymsg/TapeStatDriveEntry.hpp"
#include "castor/legacymsg/TapeUpdateDriveRqstMsgBody.hpp"
#include "castor/messages/NotifyDrive.pb.h"
#include "castor/tape/tapeserver/daemon/DriveCatalogueSession.hpp"

#include <string>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <memory>
#include <iostream>

namespace castor     {
namespace tape       {
namespace tapeserver {
namespace daemon     {

/**
 * A tape-drive entry in the tape-drive catalogue.
 */
class DriveCatalogueEntry {
public:
  
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
   *      ---------------      ------------------    | SIGCHLD             |
   *     | WAITFORKLABEL |    | WAITFORKTRANSFER |   | [success]           |
   *      ---------------      ------------------    |                     |
   *             |                     |             |                     |
   *             |                     |             |                     |
   *             | forked              | forked      |                     |
   *             |                     |             |                     |
   *             |                     v             |                     |
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
   * DRIVE_STATE_WAITFORKTRANSFER state.
   *
   * The DRIVE_STATE_WAITFORKTRANSFER state allows the object responsible for
   * handling the connection from the vdqmd daemon (an instance of
   * VdqmConnectionHandler) to delegate the task of forking a mount session.
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
    DRIVE_STATE_SESSIONRUNNING,
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
   * Default constructor that initializes all strings to the empty string,
   * all integers to zero, all file descriptors to -1, all lists to empty,
   * the drive state to DRIVE_STATE_INIT and the sessionType to
   * SESSION_TYPE_NONE.
   */
  DriveCatalogueEntry() throw();
  
  /**
   * Destructor
   */
  ~DriveCatalogueEntry() throw();

  /**
   * Constructor that except for its parameters, initializes all strings to
   * the empty string, all integers to zero, all file descriptors to -1,
   * all lists to the emptylist, and the sessionType to SESSION_TYPE_NONE.
   *
   * @param config The configuration of the tape drive.
   * @param state The initial state of the tape drive.
   */
  DriveCatalogueEntry(const utils::DriveConfig &config,
    const DriveState state) throw();

  /**
   * Sets the configuration of the tape-drive.
   */
  void setConfig(const tape::utils::DriveConfig &config) throw();

  /**
   * Gets the configuration of the tape-drive.
   *
   * @return The configuration of the tape-drive.
   */
  const tape::utils::DriveConfig &getConfig() const;

  /**
   * Sets the Volume ID of the tape mounted in the drive. Empty string if drive
   * is empty.
   */
  void setVid(const std::string &vid);

  /**
   * Gets the Volume ID of the tape mounted in the drive. Empty string if drive
   * is empty.
   *
   * @return The Volume ID or an empty string if teh drive is empty.
   */
  std::string getVid() const;

  /**
   * Sets the point in time when the drive was assigned a tape.
   */
  void setAssignmentTime(const time_t assignmentTime);

  /**
   * Gets the point in time when the drive was assigned a tape.
   *
   * @return Te point in time when the drive was assigned a tape.
   */
  time_t getAssignmentTime() const;

  /**
   * Gets the current state of the tape drive.
   *
   * @return The current state of the tape drive.
   */
  DriveState getState() const throw();
  
  /**
   * Gets the current state of the tape drive session.
   *
   * @return The current state of the tape drive session.
   */
  castor::tape::tapeserver::daemon::DriveCatalogueSession::SessionState getSessionState() const throw();

  /**
   * Gets the type of session associated with the tape drive.
   */
  SessionType getSessionType() const throw();

  /**
   * Configures the tape-drive up.
   *
   * This method throws an exception if the current state of the tape drive is
   * not DRIVE_STATE_UP, DRIVE_STATE_DOWN or DRIVE_STATE_WAITDOWN.
   *
   * configureUp() is idempotent.
   */
  void configureUp();

  /**
   * Configures the tape drive down.
   *
   * This method throws an exception if the current state of the tape drive is
   * not DRIVE_STATE_UP, DRIVE_STATE_DOWN or DRIVE_STATE_RUNNING.
   *
   * configureDown() is idempotent.
   */
  void configureDown();

  /**
   * Moves the state of tape drive to DRIVE_STATE_WAITFORK.
   *
   * This method throws an exception if the current state of the tape drive is
   * not DRIVE_STATE_UP. 
   *    
   * The unit name of a tape drive is unique for a given host.  No two drives
   * on the same host can have the same unit name.
   *
   * A tape drive cannot be a member of more than one device group name (DGN).
   *
   * This method throws an exception if the DGN field of the specified vdqm job
   * does not match the value that was entered into the catalogue with the
   * populateCatalogue() method.
   *  
   * @param job The job received from the vdqmd daemon.
   */   
  void receivedVdqmJob(const legacymsg::RtcpJobRqstMsgBody &job);
      
  /**
   * Moves the state of the tape drive to DRIVE_STATE_WAITLABEL.
   *
   * This method throws an exception if the current state of the tape drive is
   * not DRIVE_STATE_UP.
   *
   * The unit name of a tape drive is unique for a given host.  No two drives
   * on the same host can have the same unit name.
   *
   * A tape drive cannot be a member of more than one device group name (DGN).
   *
   * This method throws an exception if the DGN field of the specified vdqm job
   * does not match the value that was entered into the catalogue with the
   * populateCatalogue() method.
   *
   * PLEASE NOTE: If this method throws an exception then it does NOT close
   * the file descriptor of the TCP/IP connection with the tape labeling
   * command-line tool castor-tape-label.  The caller of this method is left
   * to close the connection because this gives them the opportunity to send
   * an approprioate error message to the client.
   *
   * @param job The label job.
   * @param labelCmdConnection The file descriptor of the TCP/IP connection
   * with the tape labeling command-line tool castor-tape-label.
   */
  void receivedLabelJob(const legacymsg::TapeLabelRqstMsgBody &job,
    const int labelCmdConnection);

  /**
   * Moves the state of the tape drive to DRIVE_STATE_RUNNING.
   *
   * This method throws an exception if the current state of the tape drive is
   * not DRIVE_STATE_WAITFORK.
   *
   * @param sessionPid The process ID of the child process responsible for
   * running the mount session.
   */
  void forkedMountSession(const pid_t sessionPid);
  
  /**
   * Moves the state of the tape drive to DRIVE_STATE_RUNNING.
   *
   * This method throws an exception if the current state of the tape drive is
   * not DRIVE_STATE_WAITLABEL.
   *
   * @param sessionPid The process ID of the child process responsible for
   * running the label session.
   */   
  void forkedLabelSession(const pid_t sessionPid);

  /**
   * Moves the state of the tape drive to DRIVE_STATE_UP if the
   * current state is DRIVE_STATE_RUNNING or to DRIVE_STATE_DOWN if the
   * current state is DRIVE_STATE_WAIT_DOWN.
   *  
   * This method throws an exception if the current state of the tape drive is
   * not DRIVE_STATE_RUNNING or DRIVE_STATE_WAITDOWN.
   */
  void sessionSucceeded(); 
      
  /**
   * Moves the state of tape drive to DRIVE_STATE_DOWN.
   *
   * This method throws an exception if the current state of the tape drive is
   * not DRIVE_STATE_RUNNING.
   */
  void sessionFailed();

  /**
   * Gets the job received from the vdqmd daemon.
   *
   * This method throws a castor::exception::Exception if the tape drive is not
   * in a state for which there is a vdqm job.
   *
   * @return The job received from the vdqmd daemon.
   */
  const legacymsg::RtcpJobRqstMsgBody getVdqmJob() const;

  /**
   * Gets the label job received from the castor-tape-label command-line tool.
   *
   * This method throws a castor::exception::Exception if the tape drive is not
   * in a state for which there is a label job.
   *
   * @return The label job received from the castor-tape-label command-line
   * tool.
   */ 
  const legacymsg::TapeLabelRqstMsgBody getLabelJob() const;

  /**
   * The process ID of the child process running the mount session.
   *
   * This method throws a castor::exception::Exception if the tape drive is not
   * in a state for which there is a session process-ID.
   *
   * @return The process ID of the child process running the mount session.
   */
  pid_t getSessionPid() const;

  /**
   * Gets the file descriptor of the TCP/IP connection with the
   * castor-tape-label command-line tool.
   *
   * This method throws a castor::exception::Exception if the tape drive is not
   * in a state for which there is a TCP/IP connection with the
   * castor-tape-label command-line tool..
   *
   * @return The file descriptor of the TCP/IP connection with the
   * castor-tape-label command-line tool.
   */
  int getLabelCmdConnection() const;

  /**
   * Releases and returns the TCP/IP connection with the
   * castor-tape-label command-line tool.
   *
   * This method throws a castor::exception::Exception if is invalid to call
   * this method.
   *
   * @return The file descriptor of the TCP/IP connection with the
   * castor-tape-label command-line tool.
   */
  int releaseLabelCmdConnection();


  //default template version for the nes messages
  template <class T> void updateVolumeInfo(const T &body) {
    m_vid = body.vid();
    m_getToBeMountedForTapeStatDriveEntry = false;
    m_mode = body.mode();
  }
  //overload that will be picked up when passing a NotifyDriveBeforeMountStarted
  void updateVolumeInfo(const castor::messages::NotifyDriveBeforeMountStarted &body);
  
  /**
   * Gets the tpstat representation of the tape drive.
   *
   * @return The tpstat representation of the tape drive.
   */
  legacymsg::TapeStatDriveEntry getTapeStatDriveEntry() const;

private:

  /**
   * Copy constructor and assignment operator are declared private since the member "m_session" is now a pointer
   * @param other
   */
  DriveCatalogueEntry( const DriveCatalogueEntry& other );  
  DriveCatalogueEntry& operator=( const DriveCatalogueEntry& other );

  /**
   * The configuration of the tape-drive.
   */
  castor::tape::utils::DriveConfig m_config;

  /**
   * Are we mounting for read, write (read/write), or dump
   */
  castor::messages::TapeMode m_mode;
  
  /**
   * The status of the tape with respect to the drive mount and unmount
   * operations.
   */
  bool m_getToBeMountedForTapeStatDriveEntry;
  
  /**
   * The Volume ID of the tape mounted in the drive. Empty string if drive is
   * empty.
   */
  std::string m_vid;
  
  /**
   * The point in time when the drive was assigned a tape.
   */
  time_t m_assignmentTime;

  /**
   * The current state of the tape drive.
   */
  DriveState m_state;
  
  /**
   * The type of mount session.
   */
  SessionType m_sessionType;

  /**
   * If the drive state is either DRIVE_WAITLABEL, DRIVE_STATE_RUNNING or
   * DRIVE_STATE_WAITDOWN and the type of the session is SESSION_TYPE_LABEL
   * then this is the file descriptor of the TCP/IP connection with the tape
   * labeling command-line tool castor-tape-label.  In any other state, the
   * value of this field is undefined.
   */
  int m_labelCmdConnection;
  
  /**
   * The session metadata associated to the drive catalogue entry
   */
  DriveCatalogueSession *m_session;
  
  /**
   * 
   * @return the associated drive catalogue session pointer
   */
  DriveCatalogueSession * getSession() const;

  /**
   * Returns the value of the uid field of a TapeStatDriveEntry to be used
   * in a TapeStatReplyMsgBody.
   */
  uint32_t getUidForTapeStatDriveEntry() const throw();

  /**
   * Returns the value of the jid field of a TapeStatDriveEntry to be used
   * in a TapeStatReplyMsgBody.
   */
  uint32_t getJidForTapeStatDriveEntry() const throw();

  /**
   * Returns the value of the up field of a TapeStatDriveEntry to be used
   * in a TapeStatReplyMsgBody.
   */
  uint16_t getUpForTapeStatDriveEntry() const throw();

  /**
   * Returns the value of the asn field of a TapeStatDriveEntry to be used
   * in a TapeStatReplyMsgBody.
   */
  uint16_t getAsnForTapeStatDriveEntry() const throw();

  /**
   * Returns the value of the asnTime field of a TapeStatDriveEntry to be used
   * in a TapeStatReplyMsgBody.
   */
  uint32_t getAsnTimeForTapeStatDriveEntry() const throw();

  /**
   * Returns the value of the mode field of a TapeStatDriveEntry to be used
   * in a TapeStatReplyMsgBody.
   */
  uint16_t getModeForTapeStatDriveEntry() const throw();

  /**
   * Returns the value of the lblCode field of a TapeStatDriveEntry to be used
   * in a TapeStatReplyMsgBody.
   */
  std::string getLblCodeForTapeStatDriveEntry() const throw();

  /**
   * Returns the value of the tobeMounted field of a TapeStatDriveEntry to be
   * used in a TapeStatReplyMsgBody.
   */
  uint16_t getToBeMountedForTapeStatDriveEntry() const throw();

  /**
   * Returns the value of the vid field of a TapeStatDriveEntry to be used
   * in a TapeStatReplyMsgBody.
   */
  std::string getVidForTapeStatDriveEntry() const throw();

  /**
   * Returns the value of the vsn field of a TapeStatDriveEntry to be used
   * in a TapeStatReplyMsgBody.
   */
  std::string getVsnForTapeStatDriveEntry() const throw();

}; // class DriveCatalogueEntry

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor
