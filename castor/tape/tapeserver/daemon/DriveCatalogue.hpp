/******************************************************************************
 *         castor/tape/tapeserver/daemon/DriveCatalogue.hpp
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

#include "castor/exception/Exception.hpp"
#include "castor/legacymsg/RtcpJobRqstMsgBody.hpp"
#include "castor/legacymsg/TapeLabelRqstMsgBody.hpp"
#include "castor/tape/utils/TpconfigLine.hpp"
#include "castor/tape/utils/TpconfigLines.hpp"
#include "castor/tape/tapeserver/client/ClientProxy.hpp"
#include "castor/legacymsg/TapeUpdateDriveRqstMsgBody.hpp"

#include <map>
#include <string>
#include <string.h>

namespace castor     {
namespace tape       {
namespace tapeserver {
namespace daemon     {

/**
 * Class responsible for keeping track of the tape drive being controlled by
 * the tapeserverd daemon.
 */
class DriveCatalogue {
public:

  /**
   * Destructor.
   *
   * Closes the connection with the label command if the drive catalogue owns
   * the connection at the time of destruction.
   */
  ~DriveCatalogue() throw();

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
   * Poplates the catalogue using the specified parsed lines from
   * /etc/castor/TPCONFIG.
   *
   * @param lines The lines parsed from /etc/castor/TPCONFIG.
   */
  void populateCatalogue(const utils::TpconfigLines &lines);

  /**
   * Returns the unit name of the tape drive on which the specified mount
   * session process is running.
   *
   * @param sessionPid The process ID of the mount session.
   * @return the unit name of the tape drive.
   */
  std::string getUnitName(const pid_t sessionPid) const ;

  /**
   * Returns an unordered list of the unit names of all of the tape drives
   * stored within the tape drive catalogue.
   *
   * @return Unordered list of the unit names.
   */
  std::list<std::string> getUnitNames() const;

  /**
   * Returns an unordered list of the unit names of the tape drives in the
   * specified state.
   *
   * @return Unordered list of the unit names.
   */
  std::list<std::string> getUnitNames(const DriveState state) const;

  /**
   * Returns the device group name (DGN) of the specified tape drive.
   *
   * @param unitName The unit name of the tape drive.
   */
  const std::string &getDgn(const std::string &unitName) const;
  
  /**
   * Returns the VID of the tape mounted on the specified tape drive.
   *
   * @param unitName The unit name of the tape drive.
   */
  const std::string &getVid(const std::string &unitName) const;
  
  /**
   * Returns the time when the tape was mounted on the specified tape drive.
   *
   * @param unitName The unit name of the tape drive.
   */
  time_t getAssignmentTime(const std::string &unitName) const;

  /**
   * Returns the filename of the device file of the specified tape drive.
   *
   * @param unitName The unit name of the tape drive.
   */
  const std::string &getDevFilename(const std::string &unitName) const;

  /**
   * Returns the tape densities supported by the specified tape drive.
   *
   * @param unitName The unit name of the tape drive.
   */
  const std::list<std::string> &getDensities(const std::string &unitName) const;

  /**
   * Returns the type of the specified session.
   *
   * @param sessionPid The process ID of the session.
   */
  SessionType getSessionType(const pid_t sessionPid) const;

  /**
   * Returns the current state of the specified tape drive.
   *
   * @param unitName The unit name of the tape drive.
   */
  DriveState getState(const std::string &unitName) const;

  /**
   * Returns the library slot of the specified tape drive.
   *
   * @param unitName The unit name of the tape drive.
   */
  const std::string &getLibrarySlot(const std::string &unitName) const;

  /**
   * Returns the device type of the specified tape drive in its libary.
   *
   * @param unitName The unit name of the tape drive.
   */
  const std::string &getDevType(const std::string &unitName) const;
  
  /**
   * Returns the last tape mode of the tape mounted on this drive
   *
   * @param unitName The unit name of the tape drive.
   */
  castor::legacymsg::TapeUpdateDriveRqstMsgBody::TapeMode getTapeMode(const std::string &unitName) const;
  
  /**
   * Returns the last tape event related to this drive
   *
   * @param unitName The unit name of the tape drive.
   */
  castor::legacymsg::TapeUpdateDriveRqstMsgBody::TapeEvent getTapeEvent(const std::string &unitName) const;

  /**
   * Releases and returns the file descriptor of the connection with the
   * command-line tool castor-tape-label.
   *
   * @param unitName The unit name of the tape drive.
   */

  int releaseLabelCmdConnection(const std::string &unitName);

  /**
   * Moves the state of the specified tape drive to DRIVE_STATE_UP.
   *
   * This method throws an exception if the current state of the tape drive is
   * not DRIVE_STATE_UP, DRIVE_STATE_DOWN or DRIVE_STATE_WAITDOWN.
   *
   * configureUp() is idempotent.
   *
   * @param unitName The unit name of the tape drive.
   */
  void configureUp(const std::string &unitName) ;

  /**
   * Moves the state of the specified tape drive to DRIVE_STATE_DOWN.
   *
   * This method throws an exception if the current state of the tape drive is
   * not DRIVE_STATE_UP, DRIVE_STATE_DOWN or DRIVE_STATE_RUNNING.
   *
   * configureDown() is idempotent.
   *
   * @param unitName The unit name of the tape drive.
   */
  void configureDown(const std::string &unitName) ;

  /**
   * Moves the state of the specified tape drive to DRIVE_STATE_WAITFORK.
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
  void receivedVdqmJob(const legacymsg::RtcpJobRqstMsgBody &job)
    ;

  /**
   * Moves the state of the specified tape drive to DRIVE_STATE_WAITLABEL.
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
    const int labelCmdConnection) ;

  /**
   * Returns the job received from the vdqmd daemon for the specified tape
   * drive.
   *
   * This method throws an exception if the current state of the tape drive is
   * not DRIVE_STATE_WAITFORK, DRIVE_STATE_RUNNING or DRIVE_STATE_WAITDOWN.
   *
   * @param unitName The unit name of the tape drive.
   * @return The job received from the vdqmd daemon.
   */
  const legacymsg::RtcpJobRqstMsgBody &getVdqmJob(const std::string &unitName)
    const ;
  
  /**
   * Returns the job received from the vdqmd daemon for the specified tape
   * drive.
   *
   * This method throws an exception if the current state of the tape drive is
   * not DRIVE_STATE_WAITFORK, DRIVE_STATE_RUNNING or DRIVE_STATE_WAITDOWN.
   *
   * @param unitName The unit name of the tape drive.
   * @return The job received from the vdqmd daemon.
   */
  const legacymsg::TapeLabelRqstMsgBody &getLabelJob(const std::string &unitName)
    const ;

  /**
   * Moves the state of the specified tape drive to DRIVE_STATE_RUNNING.
   *
   * This method throws an exception if the current state of the tape drive is
   * not DRIVE_STATE_WAITFORK.
   *
   * @param unitName The unit name of the tape drive.
   * @param sessionPid The process ID of the child process responsible for
   * running the mount session.
   */
  void forkedMountSession(const std::string &unitName, const pid_t sessionPid)
    ; 

  /**
   * Moves the state of the specified tape drive to DRIVE_STATE_RUNNING.
   *
   * This method throws an exception if the current state of the tape drive is
   * not DRIVE_STATE_WAITLABEL.
   *
   * @param unitName The unit name of the tape drive.
   * @param sessionPid The process ID of the child process responsible for
   * running the label session.
   */
  void forkedLabelSession(const std::string &unitName, const pid_t sessionPid)
    ;

  /**
   * Returns the process ID of the child process responsible the mount session
   * running on the specified tape drive.
   *
   * This method throws an exception if the current state of the tape drive is
   * not DRIVE_STATE_RUNNING or DRIVE_STATE_WAITDOWN.
   *
   * @param unitName The unit name of the tape drive.
   * @return The process ID of the child process responsible for mount session
   * running on the specified tape drive.
   */
  pid_t getSessionPid(const std::string &unitName) const ;

  /**
   * Moves the state of the specified tape drive to DRIVE_STATE_UP if the
   * current state is DRIVE_STATE_RUNNING or to DRIVE_STATE_DOWN if the
   * current state is DRIVE_STATE_WAIT_DOWN.
   *
   * This method throws an exception if the current state of the tape drive is
   * not DRIVE_STATE_RUNNING or DRIVE_STATE_WAITDOWN.
   *
   * @param sessionPid Process ID of the child process handling the session.
   */
  void sessionSucceeded(const pid_t sessionPid) ;

  /**
   * Moves the state of the specified tape drive to DRIVE_STATE_UP if the
   * current state is DRIVE_STATE_RUNNING or to DRIVE_STATE_DOWN if the
   * current state is DRIVE_STATE_WAIT_DOWN.
   *
   * This method throws an exception if the current state of the tape drive is
   * not DRIVE_STATE_RUNNING or DRIVE_STATE_WAITDOWN.
   *
   * @param unitName The unit name of the tape drive.
   */
  void sessionSucceeded(const std::string &unitName) ;
  
  /**
   * Moves the state of the specified tape drive to DRIVE_STATE_DOWN.
   *
   * This method throws an exception if the current state of the tape drive is
   * not DRIVE_STATE_RUNNING.
   *
   * @param sessionPid Process ID of the child process handling the session.
   */
  void sessionFailed(const pid_t sessionPid) ;

  /**
   * Moves the state of the specified tape drive to DRIVE_STATE_DOWN.
   *
   * This method throws an exception if the current state of the tape drive is
   * not DRIVE_STATE_RUNNING.
   *
   * @param unitName The unit name of the tape drive.
   */
  void sessionFailed(const std::string &unitName) ;
  
  /**
   * Updates the vid and assignment time of the specified drive
   * 
   * @param vid Volume ID of the tape mounted
   * @param unitName Name of the drive
   */
  void updateDriveVolumeInfo(const legacymsg::TapeUpdateDriveRqstMsgBody &body) ;

private:

  /**
   * Structure used to store a tape drive in the catalogue.
   */
  struct DriveEntry {
    
    /**
     * Are we mounting for read, write (read/write), or dump
     */
    castor::legacymsg::TapeUpdateDriveRqstMsgBody::TapeMode mode;
    
    /**
     * The status of the tape with respect to the drive mount and unmount operations
     */
    castor::legacymsg::TapeUpdateDriveRqstMsgBody::TapeEvent event;
    
    /**
     * The device group name of the tape drive as defined in
     * /etc/castor/TPCONFIG.
     */
    std::string dgn;
    
    /**
     * The Volume ID of the tape mounted in the drive. Empty string if drive is empty.
     */
    std::string vid;
    
    /**
     * The point in time when the drive has been assigned a tape
     */
    time_t assignment_time;
    
    /**
     * The device file of the tape drive, for example: /dev/nst0
     */
    std::string devFilename;

    /**
     * The tape densities supported by the tape drive.
     */
    std::list<std::string> densities;

    /**
     * The type of mount session.
     */
    SessionType sessionType;

    /**
     * The current state of the tape drive.
     */
    DriveState state;

    /**
     * The library slot n which the tape drive is located, for example:
     * smc@localhost,0
     */
    std::string librarySlot;

    /**
     * The device type of the tape drive, for example: T10000
     */
    std::string devType;

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
    DriveEntry() throw():
      mode(castor::legacymsg::TapeUpdateDriveRqstMsgBody::TAPE_MODE_NONE),
      event(castor::legacymsg::TapeUpdateDriveRqstMsgBody::TAPE_STATUS_NONE),
      sessionType(SESSION_TYPE_NONE),
      state(DRIVE_STATE_INIT),
      labelCmdConnection(-1) {
      labelJob.gid = 0;
      labelJob.uid = 0;
      memset(labelJob.vid, '\0', sizeof(labelJob.vid));
      vdqmJob.volReqId = 0;
      vdqmJob.clientPort = 0;
      vdqmJob.clientEuid = 0;
      vdqmJob.clientEgid = 0;
      memset(vdqmJob.clientHost, '\0', sizeof(vdqmJob.clientHost));
      memset(vdqmJob.dgn, '\0', sizeof(vdqmJob.dgn));
      memset(vdqmJob.driveUnit, '\0', sizeof(vdqmJob.driveUnit));
      memset(vdqmJob.clientUserName, '\0', sizeof(vdqmJob.clientUserName));
    }
  };

  /**
   * Type that maps the unit name of a tape drive to the catalogue entry of
   * that drive.
   */
  typedef std::map<std::string, DriveEntry> DriveMap;

  /**
   * Map from the unit name of a tape drive to the catalogue entry of that
   * drive.
   */
  DriveMap m_drives;

  /** 
   * Enters the specified parsed line from /etc/castor/TPCONFIG into the
   * catalogue.
   *
   * @param line The line parsed from /etc/castor/TPCONFIG.
   */
  void enterTpconfigLine(const utils::TpconfigLine &line) ;

  /**
   * Checks the semantics of the specified TPCONFIG line against the specified
   * current catalogue entry.
   *
   * @param catalogueEntry The catalogue entry.
   * @param line The line parsed from /etc/castor/TPCONFIG.
   */
  void checkTpconfigLine(const DriveEntry &catalogueEntry, const utils::TpconfigLine &line) ;

  /**
   * Throws an exception if the specified catalogue value does not match the
   * specified TPCONFIG line value.
   *
   * @param catalogueDgn The DGN of the tape drive that has been retrieved from
   * the tape-drive catalogue.
   * @param line The line parsed from /etc/castor/TPCONFIG.
   */
  void checkTpconfigLineDgn(const std::string &catalogueDgn, const utils::TpconfigLine &line) ;

  /**
   * Throws an exception if the specified catalogue value does not match the
   * specified TPCONFIG line value.
   *
   * @param catalogueDevFilename The filename of the device file of the tape
   * drive that has been retrieved from the tape-drive catalogue.
   * @param line The line parsed from /etc/castor/TPCONFIG.
   */
  void checkTpconfigLineDevFilename(const std::string &catalogueDevFilename, const utils::TpconfigLine &line) ;

  /**
   * Throws an exception if the specified catalogue value does not match the
   * specified TPCONFIG line value.
   *
   * @param catalogueDensities The densities supported by the tape drive that
   * have been retrived from the tape-drive catalogue.
   * @param line The line parsed from /etc/castor/TPCONFIG.
   */
  void checkTpconfigLineDensity(const std::list<std::string> &catalogueDensities, const utils::TpconfigLine &line) ;

  /**
   * Throws an exception if the specified catalogue value does not match the
   * specified TPCONFIG line value.
   *
   * @param catalogueInitialState The initial state of the tape drive that
   * has been retrieved from the tape-drive catalogue.
   * @param line The line parsed from /etc/castor/TPCONFIG.
   */
  void checkTpconfigLineInitialState(const DriveState catalogueInitialState, const utils::TpconfigLine &line) ;

  /**
   * Throws an exception if the specified catalogue value does not match the
   * specified TPCONFIG line value.
   *
   * @param catalogueLibrarySlot The library slot of the tape drive that has
   * been retrieved from the tape-drive catalogue.
   * @param line The line parsed from /etc/castor/TPCONFIG.
   */
  void checkTpconfigLineLibrarySlot(const std::string &catalogueLibrarySlot, const utils::TpconfigLine &line) ;

  /**
   * Throws an exception if the specified catalogue value does not match the
   * specified TPCONFIG line value.
   *
   * @param catalogueDevType The device type of the tape drive that has been
   * retrieved from the tape-drive library.
   * @param line The line parsed from /etc/castor/TPCONFIG.
   */
  void checkTpconfigLineDevType(const std::string &catalogueDevType, const utils::TpconfigLine &line) ;

}; // class DriveCatalogue

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor

