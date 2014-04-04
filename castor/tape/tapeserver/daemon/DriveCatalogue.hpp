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
#include "castor/tape/legacymsg/RtcpJobRqstMsgBody.hpp"
#include "castor/tape/utils/TpconfigLine.hpp"
#include "castor/tape/utils/TpconfigLines.hpp"

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
   *                                |                ^                     |
   *                                |                |                     |
   *                                | vdqm job /     | SIGCHLD [success]   |
   *                                | fork           |                     |
   *                                |                |                     |
   *                                v                |                     |
   *                                ------------------    SIGCHLD [fail]   |
   *                               |     RUNNING      |--------------------|
   *                                ------------------                     |
   *                                |                ^                     |
   *                                |                |                     |
   *                                | tpconfig down  | tpconfig up         |
   *                                |                |                     |
   *                                v                |                     |
   *                                ------------------      SIGCHLD        |
   *                               |     WAITDOWN     |--------------------
   *                                ------------------
   *
   * When the tapeserverd daemon is started, depending on the initial state
   * defined in /etc/castor/TPCONFIG, the daemon sends either a VDQM_UNIT_UP
   * or VDQM_UNIT_DOWN status message to the vdqmd daemon.  Once sent the state
   * of the tape drive is either DRIVE_STATE_UP or DRIVE_STATE_DOWN
   * respectively.
   *
   * A tape operator toggles the state of tape drive between DOWN and UP
   * using the tpconfig adminstration tool.
   *
   * The tape daemon can receive a job from the vdqmd daemon when the state of
   * tape drive is DRIVE_STATE_UP.  On reception of the job the daemon forks a
   * child process to run a tape mount session.  A tape will be mounted and
   * data will be transfered to and/or from that tape during the session. The
   * tape drive is in the DRIVE_STATE_RUNNING state whilst the tape session is
   * running.
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
   */
  enum DriveState { DRIVE_STATE_INIT, DRIVE_STATE_DOWN, DRIVE_STATE_UP,
    DRIVE_STATE_RUNNING, DRIVE_STATE_WAITDOWN };

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
  static const char *driveState2Str(const DriveState state) throw();

  /**
   * Poplates the catalogue using the specified parsed lines from
   * /etc/castor/TPCONFIG.
   *
   * @param lines The lines parsed from /etc/castor/TPCONFIG.
   */
  void populateCatalogue(const utils::TpconfigLines &lines)
    throw(castor::exception::Exception);

  /**
   * Returns the unit name of the drive on which the given process is running
   * @param sessionPid
   * @return the unit name of the drive on which the given process is running
   */
  std::string getUnitName(const pid_t sessionPid)
    throw(castor::exception::Exception);

  /**
   * Returns an unordered list of the unit names of all of the tape drives
   * stored within the tape drive catalogue.
   *
   * @return Unordered list of the unit names of all of the tape drives stored
   * within the tape drive catalogue.
   */
  std::list<std::string> getUnitNames()
    const throw(castor::exception::Exception);

  /**
   * Returns the device group name (DGN) of the specified tape drive.
   *
   * @param unitName The unit name of the tape drive.
   */
  const std::string &getDgn(const std::string &unitName)
    const throw(castor::exception::Exception);

  /**
   * Returns the filename of the device file of the specified tape drive.
   *
   * @param unitName The unit name of the tape drive.
   */
  const std::string &getDevFilename(const std::string &unitName)
    const throw(castor::exception::Exception);

  /**
   * Returns the tape densities supported by the specified tape drive.
   *
   * @param unitName The unit name of the tape drive.
   */
  const std::list<std::string> &getDensities(const std::string &unitName)
    const throw(castor::exception::Exception);

  /**
   * Returns the current state of the specified tape drive.
   *
   * @param unitName The unit name of the tape drive.
   */
  DriveState getState(const std::string &unitName)
    const throw(castor::exception::Exception);

  /**
   * Returns the position of the specified tape drive in its libary.
   *
   * @param unitName The unit name of the tape drive.
   */
  const std::string &getPositionInLibrary(const std::string &unitName)
    const throw(castor::exception::Exception);

  /**
   * Returns the device type of the specified tape drive in its libary.
   *
   * @param unitName The unit name of the tape drive.
   */
  const std::string &getDevType(const std::string &unitName) 
    const throw(castor::exception::Exception);

  /**
   * Moves the state of the specified tape drive from DRIVE_STATE_DOWN to
   * DRIVE_STATE_UP.
   *
   * This method throws an exception if the current state of the tape drive is
   * not DRIVE_STATE_DOWN.
   *
   * @param unitName The unit name of the tape drive.
   */
  void configureUp(const std::string &unitName)
    throw(castor::exception::Exception);

  /**
   * Moves the state of the specified tape drive from DRIVE_STATE_UP to
   * DRIVE_STATE_DOWN.
   *
   * This method throws an exception if the current state of the tape drive is
   * not DRIVE_STATE_UP.
   *
   * @param unitName The unit name of the tape drive.
   */
  void configureDown(const std::string &unitName)
    throw(castor::exception::Exception);

  /**
   * Moves the state of the specified tape drive from DRIVE_STATE_UP to
   * DRIVE_STATE_RUNNING.
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
   * @param unitName The unit name of the tape drive.
   * @param job The job received from the vdqmd daemon.
   * @param sessionPid The pid of the child process responsible for the tape session
   */
  void tapeSessionStarted(const std::string &unitName,
    const legacymsg::RtcpJobRqstMsgBody &job, const pid_t sessionPid)
    throw(castor::exception::Exception);

  /**
   * If the current state of the specified tape drive is DRIVE_STATE_RUNNING
   * then this method returns the job received from the vdqmd daemon.
   *
   * This method throws an exception if the current state of the tape drive is
   * not DRIVE_STATE_RUNNING.
   *
   * @param unitName The unit name of the tape drive.
   */
  const legacymsg::RtcpJobRqstMsgBody &getJob(const std::string &unitName)
    const throw(castor::exception::Exception);

  /**
   * Moves the state of the specified tape drive from DRIVE_STATE_RUNNING to
   * DRIVE_STATE_UP.
   *
   * This method throws an exception if the current state of the tape drive is
   * not DRIVE_STATE_RUNNING.
   *
   * @param unitName The unit name of the tape drive.
   */
  void tapeSessionSucceeded(const std::string &unitName)
     throw(castor::exception::Exception);
  
  /**
   * Same as above but one can use with the pid of the child process instead of
   * the unit name
   * @param pid of the child process handling the session
   */
  void tapeSessionSucceeded(const pid_t pid)
     throw(castor::exception::Exception);

  /**
   * Moves the state of the specified tape drive from DRIVE_STATE_RUNNING to
   * DRIVE_STATE_DOWN.
   *
   * This method throws an exception if the current state of the tape drive is
   * not DRIVE_STATE_RUNNING.
   *
   * @param unitName The unit name of the tape drive.
   */
  void tapeSessionFailed(const std::string &unitName)
     throw(castor::exception::Exception);
  
  /**
   * Same as above but one can use with the pid of the child process instead of
   * the unit name
   * @param pid of the child process handling the session
   */
  void tapeSessionFailed(const pid_t pid)
     throw(castor::exception::Exception);

private:

  /**
   * Structure used to store a tape drive in the catalogue.
   */
  struct DriveEntry {
    /**
     * The device group name of the tape drive as defined in
     * /etc/castor/TPCONFIG.
     */
    std::string dgn;

    /**
     * The device file of the tape drive, for eexample: /dev/nst0
     */
    std::string devFilename;

    /**
     * The tape densities supported by the tape drive.
     */
    std::list<std::string> densities;

    /**
     * The current state of the tape drive.
     */
    DriveState state;

    /**
     * The position of the tape drive within the tape library, for example:
     * smc@localhost,0
     */
    std::string positionInLibrary;

    /**
     * The device type of the tape drive, for example: T10000
     */
    std::string devType;

    /**
     * The job received from the vdqmd daemon when the drive is in the
     * DRIVE_STATE_RUNNING state.  In all other states the value of this
     * member variable is undefined.
     */
    legacymsg::RtcpJobRqstMsgBody job;
    
    /**
     * The pid of the child process handling the tape session running on the 
     * tape drive.
     */
    pid_t pid;

    /**
     * Default constructor that initializes all strings to the empty string,
     * all integers to zero, all lists to empty and the drive state to
     * DRIVE_STATE_INIT.  This initialization includes the individual member
     * variables of the nested vdqm job.
     */
    DriveEntry() throw():
      state(DRIVE_STATE_INIT) {
      job.volReqId = 0;
      job.clientPort = 0;
      job.clientEuid = 0;
      job.clientEgid = 0;
      memset(job.clientHost, '\0', sizeof(job.clientHost));
      memset(job.dgn, '\0', sizeof(job.dgn));
      memset(job.driveUnit, '\0', sizeof(job.driveUnit));
      memset(job.clientUserName, '\0', sizeof(job.clientUserName));
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
  void enterTpconfigLine(const utils::TpconfigLine &line)
    throw(castor::exception::Exception);

  /**
   * Returns the equivalent DriveState value of the specified string
   * representation of the initial state of a tape drive.
   *
   * This method throws an exception if the specified string is neither "up"
   * nor "down" (case insensitive).
   *
   * @param initialState String representation of the initial tape-drive state.
   */
  DriveState str2InitialState(const std::string &initialState)
    const throw(castor::exception::Exception);

  /**
   * Checks the semantics of the specified TPCONFIG line against the specified
   * current catalogue entry.
   *
   * @param catalogueEntry The catalogue entry.
   * @param line The line parsed from /etc/castor/TPCONFIG.
   */
  void checkTpconfigLine(const DriveEntry &catalogueEntry,
    const utils::TpconfigLine &line) throw(castor::exception::Exception);

  /**
   * Throws an exception if the specified catalogue value does not match the
   * specified TPCONFIG line value.
   *
   * @param catalogueDgn The DGN of the tape drive that has been retrieved from
   * the tape-drive catalogue.
   * @param line The line parsed from /etc/castor/TPCONFIG.
   */
  void checkTpconfigLineDgn(const std::string &catalogueDgn,
    const utils::TpconfigLine &line) throw(castor::exception::Exception);

  /**
   * Throws an exception if the specified catalogue value does not match the
   * specified TPCONFIG line value.
   *
   * @param catalogueDevFilename The filename of the device file of the tape
   * drive that has been retrieved from the tape-drive catalogue.
   * @param line The line parsed from /etc/castor/TPCONFIG.
   */
  void checkTpconfigLineDevFilename(const std::string &catalogueDevFilename,
    const utils::TpconfigLine &line) throw(castor::exception::Exception);

  /**
   * Throws an exception if the specified catalogue value does not match the
   * specified TPCONFIG line value.
   *
   * @param catalogueDensities The densities supported by the tape drive that
   * have been retrived from the tape-drive catalogue.
   * @param line The line parsed from /etc/castor/TPCONFIG.
   */
  void checkTpconfigLineDensity(
    const std::list<std::string> &catalogueDensities, 
    const utils::TpconfigLine &line) throw(castor::exception::Exception);

  /**
   * Throws an exception if the specified catalogue value does not match the
   * specified TPCONFIG line value.
   *
   * @param catalogueInitialState The initial state of the tape drive that
   * has been retrieved from the tape-drive catalogue.
   * @param line The line parsed from /etc/castor/TPCONFIG.
   */
  void checkTpconfigLineInitialState(const DriveState catalogueInitialState,
    const utils::TpconfigLine &line) throw(castor::exception::Exception);

  /**
   * Throws an exception if the specified catalogue value does not match the
   * specified TPCONFIG line value.
   *
   * @param cataloguePositionInLibrary The position of the tape drive within
   * its library that has been retrieved from tape-drive catalogue.
   * @param line The line parsed from /etc/castor/TPCONFIG.
   */
  void checkTpconfigLinePositionInLibrary(
    const std::string &cataloguePositionInLibrary,
    const utils::TpconfigLine &line) throw(castor::exception::Exception);

  /**
   * Throws an exception if the specified catalogue value does not match the
   * specified TPCONFIG line value.
   *
   * @param catalogueDevType The device type of the tape drive that has been
   * retrieved from the tape-drive library.
   * @param line The line parsed from /etc/castor/TPCONFIG.
   */
  void checkTpconfigLineDevType(const std::string &catalogueDevType,
    const utils::TpconfigLine &line) throw(castor::exception::Exception);

}; // class DriveCatalogue

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor

