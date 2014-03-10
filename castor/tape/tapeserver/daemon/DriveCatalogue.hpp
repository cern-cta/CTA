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

#ifndef CASTOR_TAPE_TAPESERVER_DAEMON_DRIVECATALOGUE_HPP
#define CASTOR_TAPE_TAPESERVER_DAEMON_DRIVECATALOGUE_HPP 1

#include "castor/exception/Exception.hpp"
#include "castor/tape/legacymsg/RtcpJobRqstMsgBody.hpp"

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
   *  ------    send VDQM_UNIT_UP   ----------------
   * | INIT |--------------------->|      DOWN      |<-------------------
   *  ------                        ----------------                     |
   *     |                          |              ^                     |
   *     |                          |              |                     |
   *     |                          | tpconfig up  | tpconfig down       |
   *     |                          |              |                     |
   *     |      start daemon /      v              |                     |
   *     |   send VDQM_UNIT_DOWN    ----------------                     |
   *      ------------------------>|      UP        |                    |
   *                                ----------------                     |
   *                                |              ^                     |
   *                                |              |                     |
   *                                | vdqm job /   | SIGCHLD [success]   |
   *                                | fork         |                     |
   *                                |              |                     |
   *                                v              |                     |
   *                                ----------------    SIGCHLD [fail]   |
   *                               |    RUNNING     |-------------------- 
   *                                ----------------
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
   */
  enum DriveState { DRIVE_STATE_INIT, DRIVE_STATE_DOWN, DRIVE_STATE_UP,
    DRIVE_STATE_RUNNING };

  /**
   * Enters the specified tape drive into the catalogue.
   *
   * This method throws an exception if the initial state is neither
   * DRIVE_STATE_DOWN nor DRIVE_STATE_UP.
   *
   * @param unitName The unit name of the drive.
   * @param dgn The device group name of the tape drive.
   * @param initialState The initial state of the drive as configured in
   * /etc/castor/TPCONFIG.
   */
  void enterDrive(const std::string &unitName, const std::string &dgn,
    const DriveState initialState) throw(castor::exception::Exception);

  /**
   * Returns the current state of the specified tape drive.
   *
   * @param unitName The unit name of the tape drive.
   */
  DriveState getState(const std::string &unitName)
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
   * enterDrive() method.
   *
   * @param unitName The unit name of the tape drive.
   * @param job The job received from the vdqmd daemon.
   */
  void tapeSessionStarted(const std::string &unitName,
    const legacymsg::RtcpJobRqstMsgBody &job)
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
  void tapeSessionSuceeeded(const std::string &unitName)
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
     * The current state of the tape drive.
     */
    DriveState state;

    /**
     * The job received from the vdqmd daemon when the drive is in the
     * DRIVE_STATE_RUNNING state.  In all other states the value of this
     * member variable is undefined.
     */
    legacymsg::RtcpJobRqstMsgBody job;

    /**
     * Default constructor that initializes all strings to the empty string,
     * all integers to zero and all drive states to DRIVE_STATE_INIT.  This
     * initialization includes the individual member variables of the nested
     * vdqm job.
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

}; // class DriveCatalogue

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TAPESERVER_DAEMON_DRIVECATALOGUE_HPP
