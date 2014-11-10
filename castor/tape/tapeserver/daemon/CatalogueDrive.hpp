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

#include "castor/legacymsg/CupvProxy.hpp"
#include "castor/legacymsg/RtcpJobRqstMsgBody.hpp"
#include "castor/legacymsg/TapeLabelRqstMsgBody.hpp"
#include "castor/legacymsg/TapeStatDriveEntry.hpp"
#include "castor/legacymsg/VdqmProxy.hpp"
#include "castor/legacymsg/VmgrProxy.hpp"
#include "castor/log/Logger.hpp"
#include "castor/tape/tapeserver/daemon/CatalogueCleanerSession.hpp"
#include "castor/tape/tapeserver/daemon/CatalogueLabelSession.hpp"
#include "castor/tape/tapeserver/daemon/CatalogueSession.hpp"
#include "castor/tape/tapeserver/daemon/CatalogueTransferSession.hpp"
#include "castor/tape/tapeserver/daemon/ProcessForkerProxy.hpp"
#include "castor/tape/utils/DriveConfig.hpp"

#include <iostream>
#include <memory>
#include <string>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

namespace castor     {
namespace tape       {
namespace tapeserver {
namespace daemon     {

/**
 * A tape-drive entry in the tape-drive catalogue.
 */
class CatalogueDrive {
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
  enum DriveState {
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
  static const char *driveStateToStr(const DriveState state) throw();

  /**
   * Constructor that except for its parameters, initializes all strings to
   * the empty string, all integers to zero, all file descriptors to -1,
   * all lists to the emptylist, and the sessionType to SESSION_TYPE_NONE.
   *
   * @param netTimeout Timeout in seconds to be used when performing network
   * I/O.
   * @param log Object representing the API of the CASTOR logging system.
   * @param processForker Proxy object representing the ProcessForker.
   * @param cupv Proxy object representing the cupvd daemon.
   * @param vdqm Proxy object representing the vdqmd daemon.
   * @param vmgr Proxy object representing the vmgrd daemon.
   * @param hostName The name of the host on which the daemon is running.  This
   * name is needed to fill in messages to be sent to the vdqmd daemon.
   * @param config The configuration of the tape drive.
   * @param state The initial state of the tape drive.
   * @param waitJobTimeoutInSecs The maximum time in seconds that the
   * data-transfer session can take to get the transfer job from the client.
   * @param mountTimeoutInSecs The maximum time in seconds that the
   * data-transfer session can take to mount a tape.
   * @param blockMoveTimeoutInSecs The maximum time in seconds that the
   * data-transfer session can cease to move data blocks.
   */
  CatalogueDrive(
    const int netTimeout,
    log::Logger &log,
    ProcessForkerProxy &processForker,
    legacymsg::CupvProxy &cupv,
    legacymsg::VdqmProxy &vdqm,
    legacymsg::VmgrProxy &vmgr,
    const std::string &hostName,
    const utils::DriveConfig &config,
    const DriveState state,
    const time_t waitJobTimeoutInSecs,
    const time_t mountTimeoutInSecs,
    const time_t blockMoveTimeoutInSecs) throw();

  /**
   * Destructor
   */
  ~CatalogueDrive() throw();

  /**
   * Handles a tick in time.  Time driven actions such as alarms should be
   * implemented here.
   *
   * This method does not have to be called at any time precise interval,
   * though it should be called at least twice as fast as the quickest reaction
   * time imposed on the catalogue.
   *
   * @return True if the main event loop should continue, else false.
   */
  bool handleTick();

  /**
   * If there is a catalogue session associated with the tape drive then this
   * method deletes it and sets the member variable pointing to it to NULL in
   * order to prevent double deletions.
   */
  void deleteSession();

  /**
   * Gets the configuration of the tape-drive.
   *
   * @return The configuration of the tape-drive.
   */
  const tape::utils::DriveConfig &getConfig() const;

  /**
   * Gets the current state of the tape drive.
   *
   * @return The current state of the tape drive.
   */
  DriveState getState() const throw();
  
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
   * Moves the state of tape drive to DRIVE_STATE_SESSIONRUNNING and sets the 
   * current session type to SESSION_TYPE_DATATRANSFER.
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
   * Moves the state of tape drive to DRIVE_STATE_SESSIONRUNNING and sets the 
   * current session type to SESSION_TYPE_LABEL.
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
   * an appropriate error message to the client.
   *
   * @param job The label job.
   * @param labelCmdConnection The file descriptor of the TCP/IP connection
   * with the tape labeling command-line tool castor-tape-label.
   */
  void receivedLabelJob(const legacymsg::TapeLabelRqstMsgBody &job,
    const int labelCmdConnection);
  
  /**
   * Creates a cleaner session to eject any tape left in the tape drive.
   *
   * @param vid The volume identifier of the tape currently in the tape drive
   * or the empty string if not know.
   * @param The assignment time associated with the tape drive or 0 if not
   * known.  The assignment time is given as the number of seconds elapsed
   * since the Epoch.
   * @param driveReadyDelayInSeconds The maximum number of seconds to wait for
   * the drive to be raedy with a tape inside of it.
   */
  void createCleaner(const std::string &vid, const time_t assignmentTime,
    const uint32_t driveReadyDelayInSeconds);

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
   * Gets the tpstat representation of the tape drive.
   *
   * @return The tpstat representation of the tape drive.
   */
  legacymsg::TapeStatDriveEntry getTapeStatDriveEntry() const;

  /** 
   * Returns the catalogue cleaner-session associated with the tape drive.
   *
   * This method throws a castor::exception::Exception if there is no
   * cleaner session associated with the tape drive.
   */
  const CatalogueCleanerSession &getCleanerSession() const;
    
  /**
   * Returns the catalogue cleaner-session associated with the tape drive.
   *
   * This method throws a castor::exception::Exception if there is no
   * cleaner session associated with the tape drive.
   */ 
  CatalogueCleanerSession &getCleanerSession();

  /**
   * Returns the catalogue label-session associated with the tape drive.
   *
   * This method throws a castor::exception::Exception if there is no
   * label session associated with the tape drive.
   */
  const CatalogueLabelSession &getLabelSession() const;

  /**
   * Returns the catalogue label-session associated with the tape drive.
   *
   * This method throws a castor::exception::Exception if there is no
   * label session associated with the tape drive.
   */
  CatalogueLabelSession &getLabelSession();

  /**
   * Returns the catalogue transfer-session associated with the tape drive.
   *
   * This method throws a castor::exception::Exception if there is no
   * transfer session associated with the tape drive.
   */
  const CatalogueTransferSession &getTransferSession() const;

  /**
   * Returns the catalogue transfer-session associated with the tape drive.
   *
   * This method throws a castor::exception::Exception if there is no
   * transfer session associated with the tape drive.
   */
  CatalogueTransferSession &getTransferSession();

  /**
   * Gets the session asscoiated with the tape drive.
   *
   * This method throws castor::exception::Exception if there is no session
   * currently associated with the tape drive.
   *
   * Please use either getCleanerSession(), getLabelSession() or
   * getTransferSession() instead of this method if a specific type of session
   * is required.
   * 
   * @return The session associated with the tape drive.
   */
  const CatalogueSession &getSession() const;

  /**
   * Tries to determine the volume identifier of the tape currently associated
   * with the tape drive.
   *
   * @return The volume identifier of the tape or the empty string if it was
   * not possible to determine.
   */
  std::string getVidForCleaner() const throw();

  /**
   * Tries to determine the assigment time associated with the tap[e drive.
   *
   * @return The assigment time associated with the tape drive or 0 if not
   * known.  The assignment time is given in seconds elapsed since the Epoch.
   */
  time_t getAssignmentTimeForCleaner() const throw();

  /**
   * If there is a running session that is not a cleaner session then this
   * method kills the session and runs a cleaner session.
   */
  void shutdown();

  /**
   * If there is a running session then this method kills it and sets the drive
   * down in the vdqm and the drive catalogue.
   *
   * If there is no running session then this method does nothing.
   */
  void killSession();

private:

  /**
   * Copy constructor declared private to prevent copies.
   */
  CatalogueDrive(const CatalogueDrive&);

  /**
   * Assignment operator declared private to prevent assignments.
   */
  CatalogueDrive& operator=(const CatalogueDrive&);

  /**
   * Timeout in seconds to be used when performing network I/O.
   */
  const int m_netTimeout;

  /**
   * The object representing the API of the CASTOR logging system.
   */
  log::Logger &m_log;

  /**
   * Proxy object representing the ProcessForker.
   */
  ProcessForkerProxy &m_processForker;

  /**
   * Proxy object reprsenting the cupvd daemon.
   */
  legacymsg::CupvProxy &m_cupv;

  /**
   * Proxy object representing the vdqmd daemon.
   */
  legacymsg::VdqmProxy &m_vdqm;

  /**
   * Proxy object representing the vmgrd daemon.
   */
  legacymsg::VmgrProxy &m_vmgr;

  /**
   * The name of the host on which the daemon is running.  This name is
   * needed to fill in messages to be sent to the vdqmd daemon.
   */   
  const std::string m_hostName;

  /**
   * The configuration of the tape-drive.
   */
  tape::utils::DriveConfig m_config;

  /**
   * The current state of the tape drive.
   */
  DriveState m_state;

  /**
   * The maximum time in seconds that the data-transfer session can take to get
   * the transfer job from the client.
   */
  const time_t m_waitJobTimeoutInSecs;

  /**
   * The maximum time in seconds that the data-transfer session can take to
   * mount a tape.
   */
  const time_t m_mountTimeoutInSecs;

  /**
   * The maximum time in seconds that the data-transfer session can cease to
   * move data blocks.
   */
  const time_t m_blockMoveTimeoutInSecs;
  
  /**
   * The session metadata associated to the drive catalogue entry
   */
  CatalogueSession *m_session;

  /**
   * Checks that there is a tape session currently associated with the
   * tape drive.
   *
   * This method throws castor::exception::Exception if there is no
   * tape session associated with the tape drive.
   */
  void checkForSession() const;

  /**
   * Checks that there is a cleaner session currently associated with the
   * tape drive.
   *
   * This method throws castor::exception::Exception if there is no
   * cleaner session associated with the tape drive.
   */
  void checkForCleanerSession() const;

  /**
   * Checks that there is a label session currently associated with the
   * tape drive.
   *
   * This method throws castor::exception::Exception if there is no
   * label session associated with the tape drive.
   */
  void checkForLabelSession() const;

  /**
   * Checks that there is a transfer session currently associated with the
   * tape drive.
   *
   * This method throws castor::exception::Exception if there is no
   * transfer session associated with the tape drive.
   */
  void checkForTransferSession() const;

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
   * Determines the value of the asnTime field of a TapeStatDriveEntry to be
   * used in a TapeStatReplyMsgBody.
   */
  uint32_t getAsnTimeForTapeStatDriveEntry() const throw();

  /**
   * Determines the value of the mode field of a TapeStatDriveEntry to be used
   * in a TapeStatReplyMsgBody.
   *
   * @return The value of the mode field if known else WRITE_DISABLE.
   */
  uint16_t getModeForTapeStatDriveEntry() const throw();

  /**
   * Determines the value of the lblCode field of a TapeStatDriveEntry to be
   * used in a TapeStatReplyMsgBody.
   *
   * @return Always "aul" because this is the only tape format supported by
   * CASTOR.
   */
  std::string getLblCodeForTapeStatDriveEntry() const throw();

  /**
   * Determines the value of the tobeMounted field of a TapeStatDriveEntry to be
   * used in a TapeStatReplyMsgBody.
   *
   * @return The value of the tobeMounted filed if known, else 0.
   */
  uint16_t getToBeMountedForTapeStatDriveEntry() const throw();

  /**
   * Determines the value of the vid field of a TapeStatDriveEntry to be used
   * in a TapeStatReplyMsgBody.
   *
   * @return The volume indenfier if known else the empty string.
   */
  std::string getVidForTapeStatDriveEntry() const throw();

  /**
   * Returns the value of the vsn field of a TapeStatDriveEntry to be used
   * in a TapeStatReplyMsgBody.
   */
  std::string getVsnForTapeStatDriveEntry() const throw();

  /**
   * Helper method for logging and changing the state of the catalogue drive.
   *
   * @param newState The state to which the catalogue drive should be changed.
   */
  void changeState(const DriveState newState) throw();

  /**
   * Called when a running session (DRIVE_STATE_RUNNING or DRIVE_STATE_WAITDOWN)
   * has failed.
   */
  void runningSessionFailed();

  /**
   * Called when a running session has been intentionally killed by the shutdown
   * sequence.
   */
  void sessionKilledByShutdown();

  /**
   * Called when a CLeanerSession of the shutdown sequence has failed.
   */
  void cleanerOfShutdownFailed();

}; // class CatalogueDrive

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor
