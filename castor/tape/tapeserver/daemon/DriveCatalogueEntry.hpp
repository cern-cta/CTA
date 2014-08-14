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

#include "castor/legacymsg/RtcpJobRqstMsgBody.hpp"
#include "castor/legacymsg/TapeLabelRqstMsgBody.hpp"
#include "castor/legacymsg/TapeStatDriveEntry.hpp"
#include "castor/legacymsg/VdqmProxy.hpp"
#include "castor/log/Logger.hpp"
#include "castor/tape/tapeserver/daemon/DriveCatalogueCleanerSession.hpp"
#include "castor/tape/tapeserver/daemon/DriveCatalogueLabelSession.hpp"
#include "castor/tape/tapeserver/daemon/DriveCatalogueSession.hpp"
#include "castor/tape/tapeserver/daemon/DriveCatalogueTransferSession.hpp"
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
   * The tape daemon can receive a job from the vdqmd daemon for a tape drive
   * when the state of that drive is DRIVE_STATE_UP.  On reception of the job
   * the daemon has the ProcessForker fork a child process.
   *
   * Once the child process is forked the drive enters the DRIVE_STATE_RUNNING
   * state.  The child process is responsible for running a data-transfer
   * session. During such a sesion a tape will be mounted, data will be
   * transfered to and/or from the tape and finally the tape will be dismounted.
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
    SESSION_TYPE_CLEANER,
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
   * Constructor that except for its parameters, initializes all strings to
   * the empty string, all integers to zero, all file descriptors to -1,
   * all lists to the emptylist, and the sessionType to SESSION_TYPE_NONE.
   *
   * @param log Object representing the API of the CASTOR logging system.
   * @param processForker Proxy object representing the ProcessForker.
   * @param vdqm Proxy object representing the vdqmd daemon.
   * @param hostName The name of the host on which the daemon is running.  This
   * name is needed to fill in messages to be sent to the vdqmd daemon.
   * @param config The configuration of the tape drive.
   * @param dataTransferConfig The configuration of a data-transfer session.
   * @param state The initial state of the tape drive.
   */
  DriveCatalogueEntry(
    log::Logger &log,
    ProcessForkerProxy &processForker,
    legacymsg::VdqmProxy &vdqm,
    const std::string &hostName,
    const utils::DriveConfig &config,
    const DataTransferSession::CastorConf &dataTransferConfig,
    const DriveState state) throw();

  /**
   * Destructor
   */
  ~DriveCatalogueEntry() throw();

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
   * Gets the current state of the tape drive.
   *
   * @return The current state of the tape drive.
   */
  DriveState getState() const throw();
  
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
   * Moves the state of tape drive to DRIVE_STATE_SESSIONRUNNING and sets the 
   * current session type to SESSION_TYPE_CLEANER.
   * 
   * This method will accept any drive state.
   */   
  void toBeCleaned();

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
   * The process ID of the child process running the data-transfer session.
   *
   * This method throws a castor::exception::Exception if the tape drive is not
   * in a state for which there is a session process-ID.
   *
   * @return The process ID of the child process running the data-transfer
   * session.
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
  const DriveCatalogueCleanerSession &getCleanerSession() const;
    
  /**
   * Returns the catalogue cleaner-session associated with the tape drive.
   *
   * This method throws a castor::exception::Exception if there is no
   * cleaner session associated with the tape drive.
   */ 
  DriveCatalogueCleanerSession &getCleanerSession();

  /**
   * Returns the catalogue label-session associated with the tape drive.
   *
   * This method throws a castor::exception::Exception if there is no
   * label session associated with the tape drive.
   */
  const DriveCatalogueLabelSession &getLabelSession() const;

  /**
   * Returns the catalogue label-session associated with the tape drive.
   *
   * This method throws a castor::exception::Exception if there is no
   * label session associated with the tape drive.
   */
  DriveCatalogueLabelSession &getLabelSession();

  /**
   * Returns the catalogue transfer-session associated with the tape drive.
   *
   * This method throws a castor::exception::Exception if there is no
   * transfer session associated with the tape drive.
   */
  const DriveCatalogueTransferSession &getTransferSession() const;

  /**
   * Returns the catalogue transfer-session associated with the tape drive.
   *
   * This method throws a castor::exception::Exception if there is no
   * transfer session associated with the tape drive.
   */
  DriveCatalogueTransferSession &getTransferSession();

private:

  /**
   * Copy constructor declared private to prevent copies.
   */
  DriveCatalogueEntry(const DriveCatalogueEntry&);

  /**
   * Assignment operator declared private to prevent assignments.
   */
  DriveCatalogueEntry& operator=(const DriveCatalogueEntry&);

  /**
   * The object representing the API of the CASTOR logging system.
   */
  log::Logger &m_log;

  /**
   * Proxy object representing the ProcessForker.
   */
  ProcessForkerProxy &m_processForker;

  /**
   * Proxy object representing the vdqmd daemon.
   */
  legacymsg::VdqmProxy &m_vdqm;

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
   * The configuration of a data-transfer session.
   */
  const DataTransferSession::CastorConf m_dataTransferConfig;

  /**
   * The current state of the tape drive.
   */
  DriveState m_state;
  
  /**
   * The type of data-transfer session.
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
   * Gets the tape-session asscoiated with the tape drive.
   *
   * This method throws castor::exception::Exception if there is no session
   * currently associated with the tape drive.
   * 
   * @return The tape-session associated with the tape drive.
   */
  const DriveCatalogueSession &getSession() const;

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
