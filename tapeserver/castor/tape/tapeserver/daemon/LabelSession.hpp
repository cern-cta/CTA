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

#include "castor/legacymsg/TapeLabelRqstMsgBody.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/Logger.hpp"
#include "common/processCap/ProcessCap.hpp"
#include "castor/tape/tapeserver/daemon/Session.hpp"
#include "castor/tape/tapeserver/daemon/VolumeInfo.hpp"
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "castor/tape/tapeserver/SCSI/Device.hpp"
#include "castor/tape/tapeserver/system/Wrapper.hpp"
#include "castor/tape/tapeserver/daemon/LabelSessionConfig.hpp"
#include "castor/tape/tapeserver/daemon/EncryptionControl.hpp"
#include "mediachanger/MediaChangerFacade.hpp"
#include "tapeserver/daemon/TapedProxy.hpp"

#include <memory>

namespace castor {
namespace tape {
namespace tapeserver {
namespace daemon {

/**
 * Class responsible for handling a tape label session.
 */
class LabelSession: public Session {
public:
  
  /**
   *  Constructor 
   *
   * @param capUtils Object providing support for UNIX capabilities.
   * @param tapeserver Proxy object representing the tapeserverd daemon.
   * @param mc Object representing the media changer.
   * @param clientRequest The request to label a tape received from the label
   * tape command.
   * @param log Object representing the API to the CASTOR logging system.
   * @param sysWrapper Object representing the operating system.
   * @param driveConfig the configuration of the tape-drive to be used to
   * label a tape.
   * @param force The flag that, if set to true, allows labeling a non-blank
   * tape.
   * @param lbp The flag that, if set to true, allows labeling a tape with 
   *            logical block protection. This parameter comes from
   *            castor-tape-label command line tool.
   * @param labelSessionConfig 
   * @param externalEncryptionKeyScript path to the operator provided script
   * for encryption key management control.
   */
  LabelSession(
    cta::server::ProcessCap &capUtils,
    cta::tape::daemon::TapedProxy &tapeserver,
    cta::mediachanger::MediaChangerFacade &mc,
    const legacymsg::TapeLabelRqstMsgBody &clientRequest, 
    cta::log::Logger &log,
    System::virtualWrapper &sysWrapper,
    const cta::tape::daemon::TpconfigLine &driveConfig,
    const bool force,
    const bool lbp,
    const LabelSessionConfig &labelSessionConfig,
    const std::string & externalEncryptionKeyScript);
  
  /** 
   * Execute the session and return the type of action to be performed
   * immediately after the session has completed.
   *
   * @return Returns the type of action to be performed after the session has
   * completed.
   */
  EndOfSessionAction execute() throw();
    
private:

  /**
   * Object providing support for UNIX capabilities.
   */
  cta::server::ProcessCap &m_capUtils;

  /**
   * Proxy object representing the tapeserverd daemon.
   */
  cta::tape::daemon::TapedProxy &m_tapeserver;
    
  /**
   * The object representing the media changer.
   */
  cta::mediachanger::MediaChangerFacade &m_mc;

  /**
   * The label request message body
   */
  legacymsg::TapeLabelRqstMsgBody m_request;
  
  /**
   * The logging object     
   */
  cta::log::Logger & m_log;
    
  /**
   * The system wrapper used to find the device and instantiate the drive object
   */
  System::virtualWrapper &m_sysWrapper;
  
  /**
   * The configuration of the tape drive to be used to label a tape.
   */
  const cta::tape::daemon::TpconfigLine m_driveConfig;
  
  /**
   * The configuration parameters from castor.conf specific for of the tape drive to be used to label a tape.
   */
  const LabelSessionConfig m_labelSessionConfig;
  
  /**
   * The flag that, if set to true, allows labeling a non-blank tape
   */
  const bool m_force;
  
  /**
   * The flag that, if set to true, allows labeling a tape with logical 
   * block protection
   */
  const bool m_lbp;

  /** 
   * Encryption helper object 
   */
  EncryptionControl m_encryptionControl;

  /** 
   * Execute the session and return the type of action to be performed
   * immediately after the session has completed.
   *
   * @return Returns the type of action to be performed after the session has
   * completed.
   */
  EndOfSessionAction exceptionThrowingExecute();

  /**
   * Sets the capabilities of the process and logs the result.
   *
   * @param capabilities The string representation of the capabilities.
   */
  void setProcessCapabilities(const std::string &capabilities);
    
  /**
   * Performs some meta-data checks that need to be done before deciding to
   * mount the tape for labeling.
   */ 
  void performPreMountChecks();

  /**
   * A meta-data check that sees if the user of the client is either the
   * owner of the tape pool containing the tape to be labelled or is an ADMIN
   * user within the CUPV privileges database.
   */
  void checkClientIsOwnerOrAdmin();

  /**
   * Returns a Drive object representing the tape drive to be used to label
   * a tape.
   *
   * @return The drive object.
   */
  std::unique_ptr<drive::DriveInterface> createDrive();

  /**
   * Mounts the tape to be labelled.
   */
  void mountTape();
  
  /**
   * Waits for the tape to be loaded into the tape drive.
   *
   * @param drive Object representing the drive hardware.
   * @param timeoutSecond The number of seconds to wait for the tape to be
   * loaded into the tape drive. 
   */
  void waitUntilTapeLoaded(drive::DriveInterface &drive,
    const int timeoutSecond);

  /**
   * Rewinds the specified tape drive.
   *
   * @param drive The tape drive.
   */
  void rewindDrive(drive::DriveInterface &drive);

  /**
   * Notifies the tape-server parent-process of the specified user error.
   *
   * @param message The error message.
   */
  void notifyTapeserverOfUserError(const std::string message);
  
  /**
   * Writes the label file to the tape.
   *
   * This method assumes the tape has been rewound.
   *
   * @param drive The tape drive.
   */
  void writeLabelToTape(drive::DriveInterface &drive);

  /**
   * Writes the label file with logical block protection to the tape.
   *
   * This method assumes the tape has been rewound.
   *
   * @param drive The tape drive.
   */
  void writeLabelWithLbpToTape(drive::DriveInterface &drive);

  /**
   * Unloads the specified tape from the specified tape drive.
   *
   * @param vid The volume identifier of the tape to be unloaded.  Please note
   * that the value of this field is only used for logging purposes.
   * @param drive The tape drive.
   */
  void unloadTape(const std::string &vid, drive::DriveInterface &drive);

  /**
   * Dismounts the specified tape.
   *
   * @param vid The volume identifier of the tape to be dismounted.
   */
  void dismountTape(const std::string &vid);

  /**
   * Returns the string representation of the specified boolean value.
   *
   * @param value The boolean value.
   * @return The string representation.
   */
  const char *boolToStr(const bool value);

}; // class LabelSession

} // namespace daemon
} // namespace tapeserver
} // namespace tape
} // namespace castor
