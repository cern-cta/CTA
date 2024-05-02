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

#include <memory>

#include "common/CmdLineTool.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/StdoutLogger.hpp"
#include "common/processCap/ProcessCap.hpp"
#include "mediachanger/MediaChangerFacade.hpp"
#include "tapeserver/castor/tape/tapeserver/daemon/EncryptionControl.hpp"
#include "tapeserver/castor/tape/tapeserver/drive/DriveGeneric.hpp"
#include "tapeserver/castor/tape/tapeserver/drive/DriveInterface.hpp"

namespace cta {

namespace catalogue {
class Catalogue;
}

namespace tapeserver::tapelabel {

/**
 * Command-line tool for pre-labeling a CTA tape.
 */
class TapeLabelCmd: public common::CmdLineTool {
public:
  /**
   * Constructor
   *
   * @param inStream Standard input stream
   * @param outStream Standard output stream
   * @param errStream Standard error stream
   * @param log The object representing the API of the CTA logging system
   */
   TapeLabelCmd(std::istream& inStream, std::ostream& outStream, std::ostream& errStream, log::StdoutLogger& log) : CmdLineTool(inStream, outStream, errStream),
      m_log(log) { }

  /**
   * Destructor
   */
  ~TapeLabelCmd() final = default;

private:
  /**
   * The object representing the API of the CTA logging system.
   */
  cta::log::StdoutLogger& m_log;
  
  /**
   * Hard coded path for the catalogue login configuration.
   */
  const std::string CATALOGUE_CONFIG_PATH = "/etc/cta/cta-catalogue.conf";
  
  /**
   * Unique pointer to the catalogue interface;
   */
  std::unique_ptr<cta::catalogue::Catalogue> m_catalogue;
  
  /**
   * Object providing utilities for working UNIX capabilities.
   */
  cta::server::ProcessCap m_capUtils;
  
  /**
   * The system wrapper used to find the device and instantiate the drive object.
   */
  castor::tape::System::realWrapper m_sysWrapper;
  
  /**
   * The filename of the device file of the tape drive.
   */
  std::string m_devFilename;
  
  /**
   * The slot in the tape library that contains the tape drive (string encoded).
   */
  std::string m_rawLibrarySlot;
  
  /**
   * The logical library of the tape drive.
   */
  std::string m_logicalLibrary;
  
  /**
   * The unit name of the tape drive.
   */
  std::string m_unitName;
  
  /**
   * The name of the user running the command-line tool.
   */
  std::string m_userName;
  
  /**
   * The tape VID to be pre-label.
   */
  std::string m_vid;
  
  /**
   * The old label on tape to be checked when pre-labeling.
   */
  std::string m_oldLabel;
  
  /** 
   * Encryption helper object 
   */
  castor::tape::tapeserver::daemon::EncryptionControl m_encryptionControl { false, "" };

  /**
   * Object representeing the rmc proxy
   */
  std::unique_ptr<cta::mediachanger::RmcProxy> m_rmcProxy;

  /**
   * The object representing the media changer.
   */
  std::unique_ptr<cta::mediachanger::MediaChangerFacade> m_mc;
  
  /**
   * Use Logical Block Protection?
   */
  const bool m_useLbp = true;
  
  /**
   * Does the drive support Logical Block Protection?
   */
  bool m_driveSupportLbp = true;
  
  /**
   * Skip label checks on non-blank tapes?
   */ 
  bool m_force = false;
  
  /**
   * An exception throwing version of main().
   *
   * @param argc The number of command-line arguments including the program name.
   * @param argv The command-line arguments.
   * @return The exit value of the program.
   */
  int exceptionThrowingMain(const int argc, char *const *const argv) override;

  /**
   * Prints the usage message of the command-line tool.
   *
   * @param os The output stream to which the usage message is to be printed.
   */
  void printUsage(std::ostream &os) override;
  
  /**
   * Sets internal configuration parameters to be used for labeling.
   * It reads drive and library parameters from /etc/cta/cta-taped-*.conf
   * and catalogue login parameters from /etc/cta/cta-catalogue.conf.
   *
   * @param username The name of the user running the command-line tool.
   * @param vid The tape VID to be pre-label.
   * @param oldLabel The old label on tape to be checked when pre-labeling. Could be empty.
   * @param unitName The unit name of the drive used to label the tape
   */
  void readAndSetConfiguration(const std::string &userName,
    const std::string &vid, const std::string &oldLabel,  const std::optional<std::string> &unitName);

  /**
   * Returns a Drive object representing the tape drive to be used to label
   * a tape.
   *
   * @return The drive object.
   */
  std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface> createDrive();
  
  /**
   * Mounts the tape to be labeled.
   * @param vid The volume identifier of the tape to be mounted.
   */
  void mountTape(const std::string &vid);
  
  /**
   * Waits for the tape to be loaded into the tape drive.
   *
   * @param drive Object representing the drive hardware.
   * @param timeoutSecond The number of seconds to wait for the tape to be
   * loaded into the tape drive. 
   */
  void waitUntilTapeLoaded(castor::tape::tapeserver::drive::DriveInterface &drive,
    const int timeoutSecond);
  
  /**
   * Writes the label file with logical block protection to the tape.
   *
   * This method assumes the tape has been rewound.
   *
   * @param drive The tape drive.
   */
  void writeLabelWithLbpToTape(castor::tape::tapeserver::drive::DriveInterface &drive);
  
  /**
   * Writes the label file to the tape.
   *
   * This method assumes the tape has been rewound.
   *
   * @param drive The tape drive.
   */
  void writeLabelToTape(castor::tape::tapeserver::drive::DriveInterface &drive);
  
  /**
   * Unloads the specified tape from the specified tape drive.
   *
   * @param vid The volume identifier of the tape to be unloaded.  Please note
   * that the value of this field is only used for logging purposes.
   * @param drive The tape drive.
   */
  void unloadTape(const std::string &vid, castor::tape::tapeserver::drive::DriveInterface &drive);
  
  /**
   * Dismounts the specified tape.
   *
   * @param vid The volume identifier of the tape to be dismounted.
   */
  void dismountTape(const std::string &vid);
  
  /**
   * Rewinds the specified tape drive.
   *
   * @param drive The tape drive.
   */
  void rewindDrive(castor::tape::tapeserver::drive::DriveInterface &drive);
  
  /**
   * Checks the specified tape on the specified tape drive.
   * This method assumes that the drive has the tape and the tape has been rewound. 
   * It checks the tape label from the VOL1 tape header on the tape against given label
   * and throws an exception in case of labels mismatch. This method leaves tape rewound.
   *  
   * @param drive The tape drive.
   * @param labelToCheck The label for what the tape should be checked for.
   */
  void checkTapeLabel(castor::tape::tapeserver::drive::DriveInterface &drive, const std::string &labelToCheck);
  
  /**
   * Writes the label file with or without logical block protection to the tape
   * depending on useLbp and driveSupportLbp parameters.
   *
   * This method assumes the tape has been rewound.
   *
   * @param drive The tape drive.
   * @param useLbp The configuration parameter for LBP mode.
   * @param driveSupportLbp The detected parameter for the drive.
   */
  void writeTapeLabel(castor::tape::tapeserver::drive::DriveInterface &drive,
    const bool useLbp, const bool driveSupportLbp);
  
  /**
   * Sets the logical block protection mode on the drive
   * depending on useLbp and driveSupportLbp parameters. This method needs to
   * be used to avoid exceptions in setLbp if drive does not support LBP (mhvtl).
   *
   * @param drive The tape drive.
   * @param useLbp The configuration parameter for LBP mode.
   * @param driveSupportLbp The detected parameter for the drive.
   */
  void setLbpMode(castor::tape::tapeserver::drive::DriveInterface &drive,
    const bool useLbp, const bool driveSupportLbp);
  
  /**
   * Detects if the drive supports the logical block protection.
   *
   * @param drive The tape drive.
   * @return The boolean value true if the drive supports LBP or false otherwise.
   */
  bool isDriveSupportLbp(castor::tape::tapeserver::drive::DriveInterface &drive) const;
  
  /**
   * Returns the string representation of the specified boolean value.
   *
   * @param value The boolean value.
   * @return The string representation.
   */
  const char *boolToStr(const bool value);
}; // class TapeLabelCmd

}} // namespace cta::tapeserver::tapelabel
