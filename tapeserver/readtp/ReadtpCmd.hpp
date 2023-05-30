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

#include "common/dataStructures/LabelFormat.hpp"
#include "common/log/DummyLogger.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/StdoutLogger.hpp"
#include "common/processCap/ProcessCap.hpp"
#include "disk/DiskFile.hpp"
#include "mediachanger/MediaChangerFacade.hpp"
#include "tapeserver/castor/tape/tapeserver/daemon/EncryptionControl.hpp"
#include "tapeserver/castor/tape/tapeserver/daemon/VolumeInfo.hpp"
#include "tapeserver/castor/tape/tapeserver/drive/DriveGeneric.hpp"
#include "tapeserver/castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "tapeserver/daemon/Tpconfig.hpp"
#include "tapeserver/readtp/CmdLineTool.hpp"
#include "tapeserver/readtp/ReadtpCmdLineArgs.hpp"
#include "tapeserver/readtp/TapeFseqRange.hpp"
#include "tapeserver/readtp/TapeFseqRangeListSequence.hpp"

namespace cta {

namespace catalogue {
class Catalogue;
}
namespace common::dataStructures {
class Tape;
}

namespace tapeserver {
namespace readtp {

/**
 * Command-line tool for reading files from a CTA tape.
 */
class ReadtpCmd: public CmdLineTool {
public:

  /**
   * Constructor.
   *
   * @param inStream Standard input stream.
   * @param outStream Standard output stream.
   * @param errStream Standard error stream.
   * @param log The object representing the API of the CTA logging system.
   * @param mc Interface to the media changer.
   */
  ReadtpCmd(std::istream &inStream, std::ostream &outStream,
    std::ostream &errStream, cta::log::StdoutLogger &log,
    cta::log::DummyLogger &dummyLog,
    cta::mediachanger::MediaChangerFacade &mc);

  /**
   * Destructor.
   */
  ~ReadtpCmd() noexcept;

private:

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
   * Sets internal configuration parameters to be used for reading.
   * It reads drive and library parameters from /etc/cta/TPCONFIG and catalogue
   * login parameters from /etc/cta/cta-catalogue.conf.
   *
   * @param username The name of the user running the command-line tool.
   * @param cmdLineArgs The arguments parsed from the command line.
   */
  void readAndSetConfiguration(const std::string &userName, const ReadtpCmdLineArgs &cmdLineArgs);

  /**
   * Checks if the tape is encrypted
   */
  bool isEncrypted(const cta::common::dataStructures::Tape &tape) const;

  /**
  * Configures encryption to be able to read from an encrypted tape
  *
  * @param volInfo The volume info of the tape to be mounted.
  * @param drive The tape drive.
  */
  void configureEncryption(castor::tape::tapeserver::daemon::VolumeInfo &volInfo,
                           castor::tape::tapeserver::drive::DriveInterface &drive);

  /**
  * Disable encryption
  *
  * @param drive The tape drive.
  */
  void disableEncryption(castor::tape::tapeserver::drive::DriveInterface &drive);

  /**
   * Reads a file line by line, strips comments and returns a list of the file lines.
   *
   * @param filename The name of the file to be read.
   * @return A list of line strings after stripping comments.
   */
  std::list<std::string> readListFromFile(const std::string &filename) const;

  /**
   * Returns the next destination file URL, or file:///dev/null if all destination files have been used.
   *
   * @return The URL of the next destination file.
   */
  std::string getNextDestinationUrl();

  /**
   * Sets the capabilities of the process and logs the result.
   *
   * @param capabilities The string representation of the capabilities.
   */
  void setProcessCapabilities(const std::string &capabilities);

  /**
   * Returns a Drive object representing the tape drive to be used to read
   * the tape.
   *
   * @return The drive object.
   */
  std::unique_ptr<castor::tape::tapeserver::drive::DriveInterface> createDrive();

  /**
   * Detects if the drive supports the logical block protection.
   *
   * @param drive The tape drive.
   * @return The boolean value true if the drive supports LBP or false otherwise.
   */
  bool isDriveSupportLbp(castor::tape::tapeserver::drive::DriveInterface &drive) const;

  /**
   * Sets the logical block protection mode on the drive
   * depending on useLbp and driveSupportLbp parameters. This method needs to
   * be used to avoid exceptions in setLbp if drive does not support LBP (mhvtl).
   *
   * @param drive The tape drive.
   */
  void setLbpMode(castor::tape::tapeserver::drive::DriveInterface &drive);

   /**
   * Mounts the tape to be read.
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
   * Read the files requested from tape
   *
   * @param drive Object representing the drive hardware.
   */
  void readTapeFiles(castor::tape::tapeserver::drive::DriveInterface &drive);

  /**
   * Read a specific file from tape
   * @param drive Object representing the drive hardware.
   * @param fSeq The tape file fSeq.
   */
  void readTapeFile(castor::tape::tapeserver::drive::DriveInterface &drive, const uint64_t &fSeq,
                    cta::disk::WriteFile &wf, const castor::tape::tapeserver::daemon::VolumeInfo &volInfo);


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
   * Returns the string representation of the specified boolean value.
   *
   * @param value The boolean value.
   * @return The string representation.
   */
  const char *boolToStr(const bool value);

  /**
   * The object representing the API of the CTA logging system.
   */
  cta::log::StdoutLogger  &m_log;

/**
   *Dummy logger for the catalogue
   */
  cta::log::DummyLogger &m_dummyLog;
  
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
   * The tape VID to read.
   */
  std::string m_vid;
  
  /**
   * Path to the xroot private key file.
   */
  std::string m_xrootPrivateKeyPath;

  /**
   * The iterator of destination urls the data read is sent to
   */
  std::list<std::string> m_destinationFiles;

  /**
   * The file fSeq to read.
   */
  TapeFseqRangeList m_fSeqRangeList;

  /**
   * The object representing the media changer.
   */
  cta::mediachanger::MediaChangerFacade &m_mc;

  /**
   * The boolean variable which determinate logical block protection usage by
   * readtp commands. Hard coded when we create the class.
   */
  bool m_useLbp;

  /**
   * The boolean variable to store if drive support LBP.
   */
  bool m_driveSupportLbp;

  /**
   * Number of files read successsfully.
   */
  uint64_t m_nbSuccessReads;

  /**
   * Number of failed reads.
   */
  uint64_t m_nbFailedReads;

  /**
  * Encryption helper object 
  */
  std::unique_ptr<castor::tape::tapeserver::daemon::EncryptionControl> m_encryptionControl;

  /**
   * Encryption on/off
   */
  bool m_isTapeEncrypted;


}; // class ReadtpCmd

CTA_GENERATE_EXCEPTION_CLASS(NoSuchFSeqException);

} // namespace readtp
} // namespace tapeserver
} // namespace cta
