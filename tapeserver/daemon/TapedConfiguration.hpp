/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once
#include <string>
#include <map>
#include <type_traits>
#include <limits>
#include "common/log/DummyLogger.hpp"
#include "common/exception/Exception.hpp"
#include "common/SourcedParameter.hpp"
#include "FetchReportOrFlushLimits.hpp"
#include "Tpconfig.hpp"

namespace cta {
namespace tape {
namespace daemon {
/**
 * Class containing all the parameters needed by the watchdog process
 * to spawn a transfer session per drive.
 */
struct TapedConfiguration {
  static TapedConfiguration createFromCtaConf(
          const std::string & generalConfigPath,
          cta::log::Logger & log = gDummyLogger);
  
  //----------------------------------------------------------------------------
  // The actual parameters:
  //----------------------------------------------------------------------------
  // Basics: tp config
  //----------------------------------------------------------------------------
  /// The user name of the cta-taped daemon process
  cta::SourcedParameter<std::string> daemonUserName{
    "taped", "DaemonUserName", "cta", "Compile time default"};
  /// The group name of the cta-taped daemon process
  cta::SourcedParameter<std::string> daemonGroupName{
    "taped", "DaemonGroupName", "tape", "Compile time default"};
  /// The log mask.  Logs with a level lower than this value will be masked.
  cta::SourcedParameter<std::string> logMask{
    "taped", "LogMask", "DEBUG", "Compile time default"};
  /// Path to the file describing the tape drives (TPCONFIG)
  cta::SourcedParameter<std::string> tpConfigPath{
    "taped" , "TpConfigPath", "/etc/cta/TPCONFIG", "Compile time default"};
  /// Extracted drives configuration.
  Tpconfig driveConfigs;
  //----------------------------------------------------------------------------
  // Memory management
  //----------------------------------------------------------------------------
  /// Memory buffer size in bytes (with a default of 5MB). TODO-switch to 32MB once validated in CASTOR.
  cta::SourcedParameter<uint64_t> bufferSizeBytes{
    "taped", "BufferSizeBytes", 5*1024*1024, "Compile time default"};
  /// Memory buffer count per drive. There is no default to this one.
  cta::SourcedParameter<uint64_t> bufferCount{
    "taped", "BufferCount"};
  //----------------------------------------------------------------------------
  // Batched metadata access and tape write flush parameters 
  //----------------------------------------------------------------------------
  /// The fetch size for archive requests 
  cta::SourcedParameter<FetchReportOrFlushLimits> archiveFetchBytesFiles{
    "taped", "ArchiveFetchBytesFiles", {80L*1000*1000*1000, 4000}, "Compile time default"};
  /// The flush to tape criteria for archiving
  cta::SourcedParameter<FetchReportOrFlushLimits> archiveFlushBytesFiles{
    "taped", "ArchiveFlushBytesFiles", {32L*1000*1000*1000, 200}, "Compile time default"};
  /// The fetch and report size for retrieve requests 
  cta::SourcedParameter<FetchReportOrFlushLimits> retrieveFetchBytesFiles{
    "taped", "RetrieveFetchBytesFiles", {80L*1000*1000*1000, 4000}, "Compile time default"};
  //----------------------------------------------------------------------------
  // Scheduling limits
  //----------------------------------------------------------------------------
  cta::SourcedParameter<FetchReportOrFlushLimits> mountCriteria{
    "taped", "MountCriteria", {80L*1000*1000*1000, 500}, "Compile time default"};
  //----------------------------------------------------------------------------
  // Disk file access parameters
  //----------------------------------------------------------------------------
  /// Number of disk threads. This is the number of parallel file transfers.
  cta::SourcedParameter<uint64_t> nbDiskThreads{
    "taped", "NbDiskThreads", 10, "Compile time default"};
  //----------------------------------------------------------------------------
  // Recommended Access Order usage
  //----------------------------------------------------------------------------
  /// Usage of Recommended Access Order for file recall
  cta::SourcedParameter<std::string> useRAO{
    "taped", "UseRAO", "no", "Compile time default"};
  /// RAO type of algorithm
  cta::SourcedParameter<std::string> raoLtoAlgorithm{
    "taped", "RAOLTOAlgorithm","linear","Compile time default"};
  cta::SourcedParameter<std::string> raoLtoOptions {
    "taped", "RAOLTOAlgorithmOptions","","Compile time default"
  };
  //----------------------------------------------------------------------------
  // Fetch EOS Free space operator's script
  //----------------------------------------------------------------------------
  cta::SourcedParameter<std::string> fetchEosFreeSpaceScript {
    "taped", "FetchEosFreeSpaceScript","", "Compile time default"
  };
  //----------------------------------------------------------------------------
  // Watchdog: parameters for timeouts in various situations.
  //----------------------------------------------------------------------------
  /// Maximum time allowed to complete a single mount scheduling.
  cta::SourcedParameter<time_t> wdScheduleMaxSecs{
    "taped", "WatchdogScheduleMaxSecs", 60, "Compile time default"};
  /// Maximum time allowed to complete mount a tape.
  cta::SourcedParameter<time_t> wdMountMaxSecs{
    "taped", "WatchdogMountMaxSecs", 900, "Compile time default"};
  /// Maximum time allowed after mounting without a single tape block move
  cta::SourcedParameter<time_t> wdNoBlockMoveMaxSecs{
    "taped", "WatchdogNoBlockMoveMaxSecs", 1800, "Compile time default"};
  /// Time to wait after scheduling came up idle
  cta::SourcedParameter<time_t> wdIdleSessionTimer{
    "taped", "WatchdogIdleSessionTimer", 10, "Compile time default"};
  //----------------------------------------------------------------------------
  // The central storage access configuration
  //---------------------------------------------------------------------------- 
  /// URL of the object store.
  cta::SourcedParameter<std::string> backendPath{
    "ObjectStore", "BackendPath"};
  /// Path to the file catalog config file
  cta::SourcedParameter<std::string> fileCatalogConfigFile{
    "general", "FileCatalogConfigFile", "/etc/cta/cta-catalogue.conf", "Compile time default"};
    
  //----------------------------------------------------------------------------
  // The authentication configuration
  //---------------------------------------------------------------------------- 
  /// The authentication protocol
  cta::SourcedParameter<std::string> authenticationProtocol{
    "environment", "XrdSecPROTOCOL"};
  /// The authentication protocol
  cta::SourcedParameter<std::string> authenticationSSSKeytab{
    "environment", "XrdSecSSSKT"};
    
  //----------------------------------------------------------------------------
  // Maintenance process configuration
  //----------------------------------------------------------------------------
  /// Usage of RepackRequestManager for repack-related operations
  cta::SourcedParameter<std::string> disableRepackManagement {
    "taped","DisableRepackManagement","no","Compile time default"
  };
  
  /// Usage of MaintenanceProcess for repack-related operations, Garbage collection and disk reporting
  cta::SourcedParameter<std::string> disableMaintenanceProcess {
    "taped","DisableMaintenanceProcess","no","Compile time default"
  };
  
  //----------------------------------------------------------------------------
  // Tape load actions
  //----------------------------------------------------------------------------
  cta::SourcedParameter<uint32_t> tapeLoadTimeout {
    "taped", "TapeLoadTimeout",60,"Compile time default"
  };

  //----------------------------------------------------------------------------
  // Tape encryption support
  //----------------------------------------------------------------------------

  cta::SourcedParameter<std::string> useEncryption {
    "taped", "UseEncryption","yes", "Compile time default"
  };

  cta::SourcedParameter<std::string> externalEncryptionKeyScript {
    "taped", "externalEncryptionKeyScript","","Compile time default"
  };
  
private:
  /** A private dummy logger which will simplify the implementation of the 
   * functions (just unconditionally log things). */
  static cta::log::DummyLogger gDummyLogger;
} ;
}
}
} // namespace cta::tape::daemon
