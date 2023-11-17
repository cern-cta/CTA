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
#include <string>
#include <map>
#include <type_traits>
#include <limits>
#include "common/log/DummyLogger.hpp"
#include "common/exception/Exception.hpp"
#include "common/SourcedParameter.hpp"
#include "FetchReportOrFlushLimits.hpp"
#include "Tpconfig.hpp"

namespace cta::tape::daemon {
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
    "taped", "LogMask", "INFO", "Compile time default"};
  /// Path to the file describing the tape drives (TPCONFIG)
  cta::SourcedParameter<std::string> tpConfigPath{
    "taped" , "TpConfigPath", "/etc/cta/TPCONFIG", "Compile time default"};
  /// Extracted drives configuration.
  Tpconfig driveConfigs;
  //----------------------------------------------------------------------------
  // Memory management
  //----------------------------------------------------------------------------
  /// Memory buffer size in bytes (with a default of 5MB).
  cta::SourcedParameter<uint64_t> bufferSizeBytes{
    "taped", "BufferSizeBytes", 5*1024*1024, "Compile time default"};
  /// Memory buffer count per drive. Default 5000.
  cta::SourcedParameter<uint64_t> bufferCount{
    "taped", "BufferCount", 5000, "Compile time default"};
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
    "taped", "MountCriteria", {50L*1000*1000*1000, 10000}, "Compile time default"};
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
    "taped", "UseRAO", "yes", "Compile time default"};
  /// RAO type of algorithm
  cta::SourcedParameter<std::string> raoLtoAlgorithm{
    "taped", "RAOLTOAlgorithm","sltf","Compile time default"};
  cta::SourcedParameter<std::string> raoLtoOptions {
    "taped", "RAOLTOAlgorithmOptions","cost_heuristic_name:cta","Compile time default"
  };
  //----------------------------------------------------------------------------
  // External script to get free disk space in the Retrieve buffer
  //----------------------------------------------------------------------------
  cta::SourcedParameter<std::string> externalFreeDiskSpaceScript {
    "taped", "externalFreeDiskSpaceScript","", "Compile time default"
  };
  //----------------------------------------------------------------------------
  // Watchdog: parameters for timeouts in various situations.
  //----------------------------------------------------------------------------
  /// Maximum time allowed to determine a drive is ready.
  cta::SourcedParameter<time_t> wdCheckMaxSecs{
    "taped", "WatchdogCheckMaxSecs", 2 * 60, "Compile time default"};
  /// Maximum time allowed to schedule a single mount.
  cta::SourcedParameter<time_t> wdScheduleMaxSecs{
    "taped", "WatchdogScheduleMaxSecs", 5 * 60, "Compile time default"};
  /// Maximum time allowed to mount a tape.
  cta::SourcedParameter<time_t> wdMountMaxSecs{
    "taped", "WatchdogMountMaxSecs", 10 * 60, "Compile time default"};
  /// Maximum time allowed to unmount a tape.
  cta::SourcedParameter<time_t> wdUnmountMaxSecs{
    "taped", "WatchdogUnmountMaxSecs", 10 * 60, "Compile time default"};
  /// Maximum time allowed to drain a file to disk during retrieve.
  cta::SourcedParameter<time_t> wdDrainMaxSecs{
    "taped", "WatchdogDrainMaxSecs", 30 * 60, "Compile time default"};
  /// Maximum time allowed to shutdown of a tape session.
  cta::SourcedParameter<time_t> wdShutdownMaxSecs{
    "taped", "WatchdogShutdownMaxSecs", 15 * 60, "Compile time default"};
  /// Maximum time allowed after mounting without a single tape block move
  cta::SourcedParameter<time_t> wdNoBlockMoveMaxSecs{
    "taped", "WatchdogNoBlockMoveMaxSecs", 10 * 60, "Compile time default"};
  /// Time to wait after scheduling came up idle
  cta::SourcedParameter<time_t> wdIdleSessionTimer{
    "taped", "WatchdogIdleSessionTimer", 10, "Compile time default"};
  /// Time to wait after which the tape server stops trying to get the next mount
  cta::SourcedParameter<time_t> wdGlobalLockAcqMaxSecs{
    "taped", "WatchdogGlobalLockAcqMaxSecs", 15 * 60, "Compile time default"};
  //----------------------------------------------------------------------------
  // The central storage access configuration
  //---------------------------------------------------------------------------- 
  /// URL of the object store.
  cta::SourcedParameter<std::string> backendPath{
    "ObjectStore", "BackendPath"};
  /// Path to the CTA catalogue config file
  cta::SourcedParameter<std::string> fileCatalogConfigFile{
    "taped", "CatalogueConfigFile", "/etc/cta/cta-catalogue.conf", "Compile time default"};

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
  cta::SourcedParameter<std::string> useRepackManagement {
    "taped","UseRepackManagement","yes","Compile time default"
  };
  
  /// Usage of MaintenanceProcess for repack-related operations, Garbage collection and disk reporting
  cta::SourcedParameter<std::string> useMaintenanceProcess {
    "taped","UseMaintenanceProcess","yes","Compile time default"
  };
  

  /// Max number of repacks to promote to ToExpand state.
  cta::SourcedParameter<std::uint64_t> repackMaxRequestsToExpand{
      "taped", "RepackMaxRequestsToExpand", 2, "Compile time default"};

  //----------------------------------------------------------------------------
  // Tape load actions
  //----------------------------------------------------------------------------
  cta::SourcedParameter<uint32_t> tapeLoadTimeout {
    "taped", "TapeLoadTimeout",300,"Compile time default"
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

  //----------------------------------------------------------------------------
  // RMC Connection Options
  //----------------------------------------------------------------------------  
  cta::SourcedParameter<uint16_t> rmcPort {
    "taped", "RmcPort", 5014, "Compile time default"
  };

  cta::SourcedParameter<uint32_t> rmcNetTimeout {
    "taped", "RmcNetTimeout", 600, "Compile time default"
  };

  cta::SourcedParameter<uint32_t> rmcRequestAttempts {
    "taped", "RmcRequestAttempts", 10, "Compile time default"
  };

private:
  /** A private dummy logger which will simplify the implementation of the 
   * functions (just unconditionally log things). */
  static cta::log::DummyLogger gDummyLogger;
} ;
} // namespace cta::tape::daemon
