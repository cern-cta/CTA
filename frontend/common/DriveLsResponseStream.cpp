/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2025 CERN
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

#include "DriveLsResponseStream.hpp"

#include "common/dataStructures/MountTypeSerDeser.hpp"
#include "common/dataStructures/DriveStatusSerDeser.hpp"
#include "frontend/common/AdminCmdOptions.hpp"

namespace cta::frontend {

DriveLsResponseStream::DriveLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                             cta::Scheduler& scheduler,
                                             const std::string& instanceName,
                                             const admin::AdminCmd& adminCmd,
                                             cta::log::LogContext& lc)
    : CtaAdminResponseStream(catalogue, scheduler, instanceName),
      m_lc(lc),
      m_listAllDrives(false) {
  using namespace cta::admin;

  cta::frontend::AdminCmdOptions request(adminCmd);
  bool has_any = false;

  // Get drives and drive configs
  m_tapeDrives = m_catalogue.DriveState()->getTapeDrives();
  auto driveConfigs = m_catalogue.DriveConfig()->getTapeDriveConfigs();

  // Convert to searchable map
  for (const auto& config : driveConfigs) {
    m_tapeDriveNameConfigMap[config.tapeDriveName].emplace_back(config);
  }

  // Get scheduler backend name
  m_schedulerBackendName = m_scheduler.getSchedulerBackendName();

  // Check if --all is specified
  m_listAllDrives = request.has_flag(OptionBoolean::ALL);

  // Handle drive regex filter and scheduler backend filter in a single pass
  auto driveRegexOpt = request.getOptional(OptionString::DRIVE, &has_any);
  std::unique_ptr<utils::Regex> driveRegex;

  if (driveRegexOpt) {
    std::string driveRegexStr = '^' + driveRegexOpt.value() + '$';
    driveRegex = std::make_unique<utils::Regex>(driveRegexStr.c_str());
  }

  // Apply both filters in a single pass
  for (auto dr_it = m_tapeDrives.begin(); dr_it != m_tapeDrives.end();) {
    bool shouldKeep = true;

    // Apply regex filter if specified
    if (driveRegex && !driveRegex->has_match(dr_it->driveName)) {
      shouldKeep = false;
    }

    // Apply scheduler backend filter if not listing all drives
    if (shouldKeep && !m_listAllDrives) {
      const auto& driveConfigs = m_tapeDriveNameConfigMap[dr_it->driveName];

      // Extract the SchedulerBackendName configuration if it exists
      std::string driveSchedulerBackendName = "unknown";
      auto config_it =
        std::find_if(driveConfigs.begin(),
                     driveConfigs.end(),
                     [&driveSchedulerBackendName](const cta::catalogue::DriveConfigCatalogue::DriveConfig& config) {
                       if (config.keyName == "SchedulerBackendName") {
                         driveSchedulerBackendName = config.value;
                         return true;
                       }
                       return false;
                     });
      if (config_it == driveConfigs.end()) {
        m_lc.log(cta::log::ERR,
                 "DriveLsStream::fillBuffer could not find SchedulerBackendName configuration for drive " +
                   dr_it->driveName);
      }

      if (m_schedulerBackendName.value_or("") != driveSchedulerBackendName) {
        shouldKeep = false;
      }
    }

    if (shouldKeep) {
      ++dr_it;
    } else {
      auto erase_it = dr_it;
      ++dr_it;
      m_tapeDrives.erase(erase_it);
    }
  }

  // Check if any drives match the regex filter (only if regex was specified)
  if (driveRegexOpt && m_tapeDrives.empty()) {
    throw cta::exception::UserError(std::string("Drive ") + driveRegexOpt.value() + " not found.");
  }
}

bool DriveLsResponseStream::isDone() {
  return m_tapeDrives.empty();
}

cta::xrd::Data DriveLsResponseStream::next() {
  if (isDone()) {
    throw std::runtime_error("Stream is exhausted");
  }

  const auto dr = m_tapeDrives.front();
  m_tapeDrives.pop_front();

  const auto& driveConfigs = m_tapeDriveNameConfigMap[dr.driveName];

  // Extract the SchedulerBackendName configuration if it exists
  std::string driveSchedulerBackendName = "unknown";
  auto it = std::find_if(driveConfigs.begin(),
                         driveConfigs.end(),
                         [&driveSchedulerBackendName](const cta::catalogue::DriveConfigCatalogue::DriveConfig& config) {
                           if (config.keyName == "SchedulerBackendName") {
                             driveSchedulerBackendName = config.value;
                             return true;
                           }
                           return false;
                         });

  if (it == driveConfigs.end()) {
    m_lc.log(cta::log::ERR,
             "DriveLsResponseStream::next could not find SchedulerBackendName configuration for drive " + dr.driveName);
  }

  cta::xrd::Data data;
  auto driveItem = data.mutable_drls_item();

  // fillDriveItem(dr, driveItem, m_instanceName, driveSchedulerBackendName, driveConfigs);
  driveItem->set_instance_name(m_instanceName);
  driveItem->set_cta_version(dr.ctaVersion.value_or(""));
  driveItem->set_logical_library(dr.logicalLibrary);
  driveItem->set_drive_name(dr.driveName);
  driveItem->set_scheduler_backend_name(driveSchedulerBackendName);
  driveItem->set_host(dr.host);
  driveItem->set_logical_library_disabled(dr.logicalLibraryDisabled.value_or(false));
  driveItem->set_desired_drive_state(dr.desiredUp.has_value() ? cta::admin::DriveLsItem::UP : cta::admin::DriveLsItem::DOWN);
  driveItem->set_mount_type(cta::admin::MountTypeToProtobuf(dr.mountType));
  driveItem->set_drive_status(cta::admin::DriveStatusToProtobuf(dr.driveStatus));
  driveItem->set_vid(dr.currentVid.value_or(""));
  driveItem->set_tapepool(dr.currentTapePool.value_or(""));
  driveItem->set_vo(dr.currentVo.value_or(""));
  driveItem->set_files_transferred_in_session(ddr.filesTransferedInSession.value_or(0));
  driveItem->set_bytes_transferred_in_session(ddr.bytesTransferedInSession.value_or(0));
  driveItem->set_session_id(dr.sessionId.value_or(0));
  const auto lastUpdateTime = dr.lastModificationLog.has_value() ? dr.lastModificationLog.value().time : time(nullptr);
  driveItem->set_time_since_last_update(time(nullptr) - lastUpdateTime);
  driveItem->set_current_priority(dr.currentPriority.value_or());
  driveItem->set_current_activity(dr.currentActivity.value_or(""));
  driveItem->set_dev_file_name(dr.devFileName.value_or(""));
  driveItem->set_raw_library_slot(dr.rawLibrarySlot.value_or(""));
  driveItem->set_comment(dr.userComment.value_or(""));
  driveItem->set_reason(dr.reasonUpDown.value_or(""));
  driveItem->set_physical_library(dr.physicalLibraryName.value_or(""));
  driveItem->set_physical_library_disabled(dr.physicalLibraryDisabled.value_or(false));
  if (dr.mountType == cta::common::dataStructures::MountType::Retrieve) {
    driveItem->set_disk_system_name(dr.diskSystemName.value_or(""));
    driveItem->set_reserved_bytes(dr.reservedBytes.value_or(0));
  }
  driveItem->set_session_elapsed_time(dr.sessionElapsedTime.value_or(0));

  auto driveConfig = driveItem->mutable_drive_config();

  for (const auto& config : driveConfigs) {
    auto driveConfigItemProto = driveConfig->Add();
    driveConfigItemProto->set_key(config.keyName);
    driveConfigItemProto->set_category(config.category);
    driveConfigItemProto->set_value(config.value);
    driveConfigItemProto->set_source(config.source);
  }

  // set the time spent in the current state
  uint64_t drive_time = time(nullptr);

  switch (dr.driveStatus) {
    using cta::common::dataStructures::DriveStatus;
      // clang-format off
    case DriveStatus::Probing:           drive_time -= dr.probeStartTime.value();    break;
    case DriveStatus::Up:                drive_time -= dr.downOrUpStartTime.value(); break;
    case DriveStatus::Down:              drive_time -= dr.downOrUpStartTime.value(); break;
    case DriveStatus::Starting:          drive_time -= dr.startStartTime.value();    break;
    case DriveStatus::Mounting:          drive_time -= dr.mountStartTime.value();    break;
    case DriveStatus::Transferring:      drive_time -= dr.transferStartTime.value(); break;
    case DriveStatus::CleaningUp:        drive_time -= dr.cleanupStartTime.value();  break;
    case DriveStatus::Unloading:         drive_time -= dr.unloadStartTime.value();   break;
    case DriveStatus::Unmounting:        drive_time -= dr.unmountStartTime.value();  break;
    case DriveStatus::DrainingToDisk:    drive_time -= dr.drainingStartTime.value(); break;
    case DriveStatus::Shutdown:          drive_time -= dr.shutdownTime.value();      break;
    case DriveStatus::Unknown:                                                       break;
      // clang-format on
  }
  driveItem->set_drive_status_since(drive_time);

  return data;
}

}  // namespace cta::frontend
