#pragma once

#include "CtaAdminResponseStream.hpp"
#include "common/dataStructures/TapeDrive.hpp"
#include "common/log/LogContext.hpp"
#include "common/utils/utils.hpp"
#include "frontend/common/AdminCmdOptions.hpp"
#include <list>
#include <unordered_map>
#include "common/dataStructures/MountTypeSerDeser.hpp"
#include "common/dataStructures/DriveStatusSerDeser.hpp"

#include "cta_frontend.pb.h"

namespace cta::cmdline {

class DriveLsResponseStream : public CtaAdminResponseStream {
public:
  DriveLsResponseStream(cta::catalogue::Catalogue& catalogue,
                        cta::Scheduler& scheduler,
                        const frontend::AdminCmdStream& requestMsg,
                        cta::log::LogContext& lc);

  bool isDone() override;
  cta::xrd::Data next() override;
  

private:
  cta::log::LogContext& m_lc;

  std::list<common::dataStructures::TapeDrive> m_tapeDrives;
  std::unordered_map<std::string, std::list<cta::catalogue::DriveConfigCatalogue::DriveConfig>>
    m_tapeDriveNameConfigMap;
  bool m_listAllDrives;
  std::optional<std::string> m_schedulerBackendName;
};

DriveLsResponseStream::DriveLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                             cta::Scheduler& scheduler,
                                             const frontend::AdminCmdStream& requestMsg,
                                             cta::log::LogContext& lc)
    : CtaAdminResponseStream(catalogue, scheduler, requestMsg.getInstanceName()),
      m_lc(lc),
      m_listAllDrives(false) {
  const admin::AdminCmd& admincmd = requestMsg.getAdminCmd();
  using namespace cta::admin;

  cta::frontend::AdminCmdOptions request(admincmd);
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
  driveItem->set_cta_version(dr.ctaVersion ? dr.ctaVersion.value() : "");
  driveItem->set_logical_library(dr.logicalLibrary);
  driveItem->set_drive_name(dr.driveName);
  driveItem->set_scheduler_backend_name(driveSchedulerBackendName);
  driveItem->set_host(dr.host);
  driveItem->set_logical_library_disabled(dr.logicalLibraryDisabled ? dr.logicalLibraryDisabled.value() : false);
  driveItem->set_desired_drive_state(dr.desiredUp ? cta::admin::DriveLsItem::UP : cta::admin::DriveLsItem::DOWN);
  driveItem->set_mount_type(cta::admin::MountTypeToProtobuf(dr.mountType));
  driveItem->set_drive_status(cta::admin::DriveStatusToProtobuf(dr.driveStatus));
  driveItem->set_vid(dr.currentVid ? dr.currentVid.value() : "");
  driveItem->set_tapepool(dr.currentTapePool ? dr.currentTapePool.value() : "");
  driveItem->set_vo(dr.currentVo ? dr.currentVo.value() : "");
  driveItem->set_files_transferred_in_session(dr.filesTransferedInSession ? dr.filesTransferedInSession.value() : 0);
  driveItem->set_bytes_transferred_in_session(dr.bytesTransferedInSession ? dr.bytesTransferedInSession.value() : 0);
  driveItem->set_session_id(dr.sessionId ? dr.sessionId.value() : 0);
  const auto lastUpdateTime = dr.lastModificationLog ? dr.lastModificationLog.value().time : time(nullptr);
  driveItem->set_time_since_last_update(time(nullptr) - lastUpdateTime);
  driveItem->set_current_priority(dr.currentPriority ? dr.currentPriority.value() : 0);
  driveItem->set_current_activity(dr.currentActivity ? dr.currentActivity.value() : "");
  driveItem->set_dev_file_name(dr.devFileName ? dr.devFileName.value() : "");
  driveItem->set_raw_library_slot(dr.rawLibrarySlot ? dr.rawLibrarySlot.value() : "");
  driveItem->set_comment(dr.userComment ? dr.userComment.value() : "");
  driveItem->set_reason(dr.reasonUpDown ? dr.reasonUpDown.value() : "");
  driveItem->set_physical_library(dr.physicalLibraryName ? dr.physicalLibraryName.value() : "");
  driveItem->set_physical_library_disabled(dr.physicalLibraryDisabled ? dr.physicalLibraryDisabled.value() : false);
  if (dr.mountType == cta::common::dataStructures::MountType::Retrieve) {
    driveItem->set_disk_system_name(dr.diskSystemName ? dr.diskSystemName.value() : "");
    driveItem->set_reserved_bytes(dr.reservedBytes ? dr.reservedBytes.value() : 0);
  }
  driveItem->set_session_elapsed_time(dr.sessionElapsedTime ? dr.sessionElapsedTime.value() : 0);

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

}  // namespace cta::cmdline