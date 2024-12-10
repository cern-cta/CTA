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

#include <algorithm>
#include <list>
#include <string>
#include <utility>

#include "catalogue/Catalogue.hpp"
#include "common/dataStructures/DriveStatusSerDeser.hpp"
#include "common/dataStructures/MountTypeSerDeser.hpp"
#include "xroot_plugins/XrdCtaStream.hpp"

namespace cta::xrd {

/*!
 * Converts list of drive configuration entries of all drives into searchable structure
 */
std::unordered_map<std::string, std::list<cta::catalogue::DriveConfigCatalogue::DriveConfig>>
convertToMap(const std::list<cta::catalogue::DriveConfigCatalogue::DriveConfig>& driveConfigs) {
  std::unordered_map<std::string, std::list<cta::catalogue::DriveConfigCatalogue::DriveConfig>> driveConfigMap;

  for (const auto& config : driveConfigs) {
    driveConfigMap[config.tapeDriveName].emplace_back(config);
  }

  return driveConfigMap;
}

/*!
 * Stream object which implements "tapepool ls" command
 */
class DriveLsStream : public XrdCtaStream {
public:
  /*!
   * Constructor
   *
   * @param[in]    requestMsg    RequestMessage containing command-line arguments
   * @param[in]    catalogue     CTA Catalogue
   * @param[in]    scheduler     CTA Scheduler
   */
  DriveLsStream(const frontend::AdminCmdStream& requestMsg,
                cta::catalogue::Catalogue& catalogue,
                cta::Scheduler& scheduler,
                log::LogContext& lc);

private:
  /*!
   * Can we close the stream?
   */
  bool isDone() const override { return m_tapeDrives.empty(); }

  /*!
   * Fill the buffer
   */
  int fillBuffer(XrdSsiPb::OStreamBuffer<Data>* streambuf) override;
  bool listAllDrives = false;
  std::optional<std::string> m_schedulerBackendName;
  cta::log::LogContext m_lc;

  static constexpr const char* const LOG_SUFFIX = "DriveLsStream";  //!< Identifier for log messages

  std::list<common::dataStructures::TapeDrive> m_tapeDrives;
  std::unordered_map<std::string, std::list<cta::catalogue::DriveConfigCatalogue::DriveConfig>>
    m_tapeDriveNameConfigMap;
};

DriveLsStream::DriveLsStream(const frontend::AdminCmdStream& requestMsg,
                             cta::catalogue::Catalogue& catalogue,
                             cta::Scheduler& scheduler,
                             log::LogContext& lc)
    : XrdCtaStream(catalogue, scheduler),
      m_lc(lc),
      m_tapeDrives(m_catalogue.DriveState()->getTapeDrives()),
      m_tapeDriveNameConfigMap(convertToMap(m_catalogue.DriveConfig()->getTapeDriveConfigs())) {
  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "DriveLsStream() constructor");

  // Check if --all is specified
  listAllDrives = requestMsg.has_flag(cta::admin::OptionBoolean::ALL);
  m_schedulerBackendName = scheduler.getSchedulerBackendName();
  if (!m_schedulerBackendName) {
    XrdSsiPb::Log::Msg(
      XrdSsiPb::Log::ERROR,
      LOG_SUFFIX,
      "DriveLsStream constructor, the cta.scheduler_backend_name is not set in the frontend configuration.");
  }

  auto driveRegexOpt = requestMsg.getOptional(cta::admin::OptionString::DRIVE);

  // Dump all drives unless we specified a drive
  if (driveRegexOpt) {
    std::string driveRegexStr = '^' + driveRegexOpt.value() + '$';
    utils::Regex driveRegex(driveRegexStr.c_str());

    // Remove non-matching drives from the list
    for (auto dr_it = m_tapeDrives.begin(); dr_it != m_tapeDrives.end();) {
      if (driveRegex.has_match(dr_it->driveName)) {
        ++dr_it;
      } else {
        auto erase_it = dr_it;
        ++dr_it;
        m_tapeDrives.erase(erase_it);
      }
    }

    if (m_tapeDrives.empty()) {
      throw exception::UserError(std::string("Drive ") + driveRegexOpt.value() + " not found.");
    }
  }
}

int DriveLsStream::fillBuffer(XrdSsiPb::OStreamBuffer<Data>* streambuf) {
  for (bool is_buffer_full = false; !m_tapeDrives.empty() && !is_buffer_full; m_tapeDrives.pop_front()) {
    Data record;

    const auto dr = m_tapeDrives.front();
    const auto& driveConfigs = m_tapeDriveNameConfigMap[dr.driveName];

    // Extract the SchedulerBackendName configuration if it exists
    std::string driveSchedulerBackendName = "unknown";
    auto it =
      std::find_if(driveConfigs.begin(),
                   driveConfigs.end(),
                   [&driveSchedulerBackendName](const cta::catalogue::DriveConfigCatalogue::DriveConfig& config) {
                     if (config.keyName == "SchedulerBackendName") {
                       driveSchedulerBackendName = config.value;
                       return true;
                     }
                     return false;
                   });

    if (it == driveConfigs.end()) {
      XrdSsiPb::Log::Msg(XrdSsiPb::Log::ERROR,
                         LOG_SUFFIX,
                         "DriveLsStream::fillBuffer could not find SchedulerBackendName configuration for drive" +
                           dr.driveName);
    }

    if (!listAllDrives && m_schedulerBackendName.value() != driveSchedulerBackendName) {
      continue;
    }
    auto dr_item = record.mutable_drls_item();

    dr_item->set_cta_version(dr.ctaVersion ? dr.ctaVersion.value() : "");
    dr_item->set_logical_library(dr.logicalLibrary);
    dr_item->set_drive_name(dr.driveName);
    dr_item->set_scheduler_backend_name(driveSchedulerBackendName);
    dr_item->set_host(dr.host);
    dr_item->set_logical_library_disabled(dr.logicalLibraryDisabled ? dr.logicalLibraryDisabled.value() : false);
    dr_item->set_desired_drive_state(dr.desiredUp ? cta::admin::DriveLsItem::UP : cta::admin::DriveLsItem::DOWN);
    dr_item->set_mount_type(cta::admin::MountTypeToProtobuf(dr.mountType));
    dr_item->set_drive_status(cta::admin::DriveStatusToProtobuf(dr.driveStatus));
    dr_item->set_vid(dr.currentVid ? dr.currentVid.value() : "");
    dr_item->set_tapepool(dr.currentTapePool ? dr.currentTapePool.value() : "");
    dr_item->set_vo(dr.currentVo ? dr.currentVo.value() : "");
    dr_item->set_files_transferred_in_session(dr.filesTransferedInSession ? dr.filesTransferedInSession.value() : 0);
    dr_item->set_bytes_transferred_in_session(dr.bytesTransferedInSession ? dr.bytesTransferedInSession.value() : 0);
    dr_item->set_session_id(dr.sessionId ? dr.sessionId.value() : 0);
    const auto lastUpdateTime = dr.lastModificationLog ? dr.lastModificationLog.value().time : time(nullptr);
    dr_item->set_time_since_last_update(time(nullptr) - lastUpdateTime);
    dr_item->set_current_priority(dr.currentPriority ? dr.currentPriority.value() : 0);
    dr_item->set_current_activity(dr.currentActivity ? dr.currentActivity.value() : "");
    dr_item->set_dev_file_name(dr.devFileName ? dr.devFileName.value() : "");
    dr_item->set_raw_library_slot(dr.rawLibrarySlot ? dr.rawLibrarySlot.value() : "");
    dr_item->set_comment(dr.userComment ? dr.userComment.value() : "");
    dr_item->set_reason(dr.reasonUpDown ? dr.reasonUpDown.value() : "");
    dr_item->set_physical_library(dr.physicalLibraryName ? dr.physicalLibraryName.value() : "");
    dr_item->set_physical_library_disabled(dr.physicalLibraryDisabled ? dr.physicalLibraryDisabled.value() : false);
    if (dr.mountType == cta::common::dataStructures::MountType::Retrieve) {
      dr_item->set_disk_system_name(dr.diskSystemName ? dr.diskSystemName.value() : "");
      dr_item->set_reserved_bytes(dr.reservedBytes ? dr.reservedBytes.value() : 0);
    }
    dr_item->set_session_elapsed_time(dr.sessionElapsedTime ? dr.sessionElapsedTime.value() : 0);

    auto driveConfig = dr_item->mutable_drive_config();

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
    dr_item->set_drive_status_since(drive_time);

    is_buffer_full = streambuf->Push(record);
  }
  return streambuf->Size();
}

}  // namespace cta::xrd
