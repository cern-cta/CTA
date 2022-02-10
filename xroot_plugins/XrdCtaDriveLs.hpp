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

#include <algorithm>
#include <list>
#include <string>
#include <utility>

#include <common/dataStructures/DriveStatusSerDeser.hpp>
#include <common/dataStructures/MountTypeSerDeser.hpp>
#include <xroot_plugins/XrdCtaStream.hpp>
#include <xroot_plugins/XrdSsiCtaRequestMessage.hpp>

namespace cta { namespace xrd {

/*!
 * Stream object which implements "tapepool ls" command
 */
class DriveLsStream: public XrdCtaStream{
 public:
  /*!
   * Constructor
   *
   * @param[in]    requestMsg    RequestMessage containing command-line arguments
   * @param[in]    catalogue     CTA Catalogue
   * @param[in]    scheduler     CTA Scheduler
   */
  DriveLsStream(const RequestMessage &requestMsg, cta::catalogue::Catalogue &catalogue, cta::Scheduler &scheduler,
    const cta::common::dataStructures::SecurityIdentity &clientID, log::LogContext &lc);

 private:
  /*!
   * Can we close the stream?
   */
  bool isDone() const override {
    return m_tapeDrives.empty();
  }

  /*!
   * Fill the buffer
   */
  int fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf) override;

  cta::log::LogContext m_lc;

  static constexpr const char* const LOG_SUFFIX  = "DriveLsStream";    //!< Identifier for log messages

  std::list<common::dataStructures::TapeDrive> m_tapeDrives;
  std::list<cta::catalogue::Catalogue::DriveConfig> m_tapeDrivesConfigs;
};


DriveLsStream::DriveLsStream(const RequestMessage &requestMsg, cta::catalogue::Catalogue &catalogue,
  cta::Scheduler &scheduler, const cta::common::dataStructures::SecurityIdentity &clientID,
  log::LogContext &lc) :
  XrdCtaStream(catalogue, scheduler),
  m_lc(lc),
  m_tapeDrives(m_catalogue.getTapeDrives()),
  m_tapeDrivesConfigs(m_catalogue.getTapeDriveConfigs()) {
  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "DriveLsStream() constructor");

  auto driveRegexOpt = requestMsg.getOptional(cta::admin::OptionString::DRIVE);

  // Dump all drives unless we specified a drive
  if (driveRegexOpt) {
    std::string driveRegexStr = '^' + driveRegexOpt.value() + '$';
    utils::Regex driveRegex(driveRegexStr.c_str());

    // Remove non-matching drives from the list
    for (auto dr_it = m_tapeDrives.begin(); dr_it != m_tapeDrives.end(); ) {
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

int DriveLsStream::fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf) {
  for (bool is_buffer_full = false; !m_tapeDrives.empty() && !is_buffer_full; m_tapeDrives.pop_front()) {
    Data record;

    const auto dr = m_tapeDrives.front();
    auto  dr_item = record.mutable_drls_item();

    dr_item->set_cta_version(dr.ctaVersion ? dr.ctaVersion.value() : "");
    dr_item->set_logical_library(dr.logicalLibrary);
    dr_item->set_drive_name(dr.driveName);
    dr_item->set_host(dr.host);
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
    dr_item->set_disk_system_name(dr.diskSystemName);
    dr_item->set_reserved_bytes(dr.reservedBytes);
    dr_item->set_session_elapsed_time(dr.sessionElapsedTime ? dr.sessionElapsedTime.value() : 0);

    auto driveConfig = dr_item->mutable_drive_config();

    for (const auto& storedDriveConfig : m_tapeDrivesConfigs) {
      if (storedDriveConfig.tapeDriveName != dr.driveName) continue;
      auto driveConfigItemProto = driveConfig->Add();
      driveConfigItemProto->set_category(storedDriveConfig.category);
      driveConfigItemProto->set_key(storedDriveConfig.keyName);
      driveConfigItemProto->set_value(storedDriveConfig.value);
      driveConfigItemProto->set_source(storedDriveConfig.source);
    }
    // set the time spent in the current state
    uint64_t drive_time = time(nullptr);

    switch (dr.driveStatus) {
      using cta::common::dataStructures::DriveStatus;

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
    }
    dr_item->set_drive_status_since(drive_time);

    is_buffer_full = streambuf->Push(record);
  }
  return streambuf->Size();
}

}} // namespace cta::xrd
