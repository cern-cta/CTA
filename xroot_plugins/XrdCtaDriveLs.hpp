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

#include <xroot_plugins/XrdCtaStream.hpp>
#include <xroot_plugins/XrdSsiCtaRequestMessage.hpp>
#include <common/dataStructures/DriveStatusSerDeser.hpp>
#include <common/dataStructures/MountTypeSerDeser.hpp>

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
  virtual bool isDone() const {
    return m_tapeDriveNames.empty();
  }

  /*!
   * Fill the buffer
   */
  virtual int fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf);

  cta::log::LogContext m_lc;

  static constexpr const char* const LOG_SUFFIX  = "DriveLsStream";    //!< Identifier for log messages

  std::list<std::string> m_tapeDriveNames;
};


DriveLsStream::DriveLsStream(const RequestMessage &requestMsg, cta::catalogue::Catalogue &catalogue,
  cta::Scheduler &scheduler, const cta::common::dataStructures::SecurityIdentity &clientID,
  log::LogContext &lc) :
  XrdCtaStream(catalogue, scheduler),
  m_lc(lc) {
  using namespace cta::admin;

  m_tapeDriveNames = m_catalogue.getTapeDriveNames();

  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "DriveLsStream() constructor");

  auto driveRegexOpt = requestMsg.getOptional(OptionString::DRIVE);

  // Dump all drives unless we specified a drive
  if (driveRegexOpt) {
    std::string driveRegexStr = '^' + driveRegexOpt.value() + '$';
    utils::Regex driveRegex(driveRegexStr.c_str());

    // Remove non-matching drives from the list
    for (auto dr_it = m_tapeDriveNames.begin(); dr_it != m_tapeDriveNames.end(); ) {
      if (driveRegex.has_match(*dr_it)) {
        ++dr_it;
      } else {
        auto erase_it = dr_it;
        ++dr_it;
        m_tapeDriveNames.erase(erase_it);
      }
    }

    if (m_tapeDriveNames.empty()) {
      throw exception::UserError(std::string("Drive ") + driveRegexOpt.value() + " not found.");
    }
  }

  // Sort drives in the result set into lexicographic order
  m_tapeDriveNames.sort();
}

int DriveLsStream::fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf) {
  using namespace cta::admin;

  for (bool is_buffer_full = false; !m_tapeDriveNames.empty() && !is_buffer_full; m_tapeDriveNames.pop_front()) {
    Data record;

    const auto dr = m_catalogue.getTapeDrive(m_tapeDriveNames.front()).value();
    auto  dr_item = record.mutable_drls_item();

    dr_item->set_cta_version(dr.ctaVersion ? dr.ctaVersion.value() : "");
    dr_item->set_logical_library(dr.logicalLibrary);
    dr_item->set_drive_name(dr.driveName);
    dr_item->set_host(dr.host);
    dr_item->set_desired_drive_state(dr.desiredUp ? DriveLsItem::UP : DriveLsItem::DOWN);
    dr_item->set_mount_type(MountTypeToProtobuf(dr.mountType));
    dr_item->set_drive_status(DriveStatusToProtobuf(dr.driveStatus));
    dr_item->set_vid(dr.currentVid ? dr.currentVid.value() : "");
    dr_item->set_tapepool(dr.currentTapePool ? dr.currentTapePool.value() : "");
    dr_item->set_vo(dr.currentVo ? dr.currentVo.value() : "");
    dr_item->set_files_transferred_in_session(dr.filesTransferedInSession ? dr.filesTransferedInSession.value() : 0);
    dr_item->set_bytes_transferred_in_session(dr.bytesTransferedInSession ? dr.bytesTransferedInSession.value() : 0);
    if (dr.latestBandwidth && std::isdigit(dr.latestBandwidth.value().at(0)))
      dr_item->set_latest_bandwidth(std::stoi(dr.latestBandwidth.value()));
    else
      dr_item->set_latest_bandwidth(0);
    dr_item->set_session_id(dr.sessionId ? dr.sessionId.value() : 0);
    const auto lastUpdateTime = dr.lastModificationLog ? dr.lastModificationLog.value().time : 0;
    dr_item->set_time_since_last_update(time(nullptr) - lastUpdateTime);
    dr_item->set_current_priority(dr.currentPriority ? dr.currentPriority.value() : 0);
    dr_item->set_current_activity(dr.currentActivity ? dr.currentActivity.value() : "");
    dr_item->set_dev_file_name(dr.devFileName ? dr.devFileName.value() : "");
    dr_item->set_raw_library_slot(dr.rawLibrarySlot ? dr.rawLibrarySlot.value() : "");
    dr_item->set_comment(dr.userComment ? dr.userComment.value() : "");
    dr_item->set_reason(dr.reasonUpDown ? dr.reasonUpDown.value() : "");
    dr_item->set_disk_system_name(dr.diskSystemName);
    dr_item->set_reserved_bytes(dr.reservedBytes);

    auto driveConfig = dr_item->mutable_drive_config();

    const auto configNamesAndKeys = m_catalogue.getDriveConfigNamesAndKeys();
    std::list<std::pair<std::string, std::string>> configItems;
    std::copy_if(configNamesAndKeys.begin(), configNamesAndKeys.end(), std::back_inserter(configItems),
      [dr](const std::pair<std::string, std::string>& elem)
        { return elem.first == dr.driveName; });
    for(const auto& driveConfigItem: configItems){
      auto driveConfigItemProto = driveConfig->Add();
      const auto config = m_catalogue.getDriveConfig(driveConfigItem.first, driveConfigItem.second);
      std::string category, value, source;
      std::tie(category, value, source) = config.value();
      driveConfigItemProto->set_category(category);
      driveConfigItemProto->set_key(driveConfigItem.second);
      driveConfigItemProto->set_value(value);
      driveConfigItemProto->set_source(source);
    }
    // set the time spent in the current state
    uint64_t drive_time = time(nullptr);

    switch(dr.driveStatus) {
      using namespace cta::common::dataStructures;

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
      case DriveStatus::Unknown:                                               break;
    }
    dr_item->set_drive_status_since(drive_time);

    is_buffer_full = streambuf->Push(record);
  }
  return streambuf->Size();
}

}} // namespace cta::xrd
