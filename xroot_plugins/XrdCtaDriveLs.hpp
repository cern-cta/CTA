/*!
 * @project        The CERN Tape Archive (CTA)
 * @brief          CTA Drive Ls stream implementation
 * @copyright      Copyright 2019 CERN
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
    return m_driveList.empty();
  }

  /*!
   * Fill the buffer
   */
  virtual int fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf);

  std::list<cta::common::dataStructures::DriveState> m_driveList;      //!< List of drives from the scheduler

  static constexpr const char* const LOG_SUFFIX  = "DriveLsStream";    //!< Identifier for log messages
};


DriveLsStream::DriveLsStream(const RequestMessage &requestMsg, cta::catalogue::Catalogue &catalogue,
  cta::Scheduler &scheduler, const cta::common::dataStructures::SecurityIdentity &clientID,
  log::LogContext &lc) :
  XrdCtaStream(catalogue, scheduler),
  m_driveList(scheduler.getDriveStates(clientID, lc))
{
  using namespace cta::admin;

  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "DriveLsStream() constructor");

  auto driveRegexOpt = requestMsg.getOptional(OptionString::DRIVE);

  // Dump all drives unless we specified a drive
  if(driveRegexOpt) {
    std::string driveRegexStr = '^' + driveRegexOpt.value() + '$';
    utils::Regex driveRegex(driveRegexStr.c_str());

    // Remove non-matching drives from the list
    for(auto dr_it = m_driveList.begin(); dr_it != m_driveList.end(); ) {
      if(driveRegex.has_match(dr_it->driveName)) {
        ++dr_it;
      } else {
        auto erase_it = dr_it;
        ++dr_it;
        m_driveList.erase(erase_it);
      }
    }

    if(m_driveList.empty()) {
      throw exception::UserError(std::string("Drive ") + driveRegexOpt.value() + " not found.");
    }
  }

  // Sort drives in the result set into lexicographic order
  typedef decltype(*m_driveList.begin()) dStateVal_t;
  m_driveList.sort([](const dStateVal_t &a, const dStateVal_t &b){ return a.driveName < b.driveName; });
}

int DriveLsStream::fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf) {
  using namespace cta::admin;

  for(bool is_buffer_full = false; !m_driveList.empty() && !is_buffer_full; m_driveList.pop_front()) {
    Data record;

    auto &dr      = m_driveList.front();
    auto  dr_item = record.mutable_drls_item();
    
    dr_item->set_cta_version(dr.ctaVersion);
    dr_item->set_logical_library(dr.logicalLibrary);
    dr_item->set_drive_name(dr.driveName);
    dr_item->set_host(dr.host);
    dr_item->set_desired_drive_state(dr.desiredDriveState.up ? DriveLsItem::UP : DriveLsItem::DOWN);
    dr_item->set_mount_type(MountTypeToProtobuf(dr.mountType));
    dr_item->set_drive_status(DriveStatusToProtobuf(dr.driveStatus));
    dr_item->set_vid(dr.currentVid);
    dr_item->set_tapepool(dr.currentTapePool);
    dr_item->set_vo(dr.currentVo);
    dr_item->set_files_transferred_in_session(dr.filesTransferredInSession);
    dr_item->set_bytes_transferred_in_session(dr.bytesTransferredInSession);
    dr_item->set_latest_bandwidth(dr.latestBandwidth);
    dr_item->set_session_id(dr.sessionId);
    dr_item->set_time_since_last_update(time(nullptr)-dr.lastUpdateTime);
    dr_item->set_current_priority(dr.currentPriority);
    dr_item->set_current_activity(dr.currentActivityAndWeight ? dr.currentActivityAndWeight.value().activity : "");
    dr_item->set_dev_file_name(dr.devFileName);
    dr_item->set_raw_library_slot(dr.rawLibrarySlot);
    dr_item->set_comment(dr.desiredDriveState.comment ? dr.desiredDriveState.comment.value() : "");
    dr_item->set_reason(dr.desiredDriveState.reason ? dr.desiredDriveState.reason.value() : "");
    
    auto driveConfig = dr_item->mutable_drive_config();
    for(auto & driveConfigItem: dr.driveConfigItems){
      auto driveConfigItemProto = driveConfig->Add();
      driveConfigItemProto->set_category(driveConfigItem.category);
      driveConfigItemProto->set_key(driveConfigItem.key);
      driveConfigItemProto->set_value(driveConfigItem.value);
      driveConfigItemProto->set_source(driveConfigItem.source);
    }
    // set the time spent in the current state
    uint64_t drive_time = time(nullptr);

    switch(dr.driveStatus) {
      using namespace cta::common::dataStructures;

      case DriveStatus::Probing:           drive_time -= dr.probeStartTime;    break;
      case DriveStatus::Up:                drive_time -= dr.downOrUpStartTime; break;
      case DriveStatus::Down:              drive_time -= dr.downOrUpStartTime; break;
      case DriveStatus::Starting:          drive_time -= dr.startStartTime;    break;
      case DriveStatus::Mounting:          drive_time -= dr.mountStartTime;    break;
      case DriveStatus::Transferring:      drive_time -= dr.transferStartTime; break;
      case DriveStatus::CleaningUp:        drive_time -= dr.cleanupStartTime;  break;
      case DriveStatus::Unloading:         drive_time -= dr.unloadStartTime;   break;
      case DriveStatus::Unmounting:        drive_time -= dr.unmountStartTime;  break;
      case DriveStatus::DrainingToDisk:    drive_time -= dr.drainingStartTime; break;
      case DriveStatus::Shutdown:          drive_time -= dr.shutdownTime;      break;
      case DriveStatus::Unknown:                                               break;
    }
    dr_item->set_drive_status_since(drive_time);

    is_buffer_full = streambuf->Push(record);
  }
  return streambuf->Size();
}

}} // namespace cta::xrd
