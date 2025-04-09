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
#include "common/Constants.hpp"
#include "common/dataStructures/DriveStatusSerDeser.hpp"
#include "common/dataStructures/MountTypeSerDeser.hpp"
#include "xroot_plugins/XrdCtaStream.hpp"
#include "cmdline/admin_common/DataItemMessageFill.hpp"

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
  const std::string m_instanceName;

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
      m_instanceName(requestMsg.getInstanceName()),
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
                     if (config.keyName == SCHEDULER_NAME_CONFIG_KEY) {
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

    fillDriveItem(dr, dr_item, m_instanceName, driveSchedulerBackendName, driveConfigs);

    is_buffer_full = streambuf->Push(record);
  }
  return streambuf->Size();
}

}  // namespace cta::xrd
