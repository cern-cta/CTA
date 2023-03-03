/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2023 CERN
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


#include "cta_frontend.pb.h"
#include "frontend/common/Version.hpp"
#include "frontend/common/FrontendService.hpp"

namespace cta {
namespace frontend {

class AdminCmd {
public:
  AdminCmd(const frontend::FrontendService& frontendService,
    const common::dataStructures::SecurityIdentity& clientIdentity,
    const admin::AdminCmd& event);

  ~AdminCmd() = default;

  /*!
   * Process the admin command
   *
   * @return Protobuf to return to the client
   */
  xrd::Response process();

private:
  /*!
   * Convert protobuf options to maps
   */
  void importOptions(const admin::AdminCmd& adminCmd);

  common::dataStructures::SecurityIdentity    m_cliIdentity;      //!< Client identity: username, host, authentication
  catalogue::Catalogue                       &m_catalogue;        //!< Reference to CTA Catalogue
  cta::Scheduler                             &m_scheduler;        //!< Reference to CTA Scheduler
  log::LogContext                             m_lc;               //!< CTA Log Context
  Version                                     m_clientVersion;    //!< CTA client (cta-admin) version

  // Command options extracted from protobuf
  std::map<cta::admin::OptionBoolean::Key, bool>                        m_option_bool;        //!< Boolean options
  std::map<cta::admin::OptionUInt64::Key, uint64_t>                     m_option_uint64;      //!< UInt64 options
  std::map<cta::admin::OptionString::Key, std::string>                  m_option_str;         //!< String options
  std::map<cta::admin::OptionStrList::Key, std::vector<std::string>>    m_option_str_list;    //!< String List options
};

}} // namespace cta::frontend
