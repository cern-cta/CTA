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

#include <xrootd/private/XrdSsi/XrdSsiStream.hh>

#include "cta_frontend.pb.h"
#include "frontend/common/FrontendService.hpp"
#include "frontend/common/Version.hpp"

namespace cta {
namespace frontend {

class AdminCmd;

class AdminCmdStream : public AdminCmd {
public:
  AdminCmdStream(const frontend::FrontendService& frontendService,
    const common::dataStructures::SecurityIdentity& clientIdentity,
    const admin::AdminCmd& adminCmd,
    XrdSsiStream*& stream);

  ~AdminCmdStream() = default;

  /*!
   * Process the admin command
   *
   * @return Protobuf to return to the client
   */
  xrd::Response process();

  /*!
   * Returns CTA client version and client protobuf version
   */
  frontend::Version getClientVersion() const;

private:
  /*!
   * Process admin commands which return a stream response
   *
   * @param[out]    response    CTA Admin Command response protocol buffer. This returns the status of the request.
   */
  void processAdmin_Ls               (cta::xrd::Response& response);
  void processArchiveRoute_Ls        (cta::xrd::Response& response);
  void processDrive_Ls               (cta::xrd::Response& response);
  void processFailedRequest_Ls       (cta::xrd::Response& response);
  void processGroupMountRule_Ls      (cta::xrd::Response& response);
  void processActivityMountRule_Ls   (cta::xrd::Response& response);
  void processLogicalLibrary_Ls      (cta::xrd::Response& response);
  void processMediaType_Ls           (cta::xrd::Response& response);
  void processMountPolicy_Ls         (cta::xrd::Response& response);
  void processRequesterMountRule_Ls  (cta::xrd::Response& response);
  void processShowQueues             (cta::xrd::Response& response);
  void processStorageClass_Ls        (cta::xrd::Response& response);
  void processTapePool_Ls            (cta::xrd::Response& response);
  void processTape_Ls                (cta::xrd::Response& response);
  void processTapeFile_Ls            (cta::xrd::Response& response);
  void processRepack_Ls              (cta::xrd::Response& response);
  void processDiskSystem_Ls          (cta::xrd::Response& response);
  void processDiskInstance_Ls        (cta::xrd::Response& response);
  void processDiskInstanceSpace_Ls   (cta::xrd::Response& response);
  void processVirtualOrganization_Ls (cta::xrd::Response& response);
  void processVersion                (cta::xrd::Response& response);
  void processRecycleTapeFile_Ls     (cta::xrd::Response& response);

  XrdSsiStream*&         m_stream;                 //!< XRootD SSI stream for responses
  cta::SchedulerDB_t&    m_schedDb;                //!< Reference to CTA SchedulerDB
  const std::string      m_catalogueConnString;    //!< CTA Catalogue DB connection string
};

}} // namespace cta::frontend
