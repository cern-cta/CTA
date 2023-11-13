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

#include "xroot_plugins/XrdCtaStream.hpp"
#include "frontend/common/Version.hpp"
#include "catalogue/SchemaVersion.hpp"
#include "version.h"

namespace cta::xrd {

/*!
 * Stream object which implements "tapepool ls" command
 */
class VersionStream: public XrdCtaStream{
 public:
  /*!
   * Constructor
   *
   * @param[in]    requestMsg    RequestMessage containing command-line arguments
   */
  VersionStream(const frontend::AdminCmdStream& requestMsg, cta::catalogue::Catalogue &catalogue,
    cta::Scheduler &scheduler, const std::string &catalogueConnString);

 private:
    static constexpr const char* const LOG_SUFFIX  = "VersionStream";    //!< Identifier for log messages

    frontend::Version m_client_versions;
    frontend::Version m_server_versions;
    std::string m_catalogue_conn_string;
    std::string m_catalogue_version;
    bool m_is_upgrading;

    bool m_is_done = false;

    /*!
    * Fill the buffer
    */
    virtual int fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf);

  /*!
  * Only one item is sent so isDone = true
  */
  virtual bool isDone() const {
    return m_is_done;
  }
};

VersionStream::VersionStream(const frontend::AdminCmdStream& requestMsg, cta::catalogue::Catalogue &catalogue,
  cta::Scheduler &scheduler, const std::string &catalogueConnString) :
  XrdCtaStream(catalogue, scheduler),
  m_client_versions(requestMsg.getClientVersion()),
  m_catalogue_conn_string(catalogueConnString),
  m_catalogue_version(m_catalogue.Schema()->getSchemaVersion().getSchemaVersion<std::string>()),
  m_is_upgrading(m_catalogue.Schema()->getSchemaVersion().getStatus<catalogue::SchemaVersion::Status>()
    == catalogue::SchemaVersion::Status::UPGRADING)
{
  XrdSsiPb::Log::Msg(XrdSsiPb::Log::DEBUG, LOG_SUFFIX, "VersionStream() constructor");
  m_server_versions.ctaVersion = CTA_VERSION;
  m_server_versions.protobufTag = XROOTD_SSI_PROTOBUF_INTERFACE_VERSION;
}

int VersionStream::fillBuffer(XrdSsiPb::OStreamBuffer<Data> *streambuf) {
  m_is_done = true;
  Data record;
  auto version = record.mutable_version_item();
  auto client_version = version->mutable_client_version();
  client_version->set_cta_version(m_client_versions.ctaVersion);
  client_version->set_xrootd_ssi_protobuf_interface_version(m_client_versions.protobufTag);
  auto server_version = version->mutable_server_version();
  server_version->set_cta_version(m_server_versions.ctaVersion);
  server_version->set_xrootd_ssi_protobuf_interface_version(m_server_versions.protobufTag);
  version->set_catalogue_connection_string(m_catalogue_conn_string);
  version->set_catalogue_version(m_catalogue_version);
  version->set_is_upgrading(m_is_upgrading);
  streambuf->Push(record);

  return streambuf->Size();
}

} // namespace cta::xrd
