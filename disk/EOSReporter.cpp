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

#include <future>

#include "EOSReporter.hpp"
#include "common/exception/XrootCl.hpp"

namespace cta {
namespace disk {

EOSReporter::EOSReporter(const std::string& hostURL, const std::string& queryValue) :
m_fs(hostURL),
m_query(queryValue) {}

void EOSReporter::asyncReport() {
  auto qcOpaque = XrdCl::QueryCode::OpaqueFile;
  XrdCl::Buffer arg(m_query.size());
  arg.FromString(m_query);
  XrdCl::XRootDStatus status = m_fs.Query(qcOpaque, arg, this, CTA_EOS_QUERY_TIMEOUT);
  cta::exception::XrootCl::throwOnError(
    status, "In EOSReporter::asyncReportArchiveFullyComplete(): failed to XrdCl::FileSystem::Query()");
}

//------------------------------------------------------------------------------
//EOSReporter::AsyncQueryHandler::HandleResponse
//------------------------------------------------------------------------------
void EOSReporter::HandleResponse(XrdCl::XRootDStatus* status, XrdCl::AnyObject* response) {
  try {
    cta::exception::XrootCl::throwOnError(
      *status, "In EOSReporter::AsyncQueryHandler::HandleResponse(): failed to XrdCl::FileSystem::Query()");
    m_promise.set_value();
  }
  catch (...) {
    try {
      // store anything thrown in the promise
      m_promise.set_exception(std::current_exception());
    }
    catch (...) {
      // set_exception() may throw too
    }
  }
  delete response;
  delete status;
}
}  // namespace disk
}  // namespace cta
