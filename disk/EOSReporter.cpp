/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "EOSReporter.hpp"

#include "XrdClException.hpp"

namespace cta::disk {

EOSReporter::EOSReporter(const std::string& hostURL, const std::string& queryValue)
    : m_fs(hostURL),
      m_query(queryValue) {}

void EOSReporter::asyncReport() {
  auto qcOpaque = XrdCl::QueryCode::OpaqueFile;
  XrdCl::Buffer arg(m_query.size());
  arg.FromString(m_query);
  XrdCl::XRootDStatus status = m_fs.Query(qcOpaque, arg, this, CTA_EOS_QUERY_TIMEOUT);
  exception::XrdClException::throwOnError(
    status,
    "In EOSReporter::asyncReportArchiveFullyComplete(): failed to XrdCl::FileSystem::Query()");
}

//------------------------------------------------------------------------------
//EOSReporter::AsyncQueryHandler::HandleResponse
//------------------------------------------------------------------------------
void EOSReporter::(XrdCl::XRootDStatus* status, XrdCl::AnyObject* response) {
  auto statusPtr = std::unique_ptr<XrdCl::XRootDStatus>(status);
  auto responsePtr = std::unique_ptr<XrdCl::AnyObject>(response);
  try {
    exception::XrdClException::throwOnError(
      *statusPtr,
      "In EOSReporter::AsyncQueryHandler::HandleResponse(): failed to XrdCl::FileSystem::Query()");
    m_promise.set_value();
  } catch (...) {
    try {
      // store anything thrown in the promise
      m_promise.set_exception(std::current_exception());
    } catch (...) {
      // set_exception() may throw too
    }
  }
}

}  // namespace cta::disk
