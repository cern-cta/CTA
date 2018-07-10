/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <future>

#include "EOSReporter.hpp"
#include "common/exception/XrootCl.hpp"

namespace cta { namespace eos {

EOSReporter::EOSReporter(const std::string& hostURL, const std::string& queryValue, std::promise<void>& reporterState):
  m_fs(hostURL), m_query(queryValue), m_reporterState(reporterState) {}

void EOSReporter::asyncReportArchiveFullyComplete() {
  auto qcOpaque = XrdCl::QueryCode::OpaqueFile;
  XrdCl::Buffer arg (m_query.size());
  arg.FromString(m_query);
  XrdCl::XRootDStatus status=m_fs.Query( qcOpaque, arg, this, CTA_EOS_QUERY_TIMEOUT);
  cta::exception::XrootCl::throwOnError(status,
      "In EOSReporter::asyncReportArchiveFullyComplete(): failed to XrdCl::FileSystem::Query()");
}

//------------------------------------------------------------------------------
//EOSReporter::AsyncQueryHandler::HandleResponse
//------------------------------------------------------------------------------
void EOSReporter::HandleResponse(XrdCl::XRootDStatus *status,
                                 XrdCl::AnyObject    *response) {
  try {
    cta::exception::XrootCl::throwOnError(*status,
      "In EOSReporter::AsyncQueryHandler::HandleResponse(): failed to XrdCl::FileSystem::Query()");
    m_reporterState.set_value();
  } catch (...) {
    try {
      // store anything thrown in the promise
      m_reporterState.set_exception(std::current_exception());
    } catch(...) {
      // set_exception() may throw too
    }
  }
  delete response;
  delete status;
  }
}} // namespace cta::disk
