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

#include "EOSReporter.hpp"
#include "common/exception/XrootCl.hpp"

namespace cta { namespace eos {

EOSReporter::EOSReporter(const std::string& hostURL, const std::string& queryValue):
  m_fs(hostURL), m_query(queryValue) {}


void EOSReporter::reportArchiveFullyComplete() {
  auto qcOpaque = XrdCl::QueryCode::OpaqueFile;
  XrdCl::Buffer arg (m_query.size());
  arg.FromString(m_query);
  XrdCl::Buffer * resp = nullptr;
  const uint16_t queryTimeout = 15; // Timeout in seconds that is rounded up to the nearest 15 seconds
  XrdCl::XRootDStatus status=m_fs.Query(qcOpaque, arg, resp, queryTimeout);
  delete (resp);
  cta::exception::XrootCl::throwOnError(status,
      "In EOSReporter::reportArchiveFullyComplete(): failed to XrdCl::FileSystem::Query()");
} 

}} // namespace cta::disk
