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

#pragma once

#include "DiskReporter.hpp"
#include <XrdCl/XrdClFileSystem.hh>

#include <future>

namespace cta { namespace disk {
const uint16_t CTA_EOS_QUERY_TIMEOUT = 15; // Timeout in seconds that is rounded up to the nearest 15 seconds
    
class EOSReporter: public DiskReporter, public XrdCl::ResponseHandler {
public:
  EOSReporter(const std::string & hostURL, const std::string & queryValue);
  void asyncReport() override;
private:
  XrdCl::FileSystem m_fs;
  std::string m_query;
  void HandleResponse(XrdCl::XRootDStatus *status,
                      XrdCl::AnyObject    *response) override;
};

}} // namespace cta::disk
