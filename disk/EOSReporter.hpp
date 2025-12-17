/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "DiskReporter.hpp"

#include <XrdCl/XrdClFileSystem.hh>
#include <future>

namespace cta::disk {

const uint16_t CTA_EOS_QUERY_TIMEOUT = 15;  // Timeout in seconds that is rounded up to the nearest 15 seconds

class EOSReporter : public DiskReporter, public XrdCl::ResponseHandler {
public:
  EOSReporter(const std::string& hostURL, const std::string& queryValue);
  void asyncReport() override;

private:
  XrdCl::FileSystem m_fs;
  std::string m_query;
  void HandleResponse(XrdCl::XRootDStatus* status, XrdCl::AnyObject* response) override;
};

}  // namespace cta::disk
