/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <xrootd/XrdCl/XrdClXRootDResponses.hh>
#include "common/exception/Exception.hpp"

namespace cta::exception {

/**
 * A class turning the XrdCl (XRootD object client) error codes into CTA exceptions
 */
class XrdClException : public Exception {
public:
  XrdClException(const XrdCl::XRootDStatus& status, std::string_view context);
  virtual ~XrdClException() = default;
  const XrdCl::XRootDStatus& xRootDStatus() const { return m_status; }
  static void throwOnError(const XrdCl::XRootDStatus& status, std::string_view context = "");

private:
  XrdCl::XRootDStatus m_status;
};

} // namespace cta::exception
