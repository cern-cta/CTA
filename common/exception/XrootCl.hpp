/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <string>

#include "Exception.hpp"

#include <xrootd/XrdCl/XrdClXRootDResponses.hh>

namespace cta {
namespace exception {
/**
 * A class turning the XrootCl (xroot 4 object client) error codes
 * into castor exceptions.
 */
class XrootCl : public cta::exception::Exception {
 public:
  XrootCl(const XrdCl::XRootDStatus & status, const std::string & context);
  virtual ~XrootCl() {}
  const XrdCl::XRootDStatus & xRootDStatus() const { return m_status; }
  static void throwOnError(const XrdCl::XRootDStatus & status, const std::string& context = "");
 protected:
  XrdCl::XRootDStatus m_status;
};
}  // namespace exception
}  // namespace cta
